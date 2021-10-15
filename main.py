import itertools
import os
from hashlib import md5
from pathlib import Path

import pandas as pd

target_dir = r"C:\Users\Scott\Pictures\Saved Pictures"
# TODO:  Implement CLI


def control():
    """
    User choices:
    - Confirm list of dirs to scan [future]
    - # of duplicate to keep from list
    - Next set of duplicates
    - Purge duplicates
    - Delete empty dirs
    - Restart scan
    - Size prioity ordering
    - Count priority ordering
    - Quit
    """
    # Gather/approve list of target dirs here
    g = DeDup(target_dir)
    current_group = None
    quit_count = 0
    while True:
        current_group, choices = g.next_group(current_group)
        if current_group is None:
            print("===End of duplicate groups===")
            g.dup_purge()
            continue
        for n, c in enumerate(choices, start=1):
            print(f"{n}: {c}")
        prompt_string = "Enter (#) to keep, (N)ext, (Q)uit, (P)urge, (D)elete empty dirs, (S)ize order, (C)ount order"
        choice = input(prompt_string).lower()
        if choice == 'n':
            continue
        if choice == 'q':
            quit_count += 1
            if g.f.purge.sum() <= 0:
                break
            if quit_count == 2:
                break
            print(f"Warning: There are {g.f.purge.sum()} files marked to purge. Choose (Q)uit again to abandon.")
        if choice == 'p':
            g.dup_purge()
            g.find_dups()
            continue
        if choice == 'd':
            print(f"Removing empty dirs and recomputing duplicates.")
            remove_empty_dirs()
            g.find_dups()
            continue
        if choice == 's':
            print("Priority sorting not implemented")
            # g.priority = 'sum'
            # Call some function that re-sorts the groups array
            # print("Setting sort priority to size...")
            continue
        if choice == 'c':
            print("Priority sorting not implemented")
            # g.priority = 'count'
            # Call some function that re-sorts the groups array
            # print("Group priority set to count.")
            continue
        if choice.isnumeric() and (1 <= int(choice) <= len(choices)):
            g.mark_group(current_group, int(choice) - 1)
    print("Done")


class DeDup:
    def __init__(self, roots):
        self.roots = roots
        self.f = None
        self.t = None
        self.fileset = None
        self.groups = None
        self.priority = 'sum'
        self.verbose = False  # TODO: Implement in print statements
        self.scan_filesystem()
        self.hash_potential_duplicates()
        self.find_dups()

    def scan_filesystem(self):
        """
        Scans filesystem and creates dataframe for deduplication
        """
        print("Scanning in filesystem tree...")
        files = [
            {
                'path': f,
                'node_size': f.stat().st_size if f.is_file() else 0,
                'purge': False,
            }
            for f in itertools.chain(
                Path(self.roots).glob('**/*'), [Path(self.roots)]
            )  # TODO: Support list of target_dir
            if f.is_file()
        ]
        print(f"Total files: {len(files)}")
        self.f = pd.DataFrame(files)
        return None

    def hash_potential_duplicates(self):
        def hasher(record):
            """
            Compute hash based on file size to avoid running out of memory
            :param record: Record for a file containing "path" element
            :return: hash:
            """
            if record.node_size <= 100e6:
                return md5(record.path.read_bytes()).hexdigest()
            BUF_SIZE = 65536
            hash = md5()
            with record.path.open(mode='rb') as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    hash.update(data)
            return hash.hexdigest()

        self.f = self.f[self.f.duplicated('node_size', keep=False)].copy()
        print(f"Potential duplicate files (same size): {self.f.shape[0]}. Computing MD5 sums...")
        self.f['hash'] = self.f.apply(hasher, axis=1)
        self.f['parent'] = self.f.path.apply(lambda p: p.parent)

    def find_dups(self):
        """
        Build list of duplicate groups in priority order
        group, choices, filecount, totalsize
        order by filecount or totalsize as user cofigured
        :return:
        """
        self.t = self.f.loc[~self.f.purge]
        if not len(self.t):
            return None
        self.t = self.t[self.t.hash.duplicated(keep=False)].copy()
        hashgroups = self.t.groupby('hash')['parent'].apply(lambda x: tuple(sorted(set(x))))
        self.t['dups'] = hashgroups[self.t.hash].values
        dup_dirs = self.t.groupby('dups').filter(lambda x: len(set(x.parent)) > 1)
        if len(dup_dirs) == 0:
            self.groups = pd.DataFrame()
        else:
            self.groups = (
                dup_dirs.groupby('dups')['node_size']
                .agg([len, sum])
                .reset_index()
                .rename(columns={'dups': 'choices', 'len': 'count'})
            )
            self.groups['is_dir'] = True
        dup_files = self.t.groupby('dups').filter(lambda x: len(set(x.parent)) == 1)
        if len(dup_files != 0):
            file_choices = (
                dup_files.groupby('hash')
                .apply(lambda x: tuple(x.path))
                .reset_index()
                .rename(columns={0: 'choices'})  # TODO: May be no dup files
            )
            file_groups = (
                dup_files.groupby(['parent', 'hash'])['node_size']
                .agg([len, sum])
                .reset_index()
                .merge(file_choices, on='hash')
                .drop(columns=['parent', 'hash'])
                .rename(columns={'len': 'count'})
            )
            file_groups['is_dir'] = False
            self.groups = self.groups.append(file_groups)
        return None

    def next_group(self, current_group: int = 0):
        """
        Manages returns group and choices for each duplicate group
        Generates summary header
        :return: header, choices or Header, None on end of duplicates
        """
        if len(self.groups) == 0:
            return None, None
        current_group = current_group or 0
        if 0 > current_group > len(self.groups) - 1:
            return None, None
        self.groups = self.groups.sort_values(self.priority, ascending=False)  # TODO: Crashes when groups is empty
        return current_group, (self.groups.iloc[current_group]['choices'])

    def mark_group(self, group, choice):
        """
        Remover takes group and choice and marks files for deletion
        if choice is dir, mark files in other dirs of same group
        if choice is file, mark other files of same checksum
        :returns None
        """
        g = self.groups.iloc[group, :]
        purge_items = g.choices[:choice] + g.choices[choice + 1:]
        if g.is_dir:
            self.f.loc[self.f.isin({'dups': g.choices, 'parent': purge_items}).parent, 'purge'] = True
        else:
            self.f.loc[self.f.path.isin(purge_items), 'purge'] = True
        self.find_dups()
        return None

    def dup_purge(self):
        """
        Delete files marked as duplicate
        offer confirmation with (at least) count of files and check that all checksums are accounted for
        :return: Number of files (and bytes?) deleted
        """
        original_hashes = set(self.f.hash)
        unpurged_hashes = set(self.f.loc[~self.f.purge, 'hash'])
        if len(original_hashes ^ unpurged_hashes) != 0:
            print("Whoa! Some content is deleted in all places. Not going to purge.")

        purge_targets = self.f.loc[self.f.purge, 'path']
        print(f"There are {purge_targets} files to purge. Purge?")
        # response, _ = get_user_response('Purge? (Y/N)'):
        # if response ==
        # # TODO: Get response here
        # print(f"Attempting to purge {len(purge_targets)} files")
        # for path in purge_targets:
        #     try:
        #         print(f"Deleting: {path}")
        #     except Exception as e:  # TODO:  Too broad; figure out delete failures
        #         print(f"Skipping {path} because of error: {e}")


# def get_user_response(prompt_string, valid_choices):
#     while True:
#         choice = input(prompt_string).lower()
#         if choice in valid_choices:
#             return choice, None
#         if choice.isnumeric():
#             return None, int(choice)


def remove_empty_dirs():
    """
    Removes empty dirs and announces number of dirs removed
    :returns Number of dirs removed  TODO: Implement return value
    """
    [os.removedirs(p) for p in Path(target_dir).glob('**/*') if p.is_dir() and len(list(p.iterdir())) == 0]
    return None


def get_tree_df(target_dir):  # TODO: Expand target_dir to take list of dirs and files
    files = [
        {'path': f, 'node_size': f.stat().st_size if f.is_file() else 0}
        for f in itertools.chain(Path(target_dir).glob('**/*'), [Path(target_dir)])
        if f.is_file()
    ]
    return pd.DataFrame(files)


if __name__ == '__main__':
    control()
    print("Goodbye")
