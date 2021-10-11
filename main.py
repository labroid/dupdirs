import itertools
import os
import sys
from hashlib import md5
from pathlib import Path

import numpy as np
import pandas as pd

# from rich import print

target_dir = r"C:\Users\Scott\Pictures\Saved Pictures"


def main():
    g = DeDup()
    g.scan_filesystem(target_dir)
    g.find_dups()
    current_group = None
    while True:
        menu = g.next_group(current_group)
        if menu is None:
            print("Done")
            break
        print(menu.group, menu.choices)
        current_group = menu.group
    print("REally done")


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
    pass


class DeDup:
    def __init__(self):
        self.roots = None
        self.f = None
        self.fileset = None
        self.groups = None
        self.priority = 'size'
        self.verbose = False  # TODO: Implement in print statements

    def scan_filesystem(self, roots=None):  # roots are a list of Path
        """
        Scans filesystem and recomputes duplicate groups
        """
        print("Scanning in filesystem tree...")
        files = [
            {
                'path': f,
                'node_size': f.stat().st_size if f.is_file() else 0,  # TODO:  Is this necessary? Dir 0 anyway?
                'purge': False,
            }
            for f in itertools.chain(Path(roots).glob('**/*'), [Path(roots)])  # TODO: Support list of target_dir
            if f.is_file()
        ]
        print(f"Total files: {len(files)}")
        self.fileset = pd.DataFrame(files)
        return None

    def find_dups(self):
        """
        Build list of duplicate groups in priority order
        group, choices, filecount, totalsize
        order by filecount or totalsize as user cofigured
        :return:
        """
        self.f = self.fileset[~self.fileset.purge]
        self.f = self.f[self.f.duplicated('node_size', keep=False)].copy()
        print(f"Potential duplicate files (same size): {self.f.shape[0]}. Computing MD5 sums...")
        self.f['hash'] = self.f['path'].apply(
            lambda p: md5(p.read_bytes()).hexdigest()  # TODO:  Unless already exists
        )  # TODO: Handle large files differently
        self.f = self.f[self.f['hash'].duplicated(keep=False)].copy()  # TODO: Is there a way to tell if copy is needed?
        print(f"Duplicated files found: {self.f.shape[0]}")
        self.f['parent'] = self.f.path.apply(lambda p: p.parent)
        hashgroups = self.f.groupby('hash')['parent'].apply(lambda x: tuple(sorted(set(x))))
        self.f['dups'] = hashgroups[self.f.hash].values

        dup_dirs = self.f.groupby('dups').filter(lambda x: len(set(x.parent)) > 1)
        dup_files = self.f.groupby('dups').filter(lambda x: len(set(x.parent)) == 1)
        groups = dup_dirs.groupby('dups')['node_size'].agg([len, sum]).reset_index().rename(columns={'dups': 'choices'})
        groups['is_dir'] = True
        file_choices = (
            dup_files.groupby('hash').apply(lambda x: tuple(x.path)).reset_index().rename(columns={0: 'choices'})
        )
        file_groups = (
            dup_files.groupby(['parent', 'hash'])['node_size']
            .agg([len, sum])
            .reset_index()
            .merge(file_choices, on='hash')
            .drop(columns=['parent', 'hash'])
        )
        file_groups['is_dir'] = False
        groups = (
            groups.append(file_groups)
            .sort_values('sum', ascending=False)
            .reset_index(drop=True)
            .reset_index()
            .rename(columns={'index': 'group'})
        )
        groups['group'] = groups.group + 1
        self.groups = groups
        return None

    def next_group(self, current_group: int = 0):
        """
        Manages returns group and choices for each duplicate group
        Generates summary header
        :return: header, choices or Header, None on end of duplicates
        """
        current_group = current_group or 0
        current_group += 1
        if current_group in range(1, len(self.groups) + 1):
            return self.groups.loc[current_group - 1, ['group', 'choices']]
        return None

    def dup_marker(self, group, choice):
        """
        Remover takes group and choice and marks files for deletion
        if choice is dir, mark files in other dirs of same group
        if choice is file, mark other files of same checksum
        :returns number marked
        """
        pass

    def dup_purge(self):
        """
        Delete files marked as duplicate
        offer confirmation with (at least) count of files
        :return: Number of files (and bytes?) deleted
        """
        pass

    # f = f.set_index(['dups', 'parent', 'hash'])
    # for group in f.index.unique('dups'):
    #     if len(group) > 1:
    #         x = {'group': group, 'choices': list(f.loc[group].index.unique('parent'))}
    #     else:
    #         x = {'group': group, 'choices': list(f.loc[group].index.unique('hash'))}
    # # for group in f.index.get_level_values('dups'):
    # #     for dirs in f.index.get_level_values('parent'):
    # #         print(f"Group {group}, directory {dirs}, list of files {f.loc[group].loc[dirs].path}")
    #
    # dir_dups = dup_sets.size()
    # priority = dup_sets.count()['hash'].sort_values(ascending=False)
    # for dirs, count in priority.items():
    #     # f.loc[dup_sets.groups[priority.index[3]], :]
    #     # show dirs or files
    #     # allow to move on to next
    #     # recompute if anything marked for deletion
    #     if len(dirs) > 1:
    #         print("=========================")
    #         print(f"Total duplicates remaining: {priority.sum()}, {count} are BETWEEN these directories")
    #         for n, d in enumerate(dirs, start=1):
    #             print(f"{n} keep {d}")
    #     else:
    #         filegroups = f[f.dups == dirs].groupby('hash')
    #         for group in filegroups:
    #             print("=========================")
    #             print(
    #                 f"Total duplicates remaining: {priority.sum()}, {len(group[1])} are duplicates WITHIN a directory"
    #             )
    #             for n, d in enumerate(group[1].path, start=1):
    #                 print(f"{n} keep {d.name}")


# def get_input(max_int, letters):
#     while True:
#         choice = input("Enter # to keep, N for next group, Q to quit, D to clear empty dir trees and quit").lower()
#         if choice in letters:
#             return choice
#         if choice.isnumeric():
#             if int(choice) <= max_int:
#                 return int(choice)


#
# def control():
#     while True:

#
#     if choice == 'q':
#         break
#     if choice == 'n':
#         continue
#     if choice == 'd':
#         print(f"Deleting empty dirs")  # TODO: Call clean up empty dir trees
#         break
#     pick = int(choice)
#         print(f"Keeping {dirs[pick-1]}")
#         print(f"Purging {dirs[:pick-1] + dirs[pick:]}")
#     print("Done")
#     sys.exit(0)
#     print("The following are directories with the same content, starting from highest in the filesystem tree.")
#     print("Choose a number to mark for deletion.")
#     for parent, group in candidates:
#         print("-----------------------")
#         print(f"Parent: {parent}")
#         new_ui(group.path)
#
#     # Have user process directories that are largely duplicated as a subset of some other directory
#     # Find directories with high percentage of duplicated files and identify other dir holding the duplicates
#     # Go through each dir and find high-percentage duplicates
#     candidates = f.loc[f.is_file, :].groupby(lambda p: p.parent)
#     for parent, group in candidates:
#         if parent in [Path(r'c:\Users'), Path(r'c:\Users\Scott\Pictures')]:
#             continue
#         fraction = group.hash_dup.sum() / group.shape[0]
#         if fraction < 1:
#             continue
#         print("-----------------")
#         print(f"{fraction:.1%} of {parent} is duplicated elsewhere")
#         parentfilecount = len(list(parent.glob('*')))
#         # Get all paths containing each file in the high-percent duplicate folder, group by parent directories
#
#     hostgroups = f.loc[f.is_file & f.loc[f.is_file, 'hash'].isin(group.hash)].groupby(lambda p: p.parent)
#     for hostdir, hostmatch in hostgroups:
#         if hostdir == parent:
#             continue
#         hostfilecount = len(list(Path(hostdir).glob('*')))
#         if all(group.hash.isin(hostmatch.hash)):
#             print(f"All {parentfilecount} files of {parent} are in the {hostfilecount} files of {hostdir}")
#         else:
#             print(f"NOT all {parentfilecount} files of {parent}  are in the {hostfilecount} files of {hostdir}")
#     new_ui([parent])
#
#     # Now work through individual files
#     candidates = (
#         f.loc[f.is_file & f.hash_dup].sort_values(['node_size', 'path'], axis='index', ascending=False).groupby('hash')
#     )
#     print("The following are individual files that need to be considered:")
#     for parent, group in candidates:
#         print("----------------")
#         print(f"Duplicated file: {parent}")
#         new_ui(candidates.path)
#
#     # TODO: Convert to mark files to delete
#     # TODO: Confirm that all hashes about to be deleted exist somewhere
#     print("Done!")
#
#


def remove_empty_dirs():
    """
    Removes empty dirs and announces number of dirs removed
    :returns Number of dirs removed
    """
    [os.removedirs(p) for p in Path(target_dir).glob('**/*') if p.is_dir() and len(list(p.iterdir())) == 0]


def get_tree_df(target_dir):  # TODO: Expand target_dir to take list of dirs and files
    files = [
        {
            'path': f,
            'node_size': f.stat().st_size if f.is_file() else 0,
        }
        for f in itertools.chain(Path(target_dir).glob('**/*'), [Path(target_dir)])
        if f.is_file()
    ]
    return pd.DataFrame(files)


if __name__ == '__main__':
    main()
    print("Done")
