import itertools
import os
import sys
from hashlib import md5
from pathlib import Path
import stat

import pandas as pd
from typing import List
import typer

# target_dirs = [r"C:\Users\Scott\Pictures\Saved Pictures"]
# target_dirs = [
#     r"C:\Users\Scott\Pictures\2016-07-01 for Scott to Deal With",
#     r"C:\Users\Scott\Pictures\Pictures for Droid",
#     r"C:\Users\Scott\Pictures\Saved Pictures",
# ]
# target_dirs = [
#     r"C:\Users\Scott\Documents\Programming\dupedirs\Test Dirs 1",
#     r"C:\Users\Scott\Documents\Programming\dupedirs\Test Dirs 2",
# ]


def control(target_dirs: List[str]):
    """
    Implements the 'controller' and 'view'. Provides interactive user interface to the application.
    """

    class Menu:
        """Parent class that registers all menu commands that inherit from this class. Creates menulist dict
        that can be traversed to get available commands."""

        menulist = {}

        def __init_subclass__(cls, keycode=None, **kwargs):
            super().__init_subclass__(**kwargs)
            cls.menulist[keycode] = cls
            cls.keycode = keycode

    class Number(Menu, keycode='number'):
        def __init__(self, model):
            self.model = model
            self.command = "(#) to keep"

        def go(self, choice):
            if choice.isnumeric() and (1 <= int(choice) <= len(self.model.groups.iloc[self.model.group]['choices'])):
                self.model.mark_group(int(choice) - 1)
                return True
            return True

    class Quit(Menu, keycode='q'):
        def __init__(self, model):
            self.model = model
            self.command = "(Q)uit"
            self.quit_count = 0

        def go(self, choice):
            if choice != self.keycode:
                return True
            if self.model.f.purge.sum() <= 0:
                return False
            self.quit_count += 1
            if self.quit_count >= 2:
                return False
            notice = f"#####Warning: There are {self.model.f.purge.sum()} files marked to purge. Purge using (P)urge or press (Q)uit again to abandon."
            typer.echo('#' * len(notice))
            typer.echo(notice)
            typer.echo("#" * len(notice))
            return True

    class Purge(Menu, keycode='p'):
        def __init__(self, model):
            self.model = model
            self.command = "(P)urge"

        def go(self, choice):
            if choice != self.keycode:
                return True
            if self.model.f.purge.sum() == 0:
                typer.echo("No files have been marked for purge")
                return True
            typer.echo(f"There are {self.model.f.purge.sum()} files marked to purge.")
            while True:
                if typer.confirm("Purge?"):
                    self.model.dup_purge()
                    self.model.find_dups()
                return True

    class Next(Menu, keycode='n'):
        def __init__(self, model):
            self.model = model
            self.command = "(N)ext"

        def go(self, choice):
            if choice != self.keycode:
                return True
            self.model.group += 1
            return True

    class RmEmptyDirs(Menu, keycode='d'):
        def __init__(self, model):
            self.model = model
            self.command = "(D)elete empty dirs"

        def go(self, choice):
            if choice != self.keycode:
                return True
            typer.echo(f"Removing empty dirs and recomputing duplicates.")
            remove_empty_dirs(self.model.roots)
            self.model.find_dups()
            return True

    class SizeSort(Menu, keycode='s'):
        def __init__(self, model):
            self.model = model
            self.command = "(S)ize sort"

        def go(self, choice):
            if choice != self.keycode:
                return True
            typer.echo("")
            typer.echo("Sorting by duplicate size.")
            self.model.priority = 'sum'
            self.model.find_dups()
            return True

    class CountSort(Menu, keycode='c'):
        def __init__(self, model):
            self.model = model
            self.command = "(C)ount sort"

        def go(self, choice):
            if choice != self.keycode:
                return True
            typer.echo("Sorting by duplicate count.")
            self.model.priority = 'count'
            self.model.find_dups()
            return True

    class Restart(Menu, keycode='r'):
        def __init__(self, model):
            self.model = model
            self.command = "(R)estart"

        def go(self, choice):
            if choice != self.keycode:
                return True
            self.model.group = 0
            return True

    g = DeDup(target_dirs)
    cmd = [f(g) for f in Menu.menulist.values()]
    prompt = ' '.join(c.command for c in cmd)
    while True:
        # TODO: Make this look better on quit while files left to purge
        if (len(g.groups) < 1) or g.group > (len(g.groups) - 1):
            typer.secho("#################################", fg=typer.colors.RED)
            typer.secho("#####End of duplicate groups#####", fg=typer.colors.RED)
            typer.secho("#################################", fg=typer.colors.RED)
            typer.echo(f"Duplicates remaining: {g.f[~g.f['purge']].duplicated('hash', keep=False).sum()}")
        else:
            current_group = g.groups.iloc[g.group]
            typer.echo("=" * 25)
            typer.echo(
                f"Duplicates remaining: {g.groups['count'].sum()}. This group contains {current_group['count']} files of {current_group['sum'] / 2 ** 20:.2f} MB"
            )
            for n, c in enumerate(current_group.choices, start=1):
                typer.echo(f"{n}: {c}")
        choice = typer.prompt(prompt).lower()
        # choice = input(prompt).lower()  # TODO: This should be part of Typer
        if not all([c.go(choice) for c in cmd]):
            break
    typer.echo("Done")


class DeDup:
    """
    This class is the data model, and all operations that occur on the data model
    """
    def __init__(self, roots: List):
        self.roots = roots
        self.group = 0
        self.f = pd.DataFrame()
        self.t = pd.DataFrame()
        self.groups = pd.DataFrame()
        self.priority = 'sum'
        self.verbose = False  # TODO: Implement in print statements
        self.scan_filesystem()
        self.hash_potential_duplicates()
        self.find_dups()

    def scan_filesystem(self):
        """
        Scans filesystem and creates dataframe for deduplication
        """
        typer.echo("Scanning in filesystem tree...")
        files = [
            {
                'path': f,
                'node_size': f.stat().st_size if f.is_file() else 0,
                'purge': False,
            }
            for f in itertools.chain.from_iterable([Path(p).rglob('*') for p in self.roots])
            if f.is_file()
        ]
        typer.echo(f"Total files: {len(files)}")
        self.f = pd.DataFrame(files)
        return None

    def hash_potential_duplicates(self):
        if len(self.f) == 0:
            return None

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

        self.f = self.f.loc[self.f.duplicated('node_size', keep=False)]
        typer.echo(f"Potential duplicate files (same size): {self.f.shape[0]}.")
        if len(self.f) == 0:
            return None
        typer.echo(" Computing MD5 sums...")
        self.f['hash'] = self.f.apply(hasher, axis=1)
        self.f = self.f.loc[self.f.duplicated('hash', keep=False)]
        if len(self.f) == 0:
            typer.echo("No duplicates found")
            return None
        self.f['parent'] = self.f.path.apply(lambda p: p.parent)
        return None

    def find_dups(self):
        """
        Build list of duplicate groups in priority order
        group, choices, filecount, totalsize
        order by filecount or totalsize as user cofigured
        :return:
        """
        if len(self.f) == 0:
            return None
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
                dup_files.groupby('hash').apply(lambda x: tuple(x.path)).reset_index().rename(columns={0: 'choices'})
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
            self.groups = self.groups.sort_values(self.priority, ascending=False)
        return None

    def mark_group(self, choice):
        """
        Given group and choice marks files for deletion
        if choice is dir, mark files in other dirs of same group
        if choice is file, mark other files of same checksum
        :returns None
        """
        g = self.groups.iloc[self.group, :]
        purge_items = g.choices[:choice] + g.choices[choice + 1:]
        if g.is_dir:
            self.f.loc[self.f.isin({'dups': g.choices, 'parent': purge_items}).parent, 'purge'] = True
        else:
            self.f.loc[self.f.path.isin(purge_items), 'purge'] = True
        self.find_dups()
        self.is_purge_safe()
        return None

    def dup_purge(self):
        """
        Delete files marked as duplicate
        offer confirmation with (at least) count of files and check that all checksums are accounted for
        :return: Number of files (and bytes?) deleted
        """
        self.is_purge_safe()
        purge_targets = self.f.loc[self.f.purge, 'path']
        for path in purge_targets:
            try:
                typer.echo(f"Deleting: {path}")
                path.unlink()
                self.f = self.f.query("path != @path")
            except Exception as e:  # TODO:  Too broad; figure out delete failures
                typer.echo(f"Skipping {path} because of error: {e}")
        self.f = self.f.loc[self.f.duplicated('node_size', keep=False)]
        return

    def is_purge_safe(self):
        """
        Confirms that at least one copy of each hash remains if files marked for purge are eliminted
        :return: True if at least one copy remains, False if all copies of a given hash are marked for purge
        """
        # Problematic:  {'392ef306582b9fe4da6b0b120cad7598', 'f073d3db9c07887b3cb2e606effec57a'}
        if set(self.f.hash) != set(self.f.loc[~self.f.purge, 'hash']):
            print("Whoa! Some content is deleted in all places. Risk of data loss - not going to purge. Sorry.")
            print(set(self.f.hash) - set(self.f.loc[~self.f.purge, 'hash']))
            print(set(self.f.loc[~self.f.purge, 'hash']) - set(self.f.hash))
            assert False, "Purge safe check failed"


# def audit_dir_removed(event, args):
#     typer.echo(f'audit: {event} with args={args}')

def remove_empty_dirs(target_dirs):
    """
    Removes empty dirs and announces number of dirs removed
    :returns None
    """

    typer.echo("Removing empty directories...")
    # sys.addaudithook(audit_dir_removed)

    all_dirs = [p for p in itertools.chain(*[Path(r).rglob('*') for r in target_dirs]) if p.is_dir()] + [
        Path(p) for p in target_dirs
    ]
    empty_dirs = [p for p in all_dirs if len(list(p.iterdir())) == 0]
    [os.chmod(p, stat.S_IWRITE) for p in all_dirs]
    [os.removedirs(p) for p in empty_dirs]
    typer.echo("Done.")
    return None


if __name__ == '__main__':
    typer.run(control)
    typer.echo("Goodbye")
