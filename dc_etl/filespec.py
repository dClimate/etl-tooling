from __future__ import annotations

import typing

import fsspec


def file(path, fs_name="file", *fs_args, **fs_kwargs) -> FileSpec:
    """Get a FileSpec instance which combines an fsspec Filesystem with a path component.

    Parameters
    ----------
    path: str
        The path to the file in the filesystem. May be a URL appropriate for the filesystem.

    fs_name: str
        The fsspec filesystem name, as pased `fsspec.fileystem`. Defaults to `"file"`.

    *fs_args:
        Arbitrary positional arguments to be passed along to `fsspec.filesystem` after the filesystem name. Particulars
        will depend on the actual filesystem being used.

    **fs_kwargs:
        Arbitrary keyword arguments to be passed along to `fsspec.filesystem`. Particulars will depend on the actual
        filesystem being used.

    Returns
    -------
    FileSpec:
        The FileSpec instance.
    """
    fs = fsspec.filesystem(fs_name, *fs_args, **fs_kwargs)
    return FileSpec(fs, str(path))


class FileSpec(typing.NamedTuple):
    """Encapsulates both the location of a file and its fsspec "filesystem"."""

    fs: fsspec.AbstractFileSystem
    """The filesystem where this file can be found.
    """

    path: str
    """The path to the file in the filesystem. Will be a URL appropriate for the filesystem.
    """

    @classmethod
    def _load_yaml(cls, loader, node) -> FileSpec:
        from .config import _Configuration  # Avoid circular import

        config = _Configuration(loader.construct_mapping(node), loader.name, [])
        fs = config.pop("fs")
        path = config.pop("path")
        return cls(fsspec.filesystem(fs, **config), path)

    def exists(self) -> bool:
        """Check if this file exists."""
        return self.fs.exists(self.path)

    def open(self, mode="rb"):
        """A passthrough to `fsspec.AbstractFilesystem.open` using the path from this instance."""
        return self.fs.open(self.path, mode)

    def with_suffix(self, suffix: str) -> FileSpec:
        """Returns a new FileSpec with the file suffix changed to `suffix`.

        If there is no file suffix, one is added.

        Parameters
        ----------
        suffix : str
            The new file suffix

        Returns
        -------
        FileSpec
            The new file spec
        """
        path = self.path
        dot = path.rfind(".")
        if dot > -1:
            sep = path.rfind("/")
            # make sure dot isn't in a parent folder before truncating old suffix
            if sep < dot:
                path = path[:dot]

        return FileSpec(self.fs, f"{path}.{suffix.lstrip('.')}")

    @property
    def name(self) -> str:
        """The name of the file or directory pointed to by this spec, without any parent folders."""
        path = self.path.rstrip("/")
        sep = path.rfind("/")
        if sep > -1:
            return path[sep + 1 :]

        return path

    def __truediv__(self, other: str) -> FileSpec:
        """Overloads the `/` (division) operator to allow new paths to be created by appending new path elements,
        much as `pathlib.Path` does in the standard library.
        """
        path = f"{self.path.rstrip('/')}/{other}"
        return type(self)(self.fs, path)

    @property
    def parent(self) -> FileSpec:
        split = self.path.rstrip("/").rsplit("/", 1)
        if len(split) == 2:
            parent, _ = split
            if not parent:
                parent = "/"
            return type(self)(self.fs, parent)
