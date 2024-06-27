from __future__ import annotations

import typing

import fsspec


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

    def __truediv__(self, other: str) -> FileSpec:
        """Overloads the `/` (division) operator to allow new paths to be created by appending new path elements,
        much as `pathlib.Path` does in the standard library.
        """
        path = f"{self.path.rstrip('/')}/{other}"
        return type(self)(self.fs, path)
