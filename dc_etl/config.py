from __future__ import annotations

import collections
import pathlib

from importlib.metadata import entry_points

import fsspec
import yaml

from . import errors
from .dataset import Dataset
from .fetch import Fetcher
from .filespec import FileSpec

CONFIG_FILE = "datasets.yaml"
_MISSING = object()


class Configuration:
    """Configuration information about datasets and their ETL pipelines."""

    @classmethod
    def from_yaml(cls, path: pathlib.Path | FileSpec | None = None) -> Configuration:
        """Import configuration from a yaml file."""
        if path is None:
            path = _find_config()

        if not isinstance(path, FileSpec):
            path = FileSpec(fsspec.filesystem("file"), str(path))

        config = _Configuration.from_yaml(path)
        datasets = [_read_dataset(dataset) for dataset in config["datasets"]]
        return cls(datasets)

    def __init__(self, datasets):
        self.datasets = datasets


def _read_dataset(config) -> Dataset:
    fetcher = _get_fetcher(config["fetcher"])
    return Dataset(config["name"], fetcher)


def _get_fetcher(config) -> Fetcher:
    name = config.pop("name")
    for fetcher in entry_points(group="fetcher"):
        if fetcher.name == name:
            return fetcher.load()(**config)

    raise errors.ConfigurationError(f"Unable to find fetcher: {name}")


class _Configuration(collections.UserDict):
    """A wrapper for a dictionary that adds some minimal validation so that users get friendlier error messages if
    required configuration is missing.
    """

    @classmethod
    def from_yaml(cls, path: FileSpec):
        data = yaml.load(path.open(), Loader=yaml.Loader)
        return cls(data, path.path, [])

    def __init__(self, data: dict, config_file: pathlib.Path, path: list[str]):
        super().__init__(data)
        self.config_file = config_file
        self.path = path

    def get(self, key, default=None):
        return self.wrap(self.data.get(key, default), self.path + [key])

    def get_required_config(self, key):
        value = self.get(key, _MISSING)
        if value is _MISSING:
            path = " -> ".join(self.path + [key])
            raise errors.ConfigurationError(f"Missing required configuration from {self.config_file}: {path}")

        return value

    def wrap(self, value, path):
        if isinstance(value, dict):
            return type(self)(value, self.config_file, path)

        elif isinstance(value, list):
            return [self.wrap(value, path + [str(index)]) for index, value in enumerate(value)]

        return value

    __getitem__ = get_required_config


def _find_config() -> pathlib.Path:
    """Search for yaml config file."""
    root = pathlib.Path("/")
    here = pathlib.Path(".").absolute()
    while here != root:
        config_file = here / CONFIG_FILE
        if config_file.is_file():
            return config_file

        config_file = here / "etc" / CONFIG_FILE
        if config_file.is_file():
            return config_file

        here = here.parent

    raise errors.ConfigurationError(
        f"Unable to find '{CONFIG_FILE}'. This file can be in the current working directory or any parent directory, "
        "or in an 'etc' folder in any of those locations. To use an arbitrary file you can pass the '--config' "
        "argument.",
    )
