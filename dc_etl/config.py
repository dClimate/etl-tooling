from __future__ import annotations

import pathlib

from importlib.metadata import entry_points

import yaml

from . import errors
from .dataset import Dataset
from .extract import Extractor

CONFIG_FILE = "datasets.yaml"
_MISSING = object()


class Configuration:
    """Configuration information about datasets and their ETL pipelines."""

    @classmethod
    def from_yaml(cls, path: pathlib.Path | None = None) -> Configuration:
        """Import configuration from a yaml file."""
        if path is None:
            path = _find_config()

        config = _Configuration.from_yaml(path)
        datasets = [_read_dataset(dataset) for dataset in config["datasets"]]
        return cls(datasets)

    def __init__(self, datasets):
        self.datasets = datasets


def _read_dataset(config) -> Dataset:
    extractor = _get_extractor(config["extractor"])
    return Dataset(config["name"], config["cluster"], extractor)


def _get_extractor(config) -> Extractor:
    name = config["name"]
    for extractor in entry_points(group="extractor"):
        if extractor.name == name:
            return extractor.load()(config["config"])

    raise errors.ConfigurationError(f"Unable to find extractor: {name}")


class _Configuration:
    """A wrapper for a dictionary that adds some minimal validation so that users get friendlier error messages if
    required configuration is missing.
    """

    @classmethod
    def from_yaml(cls, path: pathlib.Path):
        config = yaml.load(open(path), Loader=yaml.Loader)
        return cls(config, path, [])

    def __init__(self, config: dict, config_file: pathlib.Path, path: list[str]):
        self.config = config
        self.config_file = config_file
        self.path = path

    def get(self, key, default=None):
        return self.wrap(self.config.get(key, default), self.path + [key])

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
