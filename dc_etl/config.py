from __future__ import annotations

import collections
import pathlib

from importlib.metadata import entry_points

import fsspec
import yaml

from . import errors
from .dataset import Dataset
from .extract import Extractor
from .fetch import Fetcher
from .filespec import FileSpec
from .combine import Combiner, CombinePreprocessor, CombinePostprocessor

CONFIG_FILE = "datasets.yaml"
_MISSING = object()


def fetcher(name: str, *args, **kwargs) -> Fetcher:
    """Get and configure a fetcher implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the fetcher implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("fetcher", name, args, kwargs)


def extractor(name: str, *args, **kwargs) -> Extractor:
    """Get and configure an extractor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the extractor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("extractor", name, args, kwargs)


def combiner(name: str, *args, **kwargs) -> Combiner:
    """Get and configure an combiner implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the combiner implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("combiner", name, args, kwargs)


def combine_preprocessor(name: str, *args, **kwargs) -> CombinePreprocessor:
    """Get and configure an preprocessor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the preprocessor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("combine_preprocessor", name, args, kwargs)


def combine_postprocessor(name: str, *args, **kwargs) -> CombinePostprocessor:
    """Get and configure an postprocessor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the postprocessor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("combine_postprocessor", name, args, kwargs)


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
    name, kwargs = config["fetcher"].immutable_pop("name")
    fetcher_inst = _get_component("fetcher", name, (), kwargs)

    name, kwargs = config["extractor"].immutable_pop("name")
    extractor_inst = _get_component("extractor", name, (), kwargs)

    name, kwargs = config["combiner"].immutable_pop("name", "default")
    kwargs["preprocessors"] = [
        _read_component("combine_preprocessor", config_) for config_ in kwargs.get("preprocessors", ())
    ]
    combiner_inst = combiner(name, **kwargs)

    return Dataset(config["name"], fetcher_inst, extractor_inst, combiner_inst)


def _read_component(group, config):
    name, kwargs = config.immutable_pop("name")
    return _get_component(group, name, (), kwargs)


def _get_component(group, name, args, kwargs):
    for component in entry_points(group=group):
        if component.name == name:
            return component.load()(*args, **kwargs)

    raise errors.MissingConfigurationError(f"Unable to find {group}: {name}")


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
            raise errors.MissingConfigurationError(f"Missing required configuration from {self.config_file}: {path}")

        return value

    def wrap(self, value, path):
        if isinstance(value, dict):
            return type(self)(value, self.config_file, path)

        elif isinstance(value, list):
            return [self.wrap(value, path + [str(index)]) for index, value in enumerate(value)]

        return value

    __getitem__ = get_required_config

    def immutable_pop(self, key, default=_MISSING) -> _Configuration:
        config = self.copy()
        if default is _MISSING:
            value = config.pop(key)
            return value, config

        value = config.pop(key, default)
        return value, config


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

    raise errors.MissingConfigurationError(
        f"Unable to find '{CONFIG_FILE}'. This file can be in the current working directory or any parent directory, "
        "or in an 'etc' folder in any of those locations. To use an arbitrary file you can pass the '--config' "
        "argument.",
    )
