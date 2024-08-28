from __future__ import annotations

import collections
import pathlib

from importlib.metadata import entry_points

import yaml

from dc_etl import errors
from dc_etl.filespec import FileSpec

_MISSING = object()


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
        return self._wrap(self.data.get(key, default), self.path + [key])

    def get_required_config(self, key):
        value = self.get(key, _MISSING)
        if value is _MISSING:
            path = " -> ".join(self.path + [key])
            raise errors.MissingConfigurationError(
                f"Missing required configuration from {self.config_file}: {path}"
            )

        return value

    def _wrap(self, value, path):
        if isinstance(value, dict):
            return type(self)(value, self.config_file, path)

        elif isinstance(value, list):
            return [
                self._wrap(value, path + [str(index)])
                for index, value in enumerate(value)
            ]

        return value

    __getitem__ = get_required_config

    def as_component(self, group):
        config = self.copy()
        name = config.pop("name", "default")
        return _get_component(group, name, (), config)


def _get_component(group, name, args, kwargs):
    for component in entry_points(group=group):
        if component.name == name:
            factory = component.load()

            if hasattr(factory, "_from_config") and isinstance(kwargs, _Configuration):
                return factory._from_config(kwargs)

            return factory(*args, **kwargs)

    raise errors.MissingConfigurationError(f"Unable to find {group}: {name}")
