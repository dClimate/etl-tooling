from unittest import mock

import fsspec
import pytest

from dc_etl import errors, filespec


class TestFileSpec:
    @staticmethod
    def test__load_yaml(mocker):
        loader = mock.Mock()
        loader.construct_mapping.return_value = {
            "fs": "afilesystem",
            "path": "path/to/some/where",
        }
        node = object()
        fsspec = mocker.patch("dc_etl.filespec.fsspec")

        fetcher = filespec.FileSpec._load_yaml(loader, node)
        assert fetcher.fs is fsspec.filesystem.return_value
        assert fetcher.path == "path/to/some/where"

        loader.construct_mapping.assert_called_once_with(node)
        fsspec.filesystem.assert_called_once_with("afilesystem")

    @staticmethod
    def test__load_yaml_with_extra_kwargs(mocker):
        loader = mock.Mock()
        loader.construct_mapping.return_value = {
            "fs": "afilesystem",
            "path": "path/to/some/where",
            "foo": "bar",
            "baz": "boo",
        }
        node = object()
        fsspec = mocker.patch("dc_etl.filespec.fsspec")

        fetcher = filespec.FileSpec._load_yaml(loader, node)
        assert fetcher.fs is fsspec.filesystem.return_value
        assert fetcher.path == "path/to/some/where"

        loader.construct_mapping.assert_called_once_with(node)
        fsspec.filesystem.assert_called_once_with("afilesystem", foo="bar", baz="boo")

    @staticmethod
    def test__load_yaml_missing_config(mocker):
        loader = mock.Mock()
        loader.construct_mapping.return_value = {
            "path": "path/to/some/where",
        }
        node = object()
        fsspec = mocker.patch("dc_etl.filespec.fsspec")

        with pytest.raises(errors.MissingConfigurationError):
            filespec.FileSpec._load_yaml(loader, node)

        loader.construct_mapping.assert_called_once_with(node)
        fsspec.filesystem.assert_not_called()

    @staticmethod
    def test_constructor():
        fs = object()
        file = filespec.FileSpec(fs, "some/thing/some/where")
        assert file.fs is fs
        assert file.path == "some/thing/some/where"

    @staticmethod
    def test___div__():
        fs = object()
        file = filespec.FileSpec(fs, "some/thing/some/where/") / "over" / "there"
        assert file.fs is fs
        assert file.path == "some/thing/some/where/over/there"

    @staticmethod
    def test_open(tmpdir):
        """Integration test for read/write with a real filesystem."""
        fs = fsspec.filesystem("file")
        folder = filespec.FileSpec(fs, str(tmpdir))
        file = folder / "foo.txt"
        assert not file.exists()
        with file.open("w") as f:
            print("Hi Mom!", file=f)

        assert file.open("r").read().strip() == "Hi Mom!"
        assert file.exists()

    @staticmethod
    def test_with_suffix():
        file = filespec.FileSpec(None, "some/thing.foo").with_suffix("bar")
        assert file.path == "some/thing.bar"

    @staticmethod
    def test_with_suffix_no_suffix():
        file = filespec.FileSpec(None, "some/thing").with_suffix(".bar")
        assert file.path == "some/thing.bar"

    @staticmethod
    def test_with_suffix_no_suffix_but_dot_in_parent():
        file = filespec.FileSpec(None, "so.me/thing").with_suffix("bar")
        assert file.path == "so.me/thing.bar"

    @staticmethod
    def test_name():
        assert filespec.FileSpec(None, "some/thing").name == "thing"

    @staticmethod
    def test_name_no_parent():
        assert filespec.FileSpec(None, "thing").name == "thing"
