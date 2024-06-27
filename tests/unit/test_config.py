import os
import pathlib

import fsspec
import pytest

from dc_etl import config as config_module
from dc_etl import errors
from dc_etl import filespec
from dc_etl.extractors import netcdf
from dc_etl.fetchers import cpc


class TestConfiguration:

    def _read_config(self, path=None):
        return config_module.Configuration.from_yaml(path)

    def test_constructor(self):
        config = self._read_config()
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].fetcher.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert config.datasets[0].extractor.foo == "bar"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].fetcher.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].fetcher.download_folder == "/opt/rawdata/open_waffle_houses"
        assert config.datasets[1].extractor.bar == "baz"

    def test_constructor_explicit_config_path(self):
        config = self._read_config("etc/datasets.yaml")
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].fetcher.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert config.datasets[0].extractor.foo == "bar"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].fetcher.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].fetcher.download_folder == "/opt/rawdata/open_waffle_houses"
        assert config.datasets[1].extractor.bar == "baz"

    def test_constructor_bad_fetcher(self):
        with pytest.raises(errors.ConfigurationError):
            self._read_config("etc/bad_fetcher.yaml")

    def test_constructor_fetcher_missing_name(self):
        with pytest.raises(errors.ConfigurationError):
            self._read_config("etc/fetcher_missing_name.yaml")

    def test_constructor_config_yaml_in_cwd(self):
        os.chdir("etc")
        config = self._read_config()
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].fetcher.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert config.datasets[0].extractor.foo == "bar"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].fetcher.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].fetcher.download_folder == "/opt/rawdata/open_waffle_houses"
        assert config.datasets[1].extractor.bar == "baz"

    def test_constructor_config_yaml_in_parent_folder(self):
        os.chdir("etc/some_folder")
        config = self._read_config()
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].fetcher.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert config.datasets[0].extractor.foo == "bar"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].fetcher.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].fetcher.download_folder == "/opt/rawdata/open_waffle_houses"
        assert config.datasets[1].extractor.bar == "baz"

    def test_constructor_config_yaml_is_missing(self, tmpdir):
        os.chdir(tmpdir)
        with pytest.raises(errors.ConfigurationError):
            self._read_config()


class TestConfigurationForConcreteImplementations:

    def test_cpc(self):
        path = pathlib.Path("etc/cpc.yaml").absolute()
        spec = filespec.FileSpec(fsspec.filesystem("file"), path)
        config = config_module.Configuration.from_yaml(spec)

        precip_global = config.datasets[0]
        assert precip_global.name == "cpc_precip_global"

        assert isinstance(precip_global.fetcher, cpc.CPCFetcher)
        assert precip_global.fetcher._cache.path == "some/path/to/somewhere"
        assert precip_global.fetcher._cache.fs.auto_mkdir is True

        assert isinstance(precip_global.extractor, netcdf.NetCDFExtractor)
        assert precip_global.extractor.inline_threshold == 42
        assert precip_global.extractor.output_folder.path == "output/folder"
