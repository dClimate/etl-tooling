import os
import pathlib

import fsspec
import pytest

from dc_etl import combine, config as config_module
from dc_etl import errors
from dc_etl import filespec
from dc_etl.extractors import netcdf
from dc_etl.fetchers import cpc
from dc_etl.transform import identity


def test_fetcher():
    fetcher = config_module.fetcher("testing", "one", "two", foo="bar", bar="baz")
    assert fetcher.args == ("one", "two")
    assert fetcher.foo == "bar"
    assert fetcher.bar == "baz"


def test_extractor():
    extractor = config_module.extractor("testing", "one", "two", foo="bar", bar="baz")
    assert extractor.args == ("one", "two")
    assert extractor.foo == "bar"
    assert extractor.bar == "baz"


def test_combiner():
    combiner = config_module.combiner("testing", "one", "two", foo="bar", bar="baz")
    assert combiner.args == ("one", "two")
    assert combiner.foo == "bar"
    assert combiner.bar == "baz"


def test_combine_preprocessor():
    combine_preprocessor = config_module.combine_preprocessor("testing", "one", "two", foo="bar", bar="baz")
    assert combine_preprocessor.args == ("one", "two")
    assert combine_preprocessor.foo == "bar"
    assert combine_preprocessor.bar == "baz"


def test_combine_postprocessor():
    combine_postprocessor = config_module.combine_postprocessor("testing", "one", "two", foo="bar", bar="baz")
    assert combine_postprocessor.args == ("one", "two")
    assert combine_postprocessor.foo == "bar"
    assert combine_postprocessor.bar == "baz"


def test_transformer():
    transformer = config_module.transformer("testing", "one", "two", foo="bar", bar="baz")
    assert transformer.args == ("one", "two")
    assert transformer.foo == "bar"
    assert transformer.bar == "baz"


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
        assert config.datasets[0].combiner.arg == "value"
        assert config.datasets[0].transformer.mut == "ate"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].fetcher.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].fetcher.download_folder == "/opt/rawdata/open_waffle_houses"
        assert config.datasets[1].extractor.bar == "baz"
        assert config.datasets[1].combiner.arg == "ument"
        assert config.datasets[1].transformer is identity

    def test_constructor_explicit_config_path(self):
        config = self._read_config("etc/datasets.yaml")
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].fetcher.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert config.datasets[0].extractor.foo == "bar"
        assert config.datasets[0].combiner.arg == "value"
        assert config.datasets[0].transformer.mut == "ate"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].fetcher.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].fetcher.download_folder == "/opt/rawdata/open_waffle_houses"
        assert config.datasets[1].extractor.bar == "baz"

    def test_constructor_bad_fetcher(self):
        with pytest.raises(errors.MissingConfigurationError):
            self._read_config("etc/bad_fetcher.yaml")

    def test_constructor_fetcher_missing_name(self):
        with pytest.raises(errors.MissingConfigurationError):
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
        with pytest.raises(errors.MissingConfigurationError):
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

        assert isinstance(precip_global.combiner, combine.DefaultCombiner)
        assert precip_global.combiner.output.path == "combined/output"
        assert precip_global.combiner.concat_dims == ["time"]
        assert precip_global.combiner.identical_dims == ["latitude", "longitude"]
        assert len(precip_global.combiner.preprocessors) == 1
        assert precip_global.combiner.preprocessors[0].__name__ == "fix_fill_value"
