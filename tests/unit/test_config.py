import os

import pytest

from dc_etl import config as config_module
from dc_etl import errors


class TestConfiguration:

    def _read_config(self, path=None):
        return config_module.Configuration.from_yaml(path)

    def test_constructor(self):
        config = self._read_config()
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].cluster == "pleiedes"
        assert config.datasets[0].extractor.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].extractor.download_folder == "/opt/rawdata/price_of_tea_in_china"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].cluster == "globular"
        assert config.datasets[1].extractor.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].extractor.download_folder == "/opt/rawdata/open_waffle_houses"

    def test_constructor_explicit_config_path(self):
        config = self._read_config("etc/datasets.yaml")
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].cluster == "pleiedes"
        assert config.datasets[0].extractor.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].extractor.download_folder == "/opt/rawdata/price_of_tea_in_china"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].cluster == "globular"
        assert config.datasets[1].extractor.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].extractor.download_folder == "/opt/rawdata/open_waffle_houses"

    def test_constructor_bad_extractor(self):
        with pytest.raises(errors.ConfigurationError):
            self._read_config("etc/bad_extractor.yaml")

    def test_constructor_missing_required_config(self):
        with pytest.raises(errors.ConfigurationError):
            self._read_config("etc/missing_config.yaml")

    def test_constructor_config_yaml_in_cwd(self):
        os.chdir("etc")
        config = self._read_config()
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].cluster == "pleiedes"
        assert config.datasets[0].extractor.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].extractor.download_folder == "/opt/rawdata/price_of_tea_in_china"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].cluster == "globular"
        assert config.datasets[1].extractor.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].extractor.download_folder == "/opt/rawdata/open_waffle_houses"

    def test_constructor_config_yaml_in_parent_folder(self):
        os.chdir("etc/some_folder")
        config = self._read_config()
        assert isinstance(config, config_module.Configuration)

        assert config.datasets[0].name == "price_of_tea_in_china"
        assert config.datasets[0].cluster == "pleiedes"
        assert config.datasets[0].extractor.base_url == "http://example.com/datasets/price_of_tea_in_china"
        assert config.datasets[0].extractor.download_folder == "/opt/rawdata/price_of_tea_in_china"

        assert config.datasets[1].name == "open_waffle_houses"
        assert config.datasets[1].cluster == "globular"
        assert config.datasets[1].extractor.base_url == "http://example.com/datasets/open_waffle_houses"
        assert config.datasets[1].extractor.download_folder == "/opt/rawdata/open_waffle_houses"

    def test_constructor_config_yaml_is_missing(self, tmpdir):
        os.chdir(tmpdir)
        with pytest.raises(errors.ConfigurationError):
            self._read_config()
