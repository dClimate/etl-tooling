from dc_etl import config as config_module


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
