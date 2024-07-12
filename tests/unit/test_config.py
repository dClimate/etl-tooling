import pytest

from dc_etl.config import _Configuration
from dc_etl.errors import MissingConfigurationError
from dc_etl.filespec import file


@pytest.fixture
def config():
    return _Configuration(
        {
            "one": {"a": "b"},
            "two": [
                {"name": "testing", "foo": "bar"},
                {"animal": "zebra"},
                {"name": "missing"},
            ],
        },
        "some/config_file",
        [],
    )


class Test_Configuration:
    def test_from_yaml(self):
        config = _Configuration.from_yaml(file("etc/config.yaml"))
        assert config["one"] == {"a": "b", "c": "d"}

    def test_get(self, config):
        config = config.get("one")
        assert config == {"a": "b"}
        assert config.path == ["one"]

    def test_get_default(self, config):
        assert config.get("three", "default") == "default"

    def test_get_with_path_that_includes_list(self, config):
        config = config["two"][1]
        assert config == {"animal": "zebra"}
        assert config.path == ["two", "1"]

    def test_missing_required_field(self, config):
        with pytest.raises(MissingConfigurationError):
            config["three"]

    def test_as_component(self, config):
        component = config["two"][0].as_component("fetcher")
        assert component.foo == "bar"

    def test_as_component_with__from_config(self, config, mocker):
        mocker.patch("tests.unit.conftest.mock_entry_point", MockComponent)
        component = config["two"][0].as_component("fetcher")
        assert component.foo == "bar|bar"

    def test_missing_component(self, config):
        with pytest.raises(MissingConfigurationError):
            config["two"][2].as_component("fetcher")


class MockComponent:
    @classmethod
    def _from_config(cls, config):
        config["foo"] = "|".join([config["foo"]] * 2)
        return cls(**config)

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
