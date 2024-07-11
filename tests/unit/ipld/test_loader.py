from unittest import mock

import numpy
import pytest

from dc_etl.config import _Configuration
from dc_etl.fetch import Timespan
from dc_etl.ipld.loader import IPLDLoader
from tests.conftest import npdate
from tests.unit.conftest import mock_dataset


class TestIPLDLoader:
    def test__from_config(self):
        config = _Configuration(
            {"time_dim": "nation time", "publisher": {"name": "testing", "foo": "bar"}}, "some/file", []
        )
        loader = IPLDLoader._from_config(config)
        assert loader.time_dim == "nation time"
        assert loader.publisher.foo == "bar"

    def test_initial(self):
        publisher = mock.Mock()
        mapper = mock.Mock()
        dataset = mock.Mock()
        span = (42, 53)

        selected = dataset.sel.return_value
        mapper.freeze.return_value = "contentid"

        loader = IPLDLoader(time_dim="tempo", publisher=publisher)
        loader._mapper = mock.Mock(return_value=mapper)
        loader.initial(dataset, span)

        dataset.sel.assert_called_once_with(tempo=slice(*span))
        selected.to_zarr.assert_called_once_with(store=mapper, consolidated=True)
        mapper.freeze.assert_called_once_with()
        publisher.publish.assert_called_once_with("contentid")
        loader._mapper.assert_called_once_with()

    def test_append(self):
        publisher = mock.Mock()
        mapper = mock.Mock()
        dataset = mock.Mock()
        span = (42, 53)

        selected = dataset.sel.return_value
        mapper.freeze.return_value = "contentid"

        loader = IPLDLoader(time_dim="tempo", publisher=publisher)
        loader._mapper = mock.Mock(return_value=mapper)
        loader.append(dataset, span)

        dataset.sel.assert_called_once_with(tempo=slice(*span))
        selected.to_zarr.assert_called_once_with(store=mapper, consolidated=True, append_dim="tempo")
        mapper.freeze.assert_called_once_with()
        publisher.publish.assert_called_once_with("contentid")
        loader._mapper.assert_called_once_with(publisher.retrieve.return_value)

    def test_replace(self):
        publisher = mock.Mock()
        mapper = mock.Mock()
        dataset = mock.Mock()
        original_dataset = mock.Mock()
        span = Timespan(42, 53)

        def mock_time_to_integer(dataset, timestmap):
            assert dataset is original_dataset
            return timestmap * 2

        selected = dataset.sel.return_value
        selected.dims = ["one", "tempo", "two"]
        dropped = selected.drop_vars.return_value
        mapper.freeze.return_value = "contentid"

        loader = IPLDLoader(time_dim="tempo", publisher=publisher)
        loader._mapper = mock.Mock(return_value=mapper)
        loader.dataset = mock.Mock(return_value=original_dataset)
        loader._time_to_integer = mock_time_to_integer
        loader.replace(dataset, span)

        dataset.sel.assert_called_once_with(tempo=slice(*span))
        selected.drop_vars.assert_called_once_with(["one", "two"])
        dropped.to_zarr.assert_called_once_with(store=mapper, consolidated=True, region={"tempo": slice(84, 107)})
        mapper.freeze.assert_called_once_with()
        publisher.publish.assert_called_once_with("contentid")
        loader._mapper.assert_called_once_with(publisher.retrieve.return_value)

    def test_dataset(self, mocker):
        xarray = mocker.patch("dc_etl.ipld.loader.xarray")
        publisher = mock.Mock()
        mapper = mock.Mock()

        publisher.retrieve.return_value = "thiscontenthere"

        loader = IPLDLoader(time_dim="tempo", publisher=publisher)
        loader._mapper = mock.Mock(return_value=mapper)
        assert loader.dataset() is xarray.open_zarr.return_value

        loader._mapper.assert_called_once_with(root="thiscontenthere")
        publisher.retrieve.assert_called_once_with()
        xarray.open_zarr.assert_called_once_with(store=mapper, consolidated=True)

    def test__mapper_no_root(self, mocker):
        ipldstore = mocker.patch("dc_etl.ipld.loader.ipldstore")
        mapper = ipldstore.get_ipfs_mapper.return_value

        loader = IPLDLoader(time_dim="tempo", publisher=None)
        assert loader._mapper() is mapper
        ipldstore.get_ipfs_mapper.assert_called_once_with()
        mapper.set_root.assert_not_called()

    def test__mapper_w_root(self, mocker):
        ipldstore = mocker.patch("dc_etl.ipld.loader.ipldstore")
        mapper = ipldstore.get_ipfs_mapper.return_value

        loader = IPLDLoader(time_dim="tempo", publisher=None)
        assert loader._mapper("potato") is mapper
        ipldstore.get_ipfs_mapper.assert_called_once_with()
        mapper.set_root.assert_called_once_with("potato")

    def test__time_to_integer(self, dataset):
        loader = IPLDLoader(time_dim="tempo", publisher=None)
        assert loader._time_to_integer(dataset, npdate(2000, 7, 7)) == 188


@pytest.fixture
def dataset():
    time = numpy.arange(
        numpy.datetime64("2000-01-01", "ns"), numpy.datetime64("2001-01-01", "ns"), numpy.timedelta64(1, "D")
    )
    data = numpy.random.randn(len(time))
    return mock_dataset(data=("data", data), dims=[("tempo", time)])
