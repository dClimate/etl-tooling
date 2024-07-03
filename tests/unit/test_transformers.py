from unittest import mock

import numpy
import pytest

from dc_etl import transformers

from .conftest import mock_dataset


def test_composite():
    def a(x):
        return x * 2

    def b(x):
        return x + 3

    composition = transformers.composite(a, b)
    assert composition(4) == 11


def test_rename_dims():
    rename = transformers.rename_dims({"a": "b", "c": "d"})
    dataset = mock.Mock()
    assert rename(dataset) is dataset.rename.return_value
    dataset.rename.assert_called_once_with({"a": "b", "c": "d"})


def test_normalize_longitudes(dataset):
    assert dataset.longitude[0] == 90
    assert dataset.longitude[-1] == 225

    normalize = transformers.normalize_longitudes()
    dataset = normalize(dataset)

    assert list(dataset.longitude) == [-180, -165, -150, -135, 90, 105, 120, 135, 150, 165]


@pytest.fixture
def dataset():
    latitude = numpy.arange(-50, 50, 10)
    longitude = numpy.arange(90, 240, 15)
    time = numpy.arange(
        numpy.datetime64("2010-05-12", "ns"), numpy.datetime64("2010-05-14", "ns"), numpy.timedelta64(1, "D")
    )
    data = numpy.random.randn(len(time), len(latitude), len(longitude))
    return mock_dataset(data=("data", data), dims=[("time", time), ("latitude", latitude), ("longitude", longitude)])