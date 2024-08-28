from unittest import mock

import numcodecs
import numpy
import pytest

from dc_etl.config import _Configuration
from dc_etl import transformers

from .conftest import mock_dataset


class TestComposite:
    def test__from_config(self):
        config = _Configuration(
            {
                "transformers": [
                    {"name": "testing", "trans": "form"},
                    {"name": "testing", "mut": "ate"},
                ]
            },
            "some/config/file",
            [],
        )
        transformer = transformers.Composite._from_config(config)
        assert len(transformer.transformers) == 2
        assert transformer.transformers[0].trans == "form"
        assert transformer.transformers[1].mut == "ate"

    def test___call__(self):
        def a(x):
            return x * 2

        def b(x):
            return x + 3

        composition = transformers.Composite(a, b)
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

    assert list(dataset.longitude) == [
        -180,
        -165,
        -150,
        -135,
        90,
        105,
        120,
        135,
        150,
        165,
    ]


def test_compress(dataset):
    assert dataset.data.encoding["compressor"] is None
    compress = transformers.compress(["data"])
    compress(dataset)

    assert isinstance(dataset.data.encoding["compressor"], numcodecs.Blosc)


@pytest.fixture
def dataset():
    latitude = numpy.arange(-50, 50, 10)
    longitude = numpy.arange(90, 240, 15)
    time = numpy.arange(
        numpy.datetime64("2010-05-12", "ns"),
        numpy.datetime64("2010-05-14", "ns"),
        numpy.timedelta64(1, "D"),
    )
    data = numpy.random.randn(len(time), len(latitude), len(longitude))
    return mock_dataset(
        data=("data", data),
        dims=[("time", time), ("latitude", latitude), ("longitude", longitude)],
    )
