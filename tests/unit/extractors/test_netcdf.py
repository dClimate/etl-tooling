from unittest import mock

import orjson

from dc_etl.extractors.netcdf import NetCDFExtractor
from dc_etl.filespec import file


class TestNetCDFExtractor:

    def test_constructor_defaults(self):
        extractor = NetCDFExtractor()
        assert extractor.output_folder is None
        assert extractor.inline_threshold == 5000

    def test_constructor_explicit_args(self):
        extractor = NetCDFExtractor("output_folder", "10 gazillion")
        assert extractor.output_folder == "output_folder"
        assert extractor.inline_threshold == "10 gazillion"

    def test_extract(self, mocker):
        hdf = mocker.patch("dc_etl.extractors.netcdf.hdf")
        zarr_json = hdf.SingleHdf5ToZarr.return_value
        zarr_json.translate.return_value = {"hi": "mom"}

        source = file("/data/file.nc", "memory")
        mock_source = mock.Mock(wraps=source, open=mock.MagicMock(), path=source.path)
        src = mock_source.open.return_value.__enter__.return_value

        extractor = NetCDFExtractor()
        extracted = next(extractor(mock_source))

        assert orjson.loads(extracted.open().read()) == {"hi": "mom"}
        assert extracted.path == "/data/file.json"
        hdf.SingleHdf5ToZarr.assert_called_once_with(src, "/data/file.nc", inline_threshold=5000)

    def test_extract_with_output_folder_and_inline_threshold(self, mocker):
        hdf = mocker.patch("dc_etl.extractors.netcdf.hdf")
        zarr_json = hdf.SingleHdf5ToZarr.return_value
        zarr_json.translate.return_value = {"hi": "mom"}

        source = file("/data/file.nc", "memory")
        output = file("/extracted", "memory")
        mock_source = mock.Mock(wraps=source, open=mock.MagicMock(), path=source.path)
        mock_source.name = source.name
        src = mock_source.open.return_value.__enter__.return_value

        extractor = NetCDFExtractor(output_folder=output, inline_threshold=42)
        extracted = next(extractor(mock_source))

        assert orjson.loads(extracted.open().read()) == {"hi": "mom"}
        assert extracted.path == "/extracted/file.json"
        hdf.SingleHdf5ToZarr.assert_called_once_with(src, "/data/file.nc", inline_threshold=42)
