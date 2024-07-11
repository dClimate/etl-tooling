import pytest
import xarray

from dc_etl.extractors.netcdf import NetCDFExtractor
from dc_etl.fetch import Timespan
from dc_etl.fetchers.cpc import CPCFetcher
from dc_etl.filespec import FileSpec

from tests.conftest import npdate


class TestNetCDFExtractor:
    def test_extract(self, cache):
        cache = cache / "cpc" / "us_precip"
        span = Timespan(npdate(1982, 11, 29), npdate(1984, 6, 25))  # Thriller, Purple Rain
        sources = CPCFetcher("us_precip", cache).fetch(span)
        extractor = NetCDFExtractor()
        zarr_jsons = list(map(extractor, sources))
        datasets = [open_dataset(zarr_json) for zarr_json in zarr_jsons]

        assert len(datasets) == 3
        assert datasets[0].time[0] == npdate(1982, 1, 1)
        assert datasets[0].time[-1] == npdate(1982, 12, 31)
        assert datasets[1].time[0] == npdate(1983, 1, 1)
        assert datasets[1].time[-1] == npdate(1983, 12, 31)
        assert datasets[2].time[0] == npdate(1984, 1, 1)
        assert datasets[2].time[-1] == npdate(1984, 12, 31)

        for year in range(1982, 1985):
            assert len(cache.fs.glob(f"{cache.path}/*{year}.json")) == 1

    def test_extract_w_output_folder(self, cache):
        cache = cache / "cpc" / "us_precip"
        output = cache / "zarr_jsons"
        span = Timespan(npdate(1982, 11, 29), npdate(1984, 6, 25))  # Thriller, Purple Rain
        sources = CPCFetcher("us_precip", cache).fetch(span)
        extractor = NetCDFExtractor(output_folder=output)
        zarr_jsons = list(map(extractor, sources))
        datasets = [open_dataset(zarr_json) for zarr_json in zarr_jsons]

        assert len(datasets) == 3
        assert datasets[0].time[0] == npdate(1982, 1, 1)
        assert datasets[0].time[-1] == npdate(1982, 12, 31)
        assert datasets[1].time[0] == npdate(1983, 1, 1)
        assert datasets[1].time[-1] == npdate(1983, 12, 31)
        assert datasets[2].time[0] == npdate(1984, 1, 1)
        assert datasets[2].time[-1] == npdate(1984, 12, 31)

        for year in range(1982, 1985):
            assert len(output.fs.glob(f"{output.path}/*{year}.json")) == 1

    @pytest.mark.skip(reason="This test is slow.")
    def test_extract_wo_cache(self, cache):  # pragma NO COVER
        """This is an experiment to see if we can leave the soruce files on the server and generate usable Zarr JSONS
        without caching source files locally. It's an interesting test because the Zarr JSONs generated refer to the
        source files and it's interesting to know those source files don't have to be local in order to read the Zarr
        JSONs.

        Utlimately, though, this test is pretty slow to run, so we can skip it normally. Also, even though it's
        possible, we will probably most often want to cache data source files near where they will be used.
        """
        output = cache / "cpc" / "us_precip" / "zarr_jsons"
        span = Timespan(npdate(1982, 11, 29), npdate(1984, 6, 25))  # Thriller, Purple Rain
        sources = CPCFetcher("us_precip").fetch(span)
        extractor = NetCDFExtractor(output_folder=output)
        zarr_jsons = list(map(extractor, sources))
        datasets = [open_dataset(zarr_json) for zarr_json in zarr_jsons]

        assert len(datasets) == 3
        assert datasets[0].time[0] == npdate(1982, 1, 1)
        assert datasets[0].time[-1] == npdate(1982, 12, 31)
        assert datasets[1].time[0] == npdate(1983, 1, 1)
        assert datasets[1].time[-1] == npdate(1983, 12, 31)
        assert datasets[2].time[0] == npdate(1984, 1, 1)
        assert datasets[2].time[-1] == npdate(1984, 12, 31)

        for year in range(1982, 1985):
            assert len(output.fs.glob(f"{output.path}/*{year}.json")) == 1


def open_dataset(source: FileSpec):
    return xarray.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": source.path,
                "remote_protocol": "file",
            },
        },
    )
