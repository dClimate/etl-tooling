import fsspec
import numpy
import os
import pytest
import xarray

from dc_etl.errors import ConfigurationError
from dc_etl.fetch import Timespan
from dc_etl.fetchers.cpc import CPCFetcher
from dc_etl.filespec import FileSpec

from ..conftest import mock_dataset, MockFilesystem


class TestCPCFetcher:

    def test_constructor(self, mocker):
        fsspec = mocker.patch("dc_etl.fetchers.cpc.fsspec")
        fetcher = CPCFetcher("global_precip")
        assert fetcher._glob == ["/Datasets/cpc_global_precip/precip.*.nc"]
        assert fetcher._fs == fsspec.filesystem.return_value
        assert fetcher._cache is None
        fsspec.filesystem.assert_called_once_with("ftp", host="ftp.cdc.noaa.gov")

    def test_constructor_with_cache(self, mocker):
        fsspec = mocker.patch("dc_etl.fetchers.cpc.fsspec")
        cache = object()
        fetcher = CPCFetcher("us_precip", cache)
        assert fetcher._glob == [
            "/Datasets/cpc_us_precip/precip.V1.0.*.nc",
            "/Datasets/cpc_us_precip/RT/precip.V1.0.*.nc",
        ]
        assert fetcher._fs == fsspec.filesystem.return_value
        assert fetcher._cache is cache
        fsspec.filesystem.assert_called_once_with("ftp", host="ftp.cdc.noaa.gov")

    def test_constructor_with_bad_dataset(self):
        with pytest.raises(ConfigurationError):
            CPCFetcher("no such dataset")

    @pytest.mark.usefixtures("patch_fs")
    def test_get_remote_timespan(self):
        fetcher = CPCFetcher("us_precip")
        span = fetcher.get_remote_timespan()
        assert span.start == numpy.datetime64("1970-01-01")
        assert span.end == numpy.datetime64("1972-12-31")

    @pytest.mark.usefixtures("patch_fs")
    def test_prefetch_noop(self):
        fetcher = CPCFetcher("us_precip")
        span = Timespan(numpy.datetime64("1971-05-12"), numpy.datetime64("1972-07-07"))

        # Doesn't do anything, so not really anything to assert. Just a smoke test
        fetcher.prefetch(span)

    @pytest.mark.usefixtures("patch_fs")
    def test_prefetch_with_cache(self, tmpdir):
        cache = FileSpec(fsspec.filesystem("file"), str(tmpdir))
        fetcher = CPCFetcher("us_precip", cache)
        span = Timespan(numpy.datetime64("1971-05-12"), numpy.datetime64("1972-07-07"))
        fetcher.prefetch(span)

        prefetched = os.listdir(tmpdir)
        assert len(prefetched) == 2
        assert "precip.V1.0.1971.nc" in prefetched
        assert "precip.V1.0.1972.nc" in prefetched

    @pytest.mark.usefixtures("patch_fs")
    def test_fetch(self, mockfs):
        fetcher = CPCFetcher("us_precip")
        span = Timespan(numpy.datetime64("1971-05-12"), numpy.datetime64("1972-07-07"))

        files = list(fetcher.fetch(span))
        assert len(files) == 2
        assert files[0].fs is mockfs
        assert files[0].path == "/Datasets/cpc_us_precip/precip.V1.0.1971.nc"
        assert files[1].fs is mockfs
        assert files[1].path == "/Datasets/cpc_us_precip/RT/precip.V1.0.1972.nc"

        fetched = [xarray.open_dataset(file.open()) for file in files]
        assert fetched[0].time.data[0] == numpy.datetime64("1971-01-01")
        assert fetched[0].time.data[-1] == numpy.datetime64("1971-12-31")
        assert fetched[1].time.data[0] == numpy.datetime64("1972-01-01")
        assert fetched[1].time.data[-1] == numpy.datetime64("1972-12-31")

    @pytest.mark.usefixtures("patch_fs")
    def test_fetch_with_cache(self, tmpdir, mocker):
        cache = FileSpec(fsspec.filesystem("file"), str(tmpdir))

        fetcher = CPCFetcher("us_precip", cache)
        span = Timespan(numpy.datetime64("1971-05-12"), numpy.datetime64("1972-07-07"))

        files = list(fetcher.fetch(span))
        assert len(files) == 2
        assert files[0].fs is cache.fs
        assert files[0].path == f"{tmpdir}/precip.V1.0.1971.nc"
        assert files[1].fs is cache.fs
        assert files[1].path == f"{tmpdir}/precip.V1.0.1972.nc"

        fetched = [xarray.open_dataset(file.open()) for file in files]
        assert fetched[0].time.data[0] == numpy.datetime64("1971-01-01")
        assert fetched[0].time.data[-1] == numpy.datetime64("1971-12-31")
        assert fetched[1].time.data[0] == numpy.datetime64("1972-01-01")
        assert fetched[1].time.data[-1] == numpy.datetime64("1972-12-31")

        # This time should only use cache
        mocker.patch("dc_etl.fetchers.cpc.fsspec", None)

        files = list(fetcher.fetch(span))
        assert len(files) == 2
        assert files[0].fs is cache.fs
        assert files[0].path == f"{tmpdir}/precip.V1.0.1971.nc"
        assert files[1].fs is cache.fs
        assert files[1].path == f"{tmpdir}/precip.V1.0.1972.nc"

        fetched = [xarray.open_dataset(file.open()) for file in files]
        assert fetched[0].time.data[0] == numpy.datetime64("1971-01-01")
        assert fetched[0].time.data[-1] == numpy.datetime64("1971-12-31")
        assert fetched[1].time.data[0] == numpy.datetime64("1972-01-01")
        assert fetched[1].time.data[-1] == numpy.datetime64("1972-12-31")


@pytest.fixture(scope="session")
def mockfs():
    return MockFilesystem(
        {
            "/Datasets/cpc_us_precip/precip.V1.0.1970.nc": cpc_us_precip(1970),
            "/Datasets/cpc_us_precip/precip.V1.0.1971.nc": cpc_us_precip(1971),
            "/Datasets/cpc_us_precip/RT/precip.V1.0.1971.nc": b"should be superceded by above",
            "/Datasets/cpc_us_precip/RT/precip.V1.0.1972.nc": cpc_us_precip(1972),
            "/Datasets/cpc_us_precip/precip.V1.0.whut.nc": b"should be excluded by regular expression",
        }
    )


@pytest.fixture
def patch_fs(mocker, mockfs):
    fsspec = mocker.patch("dc_etl.fetchers.cpc.fsspec")
    fsspec.filesystem.return_value = mockfs


def cpc_us_precip(year):
    """Emulate basic structure of a CPC US precip dataset."""
    lat = numpy.arange(20.125, 50, 0.25)
    lon = numpy.arange(230.125, 305, 0.25)

    time = numpy.arange(
        numpy.datetime64(f"{year}-01-01", "ns"), numpy.datetime64(f"{year + 1}-01-01", "ns"), numpy.timedelta64(1, "D")
    )
    data = numpy.random.randn(len(time), len(lat), len(lon))
    return mock_dataset(data=("precip", data), dims=[("time", time), ("lat", lat), ("lon", lon)])
