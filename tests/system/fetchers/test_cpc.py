import numpy
import pytest
import xarray

from dc_etl.fetch import Timespan
from dc_etl.fetchers.cpc import CPCFetcher

from ...conftest import npdate


class TestCPCFetcher:

    @pytest.mark.parametrize(
        "dataset,start",
        [
            ("global_precip", npdate(1979, 1, 1)),
            ("us_precip", npdate(1948, 1, 1)),
            ("global_temp_max", npdate(1979, 1, 1)),
            ("global_temp_min", npdate(1979, 1, 1)),
        ],
    )
    def test_get_remote_timespan(self, dataset, start, cache):
        fetcher = CPCFetcher(dataset, cache / "cpc" / dataset)
        span = fetcher.get_remote_timespan()

        nyd_2024 = numpy.datetime64("2024-01-01")
        assert span.start == start
        assert span.end >= nyd_2024

    @pytest.mark.parametrize("dataset", ["global_precip", "us_precip", "global_temp_max", "global_temp_min"])
    def test_fetch(self, dataset, cache):
        fetcher = CPCFetcher(dataset, cache / "cpc" / dataset)
        span = Timespan(npdate(1982, 11, 29), npdate(1984, 6, 25))  # Thriller, Purple Rain
        files = list(fetcher.fetch(span))
        for file, year in zip(files, range(1982, 1985)):
            ds = xarray.load_dataset(file.open())
            assert ds.time[0].values == npdate(year, 1, 1)
            assert ds.time[-1].values == npdate(year, 12, 31)

    @pytest.mark.parametrize("dataset", ["global_precip", "us_precip", "global_temp_max", "global_temp_min"])
    def test_prefetch(self, dataset, cache):
        cache = cache / "cpc" / dataset
        fetcher = CPCFetcher(dataset, cache)
        span = Timespan(npdate(2000, 10, 2), npdate(2001, 5, 30))  # Kid A, Amnesiac
        fetcher.prefetch(span)
        for year in range(2000, 2002):
            assert len(cache.fs.glob(f"{cache.path}/*{year}.nc")) == 1
