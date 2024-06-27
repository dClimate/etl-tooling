import numpy
import pytest
import xarray

from dc_etl.fetch import Timespan
from dc_etl.fetchers.cpc import CPCFetcher


def date(year: int, month: int, day: int) -> numpy.datetime64:
    return numpy.datetime64(f"{year}-{month:02d}-{day:02d}")


class TestCPCFetcher:

    @pytest.mark.parametrize(
        "dataset,start",
        [
            ("global_precip", date(1979, 1, 1)),
            ("us_precip", date(1948, 1, 1)),
            ("global_temp_max", date(1979, 1, 1)),
            ("global_temp_min", date(1979, 1, 1)),
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
        span = Timespan(date(1982, 11, 29), date(1984, 6, 25))  # Thriller, Purple Rain
        files = list(fetcher.fetch(span))
        for file, year in zip(files, range(1982, 1985)):
            ds = xarray.load_dataset(file.open())
            assert ds.time[0].values == date(year, 1, 1)
            assert ds.time[-1].values == date(year, 12, 31)

    @pytest.mark.parametrize("dataset", ["global_precip", "us_precip", "global_temp_max", "global_temp_min"])
    def test_prefetch(self, dataset, cache):
        cache = cache / "cpc" / dataset
        fetcher = CPCFetcher(dataset, cache)
        span = Timespan(date(2000, 10, 2), date(2001, 5, 30))  # Kid A, Amnesiac
        fetcher.prefetch(span)
        for year in range(2000, 2002):
            assert len(cache.fs.glob(f"{cache.path}/*{year}.nc")) == 1
