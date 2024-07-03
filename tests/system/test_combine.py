from dc_etl import config
from dc_etl.fetch import Timespan

from tests.conftest import npdate


class TestDefaultCombiner:
    def test_transform(self, cache):
        cache = cache / "cpc" / "us_precip"
        span = Timespan(npdate(1982, 11, 29), npdate(1984, 6, 25))  # Thriller, Purple Rain
        sources = config.fetcher("cpc", "us_precip", cache=cache).fetch(span)
        extractor = config.extractor("netcdf")
        zarr_jsons = list(map(extractor.extract, sources))
        multizarr = cache / "cpc" / "us_precip" / "combined-cpc-us-precip-1982-1984.json"
        combine = config.combiner(
            "default",
            multizarr,
            concat_dims=["time"],
            identical_dims=["latitude", "longitude"],
            preprocessors=[config.combine_preprocessor("fix_fill_value", -9.96921e36)],
        )
        dataset = combine(zarr_jsons)
        assert dataset.time[0] == npdate(1982, 1, 1)
        assert dataset.time[-1] == npdate(1984, 12, 31)
