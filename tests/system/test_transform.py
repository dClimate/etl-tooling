from dc_etl import component
from dc_etl.fetch import Timespan

from tests.conftest import npdate


class TestCompositeTransformer:
    def test_transform(self, cache):
        cache = cache / "cpc" / "us_precip"
        span = Timespan(
            npdate(1982, 11, 29), npdate(1984, 6, 25)
        )  # Thriller, Purple Rain
        sources = component.fetcher("cpc", "us_precip", cache=cache).fetch(span)
        extractor = component.extractor("netcdf")
        zarr_jsons = list(map(extractor, sources))
        multizarr = (
            cache / "cpc" / "us_precip" / "combined-cpc-us-precip-1982-1984.json"
        )
        combine = component.combiner(
            "default",
            multizarr,
            concat_dims=["time"],
            identical_dims=["latitude", "longitude"],
            preprocessors=[
                component.combine_preprocessor("fix_fill_value", -9.96921e36)
            ],
        )
        dataset = combine(zarr_jsons)

        transform = component.transformer(
            "composite",
            component.transformer(
                "rename_dims", {"lat": "latitude", "lon": "longitude"}
            ),
            component.transformer("normalize_longitudes"),
        )

        assert dataset.time[0] == npdate(1982, 1, 1)
        assert dataset.time[-1] == npdate(1984, 12, 31)
        assert dataset.lon[0] == 230.125
        assert dataset.lat[0] == 20.125

        dataset = transform(dataset)

        assert dataset.longitude[0] == -129.875  # Renamed and normalized
        assert dataset.latitude[0] == 20.125  # Renamed
