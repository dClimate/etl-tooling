import orjson

from dc_etl.combine_preprocessors import fix_fill_value


def test_fix_fill_value(zarr_json):
    def get_fill_value(refvalue):
        return orjson.loads(refvalue)["fill_value"]

    fill_value = -8888
    fix = fix_fill_value(fill_value)
    prev = zarr_json["refs"]
    fixed = fix(prev.copy())

    for ref in prev:
        if not ref.endswith(".zarray"):
            continue

        assert get_fill_value(prev[ref]) != fill_value
        assert get_fill_value(fixed[ref]) == fill_value
