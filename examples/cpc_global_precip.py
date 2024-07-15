"""
An example using CPC Global Precipitation data.

This example uses the imperative config option, which allows you to construct a pipeline in Python code. For an example
that uses a YAML config file instead of Python code, see the CPC US Precipitation example.
"""

import os
import pathlib

from dc_etl import filespec, component
from dc_etl.pipeline import Pipeline

import cli

HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "cached_data" / "cpc" / "global_precip"


def main():
    """Run the example.

    Other than how they're configured, the two examples aren't particularly different, so we use the same command
    line interface code for both."""
    pipeline = Pipeline(
        fetcher=component.fetcher("cpc", dataset="global_precip", cache=CACHE),
        extractor=component.extractor("netcdf"),
        combiner=component.combiner(
            "default",
            output_folder=CACHE / "combined",
            concat_dims=["time"],
            identical_dims=["lat", "lon"],
            preprocessors=[component.combine_preprocessor("fix_fill_value", -9.96921e36)],
        ),
        transformer=component.transformer(
            "composite",
            component.transformer("rename_dims", {"lat": "latitude", "lon": "longitude"}),
            component.transformer("normalize_longitudes"),
            component.transformer("compress", ["precip"]),
        ),
        loader=component.loader(
            "ipld", time_dim="time", publisher=component.ipld_publisher("local_file", CACHE / "zarr_head.cid")
        ),
    )

    cli.main(pipeline)


if __name__ == "__main__":
    main()
