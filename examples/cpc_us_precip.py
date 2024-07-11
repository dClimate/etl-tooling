"""
An example using CPC US Precipitation data.

This example uses the declaritive config option, which allows you to construct a pipeline from a YAML config file. This
is not required. For an example that uses Python code instead of YAML, see the CPC Global Precipitation example.
"""

import os
import pathlib

from dc_etl import filespec
from dc_etl.pipeline import Pipeline

import cli

HERE = pathlib.Path(__file__).parent
CONFIG_FILE = HERE / "cpc_us_precip.yaml"

os.chdir(HERE)


def main():
    """Run the example.

    Other than how they're configured, the two examples aren't particularly different, so we use the same command
    line interface code for both."""
    pipeline = Pipeline.from_yaml(filespec.file(CONFIG_FILE))
    cli.main(pipeline)


if __name__ == "__main__":
    main()
