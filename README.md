===========================
dClimate ETL Pipeline Tools
===========================

This package contains tools for creating ETL pipelines for loading gridded data into large, distributed ZARRs.

The documentation (and this code) is still a work in progress.

Quickstart
----------

Create a python virtual environment with the current version supported by this library, python 3.11.
```sh
$ uv venv --python 3.11
```

Then, in the directory where you've checked out this repo::

    $ pip install -e .[all]

To run unit tests::

    $ pytest tests/unit

To run system tests (requires a local running IPFS daemon)::

    $ pytest tests/system

To run all acceptance tests::

    $ nox

There are examples in the `examples` folder that illustrate how this package is used. You can run an example ETL for
`cpc_us_precip`, for instance, by::

    $ python examples/cpc_us_precip.py
