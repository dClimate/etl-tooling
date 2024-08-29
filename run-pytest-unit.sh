#!/bin/bash

# Checks if a python virtual environment is already set
if [[ ! -v VIRTUAL_ENV ]]; then
    # Virtual env is not activated, activate it
    echo "MY_VARIABLE is unset"
    source .venv/bin/activate
fi
pytest --cov=dc_etl --cov=tests.unit --cov-append --cov-config .coveragerc --cov-report=term-missing tests/unit
