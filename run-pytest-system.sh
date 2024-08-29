#!/bin/bash

# Checks if a python virtual environment is already set
if [[ ! -v VIRTUAL_ENV ]]; then
    # Virtual env is not activated, activate it
    echo "MY_VARIABLE is unset"
    source .venv/bin/activate
fi
pytest --cov=tests.system --cov-config .coveragerc --cov-report=term-missing --cov-fail-under=100 tests/system
