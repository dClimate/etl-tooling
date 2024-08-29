#!/bin/bash

# Checks if a python virtual environment is already set
if [[ ! -v VIRTUAL_ENV ]]; then
    # Virtual env is not activated, activate it
    echo "MY_VARIABLE is unset"
    source .venv/bin/activate
fi
coverage report --fail-under=100 --show-missing
coverage erase
