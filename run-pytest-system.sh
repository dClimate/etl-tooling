#!/bin/bash

pytest --cov=tests.system --cov-config .coveragerc --cov-report=term-missing --cov-fail-under=100 tests/system
