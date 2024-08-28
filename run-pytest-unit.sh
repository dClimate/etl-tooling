#!/bin/bash

pytest --cov=dc_etl --cov=tests.unit --cov-append --cov-config .coveragerc --cov-report=term-missing tests/unit
