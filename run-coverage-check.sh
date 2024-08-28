#!/bin/bash

coverage report --fail-under=100 --show-missing
coverage erase
