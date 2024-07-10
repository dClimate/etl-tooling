import os
import pathlib

import fsspec
import pytest

from dc_etl.filespec import FileSpec

HERE = pathlib.Path(__file__).absolute().parent


@pytest.fixture
def cache():
    system_tests = pathlib.Path(__file__).absolute().parent
    return FileSpec(fsspec.filesystem("file", auto_mkdir=True), str(system_tests)) / "cached_data"


@pytest.fixture(autouse=True)
def use_this_folder_as_cwd_for_tests():
    # Save the old cwd
    prev = os.getcwd()

    os.chdir(HERE)

    # Wait for test to finish then change back
    yield
    os.chdir(prev)
