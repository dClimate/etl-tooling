import pathlib

import fsspec
import pytest

from dc_etl.filespec import FileSpec


@pytest.fixture
def cache():
    system_tests = pathlib.Path(__file__).absolute().parent
    return FileSpec(fsspec.filesystem("file", auto_mkdir=True), str(system_tests)) / "cached_data"
