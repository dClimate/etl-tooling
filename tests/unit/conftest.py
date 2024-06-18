import os
import pathlib

from unittest import mock

import pytest

from dc_etl.extract import Extractor


@pytest.fixture(autouse=True)
def use_this_folder_as_cwd_for_tests():
    # Save the old cwd
    prev = os.getcwd()

    # Where is here?
    here = pathlib.Path(__file__).absolute().parent
    os.chdir(here)

    # Wait for test to finish then change back
    yield
    os.chdir(prev)


def extractor_entry_point(config):
    extractor = mock.Mock(spec=Extractor)
    extractor.__dict__.update(config.config)
    return extractor
