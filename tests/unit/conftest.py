import io
import os
import pathlib

from unittest import mock

import pytest
import xarray

from dc_etl.fetch import Fetcher


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


def fetcher_entry_point(**config):
    fetcher = mock.Mock(spec=Fetcher)
    fetcher.__dict__.update(config)
    return fetcher


class MockFilesystem:

    def __init__(self, contents: dict[str, bytes]):
        self.contents = contents

    def glob(self, glob):
        start, end = glob.split("*")
        return [name for name in self.contents if name.startswith(start) and name.endswith(end)]

    def open(self, path, mode="rb"):
        assert mode.endswith("b")
        return io.BytesIO(self.contents[path])

    def get_file(self, path, dest):
        dest.write(self.contents[path])


def mock_dataset(data, dims):
    """Returns a serialized xarray dataset using supplied data and dimensions."""
    coords = []
    coord_names = []
    for name, dim in dims:
        coords.append(xarray.DataArray(dim, dims=name, coords={name: dim}))
        coord_names.append(name)

    name, data = data
    data = xarray.DataArray(data, dims=coord_names, coords=coords)
    ds = xarray.Dataset({name: data})
    buf = io.BytesIO()
    serialized = ds.to_netcdf(buf)
    return serialized
