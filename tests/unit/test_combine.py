from unittest import mock

import fsspec
import orjson

from dc_etl import combine as combine_module
from dc_etl import filespec


class MockMultiZarrToZarr:

    def __init__(self, sources, remote_protocol, concat_dims, identical_dims, preprocessed, postprocessed):
        self.expected_sources = sources
        self.expected_remote_protocol = remote_protocol
        self.expected_concat_dims = concat_dims
        self.expected_identical_dims = identical_dims
        self.expected_preprocessed = preprocessed
        self.expected_postprocessed = postprocessed

    def __call__(self, sources, remote_protocol, concat_dims, identical_dims, preprocess, postprocess):
        assert self.expected_sources == sources
        assert self.expected_remote_protocol == remote_protocol
        assert self.expected_concat_dims == concat_dims
        assert self.expected_identical_dims == identical_dims

        assert preprocess("hello there") == self.expected_preprocessed
        assert postprocess("goodbye") == self.expected_postprocessed

        return self

    def translate(self):
        return {"hi": "mom!"}


class TestDefaultCombiner:

    def test___call__(self, tmpdir, mocker):
        kerchunk = mocker.patch("dc_etl.combine.combine")
        xarray = mocker.patch("dc_etl.combine.xarray")

        pre1 = mock.Mock(return_value="what's your name?")
        pre2 = mock.Mock(return_value="my name is george")
        post1 = mock.Mock(return_value="i have 5 dollars")
        post2 = mock.Mock(return_value="i have a cheeseburger")

        source1 = filespec.FileSpec(fsspec.filesystem("file"), "path/one")
        source2 = filespec.FileSpec(fsspec.filesystem("file"), "path/two")
        outfile = filespec.FileSpec(fsspec.filesystem("file"), str(tmpdir)) / "output_zarr.json"

        kerchunk.MultiZarrToZarr = MockMultiZarrToZarr(
            [source1, source2], "file", ["a", "b"], ["c", "d"], "my name is george", "i have a cheeseburger"
        )

        combine = combine_module.DefaultCombiner(
            outfile,
            ["a", "b"],
            ["c", "d"],
            preprocessors=[pre1, pre2],
            postprocessors=[post1, post2],
        )

        assert combine([source1, source2]) == xarray.open_dataset.return_value
        pre1.assert_called_once_with("hello there")
        pre2.assert_called_once_with("what's your name?")
        post1.assert_called_once_with("goodbye")
        post2.assert_called_once_with("i have 5 dollars")

        assert orjson.loads(outfile.open().read()) == {"hi": "mom!"}
