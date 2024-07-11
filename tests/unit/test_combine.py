from unittest import mock

import fsspec
import orjson

from dc_etl import combine as combine_module
from dc_etl import filespec
from dc_etl.config import _Configuration


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

    def test__from_config(self):
        config = _Configuration(
            {
                "output_folder": "put/it/here",
                "concat_dims": ["a"],
                "identical_dims": ["b", "c"],
                "preprocessors": [{"name": "testing", "one": "two"}, {"name": "testing", "three": "four"}],
                "postprocessors": [{"name": "testing", "five": "six"}, {"name": "testing", "seven": "eight"}],
            },
            "some/config/file",
            [],
        )
        combiner = combine_module.DefaultCombiner._from_config(config)
        assert combiner.output_folder == "put/it/here"
        assert combiner.concat_dims == ["a"]
        assert combiner.identical_dims == ["b", "c"]
        assert combiner.preprocessors[0].one == "two"
        assert combiner.preprocessors[1].three == "four"
        assert combiner.postprocessors[0].five == "six"
        assert combiner.postprocessors[1].seven == "eight"

    def test___call__(self, tmpdir, mocker):
        kerchunk = mocker.patch("dc_etl.combine.combine")
        xarray = mocker.patch("dc_etl.combine.xarray")
        time = mocker.patch("dc_etl.combine.time")
        time.time.return_value = 42

        pre1 = mock.Mock(return_value="what's your name?")
        pre2 = mock.Mock(return_value="my name is george")
        post1 = mock.Mock(return_value="i have 5 dollars")
        post2 = mock.Mock(return_value="i have a cheeseburger")

        source1 = filespec.FileSpec(fsspec.filesystem("file"), "path/one")
        source2 = filespec.FileSpec(fsspec.filesystem("file"), "path/two")
        outfile = filespec.FileSpec(fsspec.filesystem("file"), str(tmpdir)) / "combined_zarr_42.json"

        kerchunk.MultiZarrToZarr = MockMultiZarrToZarr(
            [source1.path, source2.path], "file", ["a", "b"], ["c", "d"], "my name is george", "i have a cheeseburger"
        )

        combine = combine_module.DefaultCombiner(
            outfile.parent,
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
