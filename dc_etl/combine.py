from __future__ import annotations

import abc

import orjson
import xarray

from kerchunk import combine

from .filespec import FileSpec


class Combiner(abc.ABC):
    """Responsible for merging Single Zarr JSONs into a MultiZarr and performing any necessary transformations.

    Although this is a pluggable component, there is a default implementation which is probably sufficient for most
    cases.
    """

    @abc.abstractmethod
    def __call__(self, sources: list[FileSpec], dest: FileSpec) -> xarray.Dataset:
        """Generate a MultiZarr from single Zarr JSONs.

        Parameters
        ----------
        sources: list[FileSpec]
            List of individual zarr json files to combine
        dest: FileSepc
            Location to write combined zarr json to

        Returns
        -------
        xarray.Dataset :
            The combined zarr json opened as an Xarray Dataset
        """


class DefaultCombiner(Combiner):
    """Default combiner.

    Delegates transformation to preprocessors and postprocessors that are passed in at initialization time and are, in
    turn, passed into Kerchunk's MultiZarrToZarr implementation.

    Parameters
    ----------
    output : FileSpec
        Location of file to write combined Zarr JSON to

    concat_dims : list[str]
        Names of the dimensions to expand with

    identical_dims : list[str]
        Variables that are to be copied across from the first input dataset, because they do not vary

    preprocessors : list[Preprocessor]
        Preprocessors to apply when combining data. Preproccessors are callables of the same kind used by
        `kerchunk.MultiZarrToZarr`. What little documentation there is of them seems to be here:
        https://fsspec.github.io/kerchunk/tutorial.html#preprocessing


    postprocessors : list[Postprocessor]
        Postprocessors to apply when combining data. Postproccessors are callables of the same kind used by
        `kerchunk.MultiZarrToZarr`. What little documentation there is of them seems to be here:
        https://fsspec.github.io/kerchunk/tutorial.html#postprocessing
    """

    @classmethod
    def _from_config(cls, config) -> DefaultCombiner:
        config["preprocessors"] = [pre.as_component("combine_preprocessor") for pre in config.get("preprocessors", ())]
        config["postprocessors"] = [
            post.as_component("combine_postprocessor") for post in config.get("postprocessors", ())
        ]
        return cls(**config)

    def __init__(
        self,
        output: FileSpec,
        concat_dims: list[str],
        identical_dims: list[str],
        preprocessors: list[CombinePreprocessor] = (),
        postprocessors: list[CombinePostprocessor] = (),
    ):
        self.output = output
        self.concat_dims = concat_dims
        self.identical_dims = identical_dims
        self.preprocessors = preprocessors
        self.postprocessors = postprocessors

    def __call__(self, sources: list[FileSpec]) -> xarray.Dataset:
        """Implementation of meth:`Combiner.__call__`.

        Calls `kerchunk.MultiZarrToZarr`.
        """

        def preprocessor(preprocessors):
            def preprocess(refs):
                for processor in preprocessors:
                    refs = processor(refs)

                return refs

            return preprocess

        def postprocessor(postprocessors):
            def postprocess(dataset):
                for processor in postprocessors:
                    dataset = processor(dataset)

                return dataset

            return postprocess

        ensemble = combine.MultiZarrToZarr(
            sources,
            remote_protocol=sources[0].fs.protocol[0],  # Does this always work for protocol or just for "file"?
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            preprocess=preprocessor(self.preprocessors),
            postprocess=postprocessor(self.postprocessors),
        )

        with self.output.open("wb") as f_out:
            f_out.write(orjson.dumps(ensemble.translate()))

        return xarray.open_dataset(
            "reference://",
            engine="zarr",
            backend_kwargs={
                "consolidated": False,
                "storage_options": {
                    "fo": self.output.path,
                    "remote_protocol": self.output.fs.protocol[0],  # Does this always work for protocol?
                },
            },
        )


# Note that the Kerchunk examples for pre and post processors seem to both mutate the input argument and return it
# which is confusing and bad practice. Generally you want it to be clear whether a function is a "pure function" with
# immutable arguments or if it is called for its side-effects. Because Kerchunk fails to make this distinction clear,
# we inherit that muddiness here.


class CombinePreprocessor(abc.ABC):
    """See: https://fsspec.github.io/kerchunk/tutorial.html#preprocessing"""

    @abc.abstractmethod
    def __call__(self, refs: dict) -> dict:
        """See: https://fsspec.github.io/kerchunk/tutorial.html#preprocessing"""


class CombinePostprocessor(abc.ABC):
    """See: https://fsspec.github.io/kerchunk/tutorial.html#postprocessing"""

    @abc.abstractmethod
    def __call__(self, refs: dict) -> dict:
        """See: https://fsspec.github.io/kerchunk/tutorial.html#postprocessing"""
