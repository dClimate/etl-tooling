from __future__ import annotations

import fsspec
import pathlib


from dc_etl.combine import Combiner
from dc_etl.config import _Configuration
from dc_etl.extract import Extractor
from dc_etl.fetch import Fetcher
from dc_etl.filespec import FileSpec
from dc_etl.transform import Transformer, identity


class Pipeline:
    """An ETL pipeline.

    Parameters
    ----------
    fetcher: Fetcher
        The fetcher to use to get data from the source provider.
    extractor: Extractor
        The extractor used to convert source files to single Zarr JSONs.
    combiner: Combiner
        The combiner used to merge single Zarr JSONs into a MultiZarr JSON.
    transformer: Transformer | None
        An optional transformer to massage dataset after combining, before loading.
    """

    @classmethod
    def from_yaml(cls, path: pathlib.Path | FileSpec) -> Pipeline:
        """Import configuration from a yaml file."""
        if not isinstance(path, FileSpec):
            path = FileSpec(fsspec.filesystem("file"), str(path))

        config = _Configuration.from_yaml(path)
        return cls._from_config(config)

    @classmethod
    def _from_config(cls, config: _Configuration) -> Pipeline:
        fetcher = config["fetcher"].as_component("fetcher")
        extractor = config["extractor"].as_component("extractor")
        combiner = config["combiner"].as_component("combiner")
        if "transformer" in config:
            transformer = config["transformer"].as_component("transformer")
        else:
            transformer = identity

        return Pipeline(fetcher, extractor, combiner, transformer)

    def __init__(self, fetcher: Fetcher, extractor: Extractor, combiner: Combiner, transformer: Transformer):
        self.fetcher = fetcher
        self.extractor = extractor
        self.combiner = combiner
        self.transformer = transformer
