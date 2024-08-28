from dc_etl import combine
from dc_etl.extractors import netcdf
from dc_etl.fetchers import cpc
from dc_etl.filespec import file
from dc_etl.ipld.loader import IPLDLoader
from dc_etl.ipld.local_file import LocalFileIPLDPublisher
from dc_etl.pipeline import Pipeline
from dc_etl.transform import identity

from .conftest import HERE


class TestPipeline:
    def test_from_yaml(self):
        pipeline = Pipeline.from_yaml(HERE / "etc" / "pipeline.yaml")
        assert isinstance(pipeline, Pipeline)

        assert (
            pipeline.fetcher.base_url
            == "http://example.com/datasets/price_of_tea_in_china"
        )
        assert pipeline.fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert pipeline.extractor.foo == "bar"
        assert pipeline.combiner.arg == "value"
        assert len(pipeline.transformer.transformers) == 2
        assert pipeline.transformer.transformers[0].mut == "ate"
        assert pipeline.transformer.transformers[1].trans == "form"
        assert pipeline.loader.load == "it"

    def test_from_yaml_no_transformer(self):
        pipeline = Pipeline.from_yaml("etc/pipeline_no_transformer.yaml")
        assert isinstance(pipeline, Pipeline)

        assert (
            pipeline.fetcher.base_url
            == "http://example.com/datasets/price_of_tea_in_china"
        )
        assert pipeline.fetcher.download_folder == "/opt/rawdata/price_of_tea_in_china"
        assert pipeline.extractor.foo == "bar"
        assert pipeline.combiner.arg == "value"
        assert pipeline.transformer is identity
        assert pipeline.loader.load == "it"

    def test_from_yaml_cpc(self):
        path = file("etc/cpc.yaml")
        precip_global = Pipeline.from_yaml(path)

        assert isinstance(precip_global.fetcher, cpc.CPCFetcher)
        assert precip_global.fetcher._cache.path == "some/path/to/somewhere"
        assert precip_global.fetcher._cache.fs.auto_mkdir is True

        assert isinstance(precip_global.extractor, netcdf.NetCDFExtractor)
        assert precip_global.extractor.inline_threshold == 42
        assert precip_global.extractor.output_folder.path == "output/folder"

        assert isinstance(precip_global.combiner, combine.DefaultCombiner)
        assert precip_global.combiner.output_folder.path == "combined/output"
        assert precip_global.combiner.concat_dims == ["time"]
        assert precip_global.combiner.identical_dims == ["latitude", "longitude"]
        assert len(precip_global.combiner.preprocessors) == 1
        assert precip_global.combiner.preprocessors[0].__name__ == "fix_fill_value"

        assert len(precip_global.transformer.transformers) == 2
        assert precip_global.transformer.transformers[0].__name__ == "rename_dims"
        assert (
            precip_global.transformer.transformers[1].__name__ == "normalize_longitudes"
        )

        assert isinstance(precip_global.loader, IPLDLoader)
        assert precip_global.loader.time_dim == "time"
        assert isinstance(precip_global.loader.publisher, LocalFileIPLDPublisher)
        assert precip_global.loader.publisher.path == "cid/goes/here"
