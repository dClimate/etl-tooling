"""Test something a lot like a real pipeline for CPC.
"""

import itertools
import numcodecs

from dc_etl.fetch import Timespan
from dc_etl.filespec import file
from dc_etl.pipeline import Pipeline

from tests.conftest import npdate


def test_declarative_configuration():
    path = file("pipelines/cpc_us_precip.yaml")
    pipeline = Pipeline.from_yaml(path)

    # Initial dataset
    span = Timespan(npdate(1982, 11, 29), npdate(1984, 6, 25))  # Thriller, Purple Rain
    sources = pipeline.fetcher.fetch(span)
    extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    dataset = pipeline.combiner(extracted)
    dataset = pipeline.transformer(dataset)
    pipeline.loader.initial(dataset, span)

    dataset = pipeline.loader.dataset()
    assert dataset.time[0] == npdate(1982, 11, 29)
    assert dataset.time[-1] == npdate(1984, 6, 25)
    assert isinstance(dataset.precip.encoding["compressor"], numcodecs.Blosc)

    # Append some more data
    span = Timespan(npdate(1984, 6, 26), npdate(1985, 9, 30))  # Purple Rain, Rain Dogs
    sources = pipeline.fetcher.fetch(span)
    extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    dataset = pipeline.combiner(extracted)
    dataset = pipeline.transformer(dataset)
    pipeline.loader.append(dataset, span)

    dataset = pipeline.loader.dataset()
    assert dataset.time[0] == npdate(1982, 11, 29)
    assert dataset.time[-1] == npdate(1985, 9, 30)

    # Change some data and insert it
    date = npdate(1984, 12, 25)
    span = Timespan(date, date)
    sources = pipeline.fetcher.fetch(span)
    extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    dataset = pipeline.combiner(extracted)
    dataset = pipeline.transformer(dataset)

    old_value = dataset.precip.sel(time=date, latitude=45.125, longitude=-104.875).copy()
    new_value = 8888.875
    dataset.precip.loc[dict(time=date, latitude=45.125, longitude=-104.875)] = new_value
    assert old_value != new_value
    assert dataset.precip.sel(time=date, latitude=45.125, longitude=-104.875) == new_value

    pipeline.loader.replace(dataset, span)

    dataset = pipeline.loader.dataset()
    assert dataset.precip.sel(time=date, latitude=45.125, longitude=-104.875) == new_value
