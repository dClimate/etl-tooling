from dc_etl.combine import Combiner
from dc_etl.extract import Extractor
from dc_etl.fetch import Fetcher


class Dataset:
    """A dataset and its associated ETL pipeline.

    Parameters
    ----------

    name : str
        The name of the dataset.
    fetcher: Fetcher
        The fetcher to use to get data from the source provider.
    extractor: Extractor
        The extractor used to convert source files to single Zarr JSONs.
    combiner: Combiner
        The combiner used to merge single Zarr JSONs into a MultiZarr JSON.
    """

    def __init__(self, name: str, fetcher: Fetcher, extractor: Extractor, combiner: Combiner):
        self.name = name
        self.fetcher = fetcher
        self.extractor = extractor
        self.combiner = combiner
