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
    """

    def __init__(self, name: str, fetcher: Fetcher, extractor: Extractor):
        self.name = name
        self.fetcher = fetcher
        self.extractor = extractor
