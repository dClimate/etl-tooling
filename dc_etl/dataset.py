from dc_etl.fetch import Fetcher


class Dataset:
    """A dataset and its associated ETL pipeline.

    Parameters
    ----------

    name : str
        The name of the dataset.
    extractor: Extractor
        The extractor to use to get data from the source provider.
    """

    def __init__(self, name: str, fetcher: Fetcher):
        self.name = name
        self.fetcher = fetcher
