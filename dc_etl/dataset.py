from dc_etl.extract import Extractor


class Dataset:
    """A dataset and its associated ETL pipeline.

    Parameters
    ----------

    name : str
        The name of the dataset.
    cluster : str
        The name of the cluster being used to host the dataset. (TODO: We probably want to actually integrate with
        `dc_provision` and make this an instance of `dc_provision.cluster.Cluster`.)
    extractor: Extractor
        The extractor to use to get data from the source provider.
    """

    def __init__(self, name: str, cluster: str, extractor: Extractor):
        self.name = name
        self.cluster = cluster
        self.extractor = extractor
