import abc

from dc_etl.filespec import FileSpec


class Extractor(abc.ABC):
    """Responsible for taking a raw source file and turning it into a single Zarr JSON."""

    @abc.abstractmethod
    def extract(self, source: FileSpec) -> FileSpec:
        """Extract a source data file into a single Zarr JSON file.

        Parameters
        ----------
        source: FileSpec
            The source data file will be loaded from here.

        Returns
        -------
        FileSpec
            Location of a written Zarr JSON file.
        """
