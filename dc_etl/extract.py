import abc
import typing

from dc_etl.filespec import FileSpec


class Extractor(abc.ABC):
    """Responsible for taking a raw source file and turning it into a single Zarr JSON."""

    @abc.abstractmethod
    def __call__(self, source: FileSpec, **kwargs) -> typing.Generator[FileSpec, None, None]:
        """Extract a source data file into one or more single Zarr JSON files.

        Parameters
        ----------
        source: FileSpec
            The source data file will be loaded from here.

        Returns
        -------
        FileSpec
            Location of a written Zarr JSON file.
        """
