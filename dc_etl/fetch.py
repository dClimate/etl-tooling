import abc
import typing

import numpy

from . import filespec


class Timespan(typing.NamedTuple):
    start: numpy.datetime64
    end: numpy.datetime64


class Fetcher(abc.ABC):
    """A component responsible for fetching data from a data source and providing it to an Extractor."""

    @abc.abstractmethod
    def get_remote_timespan(self) -> Timespan:
        """Get the timespan available remotely for this dataset.

        Assumes remote dataset represents a single contiguous timespan.


        Returns
        -------
        Timespan :
            Timespan including the first and last timestamps available in the source data.
        """

    @abc.abstractmethod
    def prefetch(self, span: Timespan):
        """Optionally download any files from the remote data provider corresponding to the given timespan.

        For implentations where it makes sense, files may be downloaded and stored locally prior to calling extract.

        Parameters
        ----------
        span : Timespan
            The timespan to download data for.
        """

    @abc.abstractmethod
    def fetch(self, span: Timespan) -> typing.Generator[filespec.FileSpec, None, None]:
        """Extract any files from the remote data provider corresponding to the given timespan.

        Data files from the data provider that correspond to the given timespan will be opened using xarray and yielded
        in chronological order.

        Parameters
        ----------
        span : Timespan
            The timespan to extract data for.

        Returns
        -------

        typing.Generator[xarray.Dataset, None, None] :
            A generator that yields one `filespec.FileSpec` for each source data file from the data provider.
        """
