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
    def fetch(self, span: Timespan, **kwargs) -> typing.Generator[filespec.FileSpec, None, None]:
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
