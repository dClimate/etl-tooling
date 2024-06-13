import abc
import typing

import numpy
import xarray


class Timespan(typing.NamedTuple):
    start: numpy.datetime64
    end: numpy.datetime64


class Extractor(abc.ABC):
    """A component responsible for downloading data from a data source and providing it to a Transformer."""

    def get_remote_timespan(self) -> Timespan:
        """Get the timespan available remotely for this dataset.

        Assumes remote dataset represents a single contiguous timespan.


        Returns
        -------
        Timespan :
            Timespan including the first and last timestamps available in the source data.
        """

    def get_local_timespans(self) -> list[Timespan]:
        """Get all of the timespans that have been downloaded and are available locally.

        The local downloaded data may contain data that is discontiguous in time, so a sequence of spans is returned.

        Returns
        -------
            list[Timespan] :
                List of all of the contiguous timespans that have already been downloaded.

        """

    def download(self, span: Timespan):
        """Download any files from the remote data provider corresponding to the given timespan.

        Parameters
        ----------
        span : Timespan
            The timespan to download data for.

        Only files that aren't already available locally need to be downloaded. This function is idempotent and will do
        nothing if all data for the given timespan is already downloaded.
        """

    def extract(self, span: Timespan) -> typing.Generator[xarray.Dataset, None, None]:
        """Extract any files from the remote data provider corresponding to the given timespan.

        Data files from the data provider that correspond to the given timespan will be opened using xarray and yielded
        in chronological order. The files themselves may already be available locally from having been downloaded
        previously, or may need to be downloaded in response to this call.

        Parameters
        ----------
        span : Timespan
            The timespan to extract data for.

        Returns
        -------

        typing.Generator[xarray.Dataset, None, None] :
            A generator that yields one Xarray Dataset for each source data file from the data provider.
        """
