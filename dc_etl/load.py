import abc

import xarray

from dc_etl.fetch import Timespan


class Loader(abc.ABC):
    @abc.abstractmethod
    def initial(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Start writing a new dataset."""

    @abc.abstractmethod
    def append(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Append data to an existing dataset."""

    @abc.abstractmethod
    def replace(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Replace a contiguous span of data in an existing dataset."""
