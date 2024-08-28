from __future__ import annotations

import abc

import ipldstore
import xarray

from multiformats import CID

from dc_etl.fetch import Timespan
from dc_etl.load import Loader


class IPLDLoader(Loader):
    """Use IPLD to store datasets."""

    @classmethod
    def _from_config(cls, config):
        config["publisher"] = config["publisher"].as_component("ipld_publisher")
        return cls(**config)

    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        self.time_dim = time_dim
        self.publisher = publisher

    def initial(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Start writing a new dataset."""
        mapper = self._mapper()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        dataset.to_zarr(store=mapper, consolidated=True)
        cid = mapper.freeze()
        self.publisher.publish(cid)

    def append(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Append data to an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        dataset.to_zarr(store=mapper, consolidated=True, append_dim=self.time_dim)
        cid = mapper.freeze()
        self.publisher.publish(cid)

    def replace(self, replace_dataset: xarray.Dataset, span: Timespan | None = None):
        """Replace a contiguous span of data in an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
        region = (
            self._time_to_integer(original_dataset, span.start),
            self._time_to_integer(original_dataset, span.end) + 1,
        )

        replace_dataset = replace_dataset.sel(**{self.time_dim: slice(*span)})
        replace_dataset = replace_dataset.drop_vars(
            [dim for dim in replace_dataset.dims if dim != self.time_dim]
        )
        replace_dataset.to_zarr(
            store=mapper, consolidated=True, region={self.time_dim: slice(*region)}
        )

        cid = mapper.freeze()
        self.publisher.publish(cid)

    def dataset(self) -> xarray.Dataset:
        """Convenience method to get the currently published dataset."""
        mapper = self._mapper(root=self.publisher.retrieve())
        return xarray.open_zarr(store=mapper, consolidated=True)

    def _mapper(self, root=None):
        mapper = ipldstore.get_ipfs_mapper()
        if root is not None:
            mapper.set_root(root)
        return mapper

    def _time_to_integer(self, dataset, timestamp):
        # It seems like an oversight in xarray that this is the best way to do this.
        nearest = dataset.sel(**{self.time_dim: timestamp, "method": "nearest"})[
            self.time_dim
        ]
        return list(dataset[self.time_dim].values).index(nearest)


class IPLDPublisher(abc.ABC):
    """Manage the publishing and retrieval of Datasets in IPLD.

    Puts a CID in a known place.
    """

    def publish(self, cid: CID):
        """Put the CID of a dataset in a place where it can be recalled later."""

    def retrieve(self) -> CID:
        """Retreive the CID of the published dataset."""
