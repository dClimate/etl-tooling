import abc

import xarray


class Transformer(abc.ABC):
    """Performs a transformation on a dataset."""

    @abc.abstractmethod
    def __call__(self, dataset: xarray.Dataset) -> xarray.Dataset:
        """Performs a transformation on a dataset.

        Parameters
        ----------
        dataset : xarray.Dataset
            The dataset to transform. There is no guarantee that the input dataset won't be mutated.

        Returns
        -------
        xarray.Dataset
            The transformed dataset. Could be the same dataset as the input if the input was mutated in place.
        """


def identity(x):
    """The identity function.

    https://en.wikipedia.org/wiki/Identity_function
    """
    return x
