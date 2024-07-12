import numcodecs
import xarray

from .transform import Transformer


class Composite:
    """A composition of transformers.

    The transformers used in the composition will be called in the order passed in with the output of one feeding the
    input of the next, in a pipeline fashion.

    Parameters
    ----------
    *transformers : Transformer
        The transformers to use for the composition.

    Returns
    -------
    Transformer :
        A single transformer composed of the transformers passed in as a pipeline.
    """

    @classmethod
    def _from_config(cls, config):
        return cls(*[transformer.as_component("transformer") for transformer in config["transformers"]])

    def __init__(self, *transformers: Transformer):
        self.transformers = transformers

    def __call__(self, dataset: xarray.Dataset) -> xarray.Dataset:
        for transform in self.transformers:
            dataset = transform(dataset)

        return dataset


def rename_dims(names: dict[str, str]) -> Transformer:
    """Transformer to rename dimensions in output dataset.

    Parameters
    ----------
    names : dict[str, str]
        Mapping of old name to new name.

    Returns
    -------
    Transformer :
        A Transformer instance that will do the renaming.
    """

    def rename_dims(dataset: xarray.Dataset) -> xarray.Dataset:
        return dataset.rename(names)

    return rename_dims


def normalize_longitudes() -> Transformer:
    """Transformer to convert longitude coordinates from 0 - 360 to -180 to 180.

    Returns
    -------
    Transformer :
        The transformer.
    """

    def normalize_longitudes(dataset: xarray.Dataset) -> xarray.Dataset:
        dataset = dataset.assign_coords(longitude=(((dataset.longitude + 180) % 360) - 180))

        # After converting, the longitudes may still start at zero. This reorders the longitude coordinates from -180
        # to 180 if necessary.
        return dataset.sortby(["latitude", "longitude"])

    return normalize_longitudes


def compress(variables: list[str]) -> Transformer:
    """Transformer to add Blosc compression to specific variables, usually the data variable.

    Parameters
    ----------
    *variables: list[str]
        Names of the variables in the dataset to apply compression to.

    Returns
    -------
    Transformer :
        The transformer.
    """

    def compress(dataset: xarray.Dataset) -> xarray.Dataset:
        for var in variables:
            dataset[var].encoding["compressor"] = numcodecs.Blosc()

        return dataset

    return compress
