import xarray

from .transform import Transformer


def composite(*transformers: Transformer) -> Transformer:
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

    def transform(dataset: xarray.Dataset) -> xarray.Dataset:
        for transform in transformers:
            dataset = transform(dataset)

        return dataset

    return transform


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

    def transform(dataset: xarray.Dataset) -> xarray.Dataset:
        return dataset.rename(names)

    return transform


def normalize_longitudes() -> Transformer:
    """Transformer to convert longitude coordinates from 0 - 360 to -180 to 180.

    Returns
    -------
    Transformer :
        The transformer.
    """

    def transform(dataset: xarray.Dataset) -> xarray.Dataset:
        dataset = dataset.assign_coords(longitude=(((dataset.longitude + 180) % 360) - 180))

        # After converting, the longitudes may still start at zero. This reorders the longitude coordinates from -180
        # to 180 if necessary.
        return dataset.sortby(["latitude", "longitude"])

    return transform
