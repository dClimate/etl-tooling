from dc_etl.ipld.loader import IPLDPublisher
from dc_etl.load import Loader
from .combine import Combiner, CombinePreprocessor, CombinePostprocessor
from .config import _get_component
from .extract import Extractor
from .fetch import Fetcher
from .transform import Transformer
from .assessor import Assessor


def assessor(name: str, *args, **kwargs) -> Assessor:
    """Get and configure a assessor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the assessor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("assessor", name, args, kwargs)


def fetcher(name: str, *args, **kwargs) -> Fetcher:
    """Get and configure a fetcher implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the fetcher implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("fetcher", name, args, kwargs)


def extractor(name: str, *args, **kwargs) -> Extractor:
    """Get and configure an extractor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the extractor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("extractor", name, args, kwargs)


def combiner(name: str, *args, **kwargs) -> Combiner:
    """Get and configure a combiner implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the combiner implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("combiner", name, args, kwargs)


def combine_preprocessor(name: str, *args, **kwargs) -> CombinePreprocessor:
    """Get and configure a preprocessor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the preprocessor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("combine_preprocessor", name, args, kwargs)


def combine_postprocessor(name: str, *args, **kwargs) -> CombinePostprocessor:
    """Get and configure a postprocessor implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the postprocessor implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("combine_postprocessor", name, args, kwargs)


def transformer(name: str, *args, **kwargs) -> Transformer:
    """Get and configure a transformer implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the transformer implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("transformer", name, args, kwargs)


def loader(name: str, *args, **kwargs) -> Loader:
    """Get and configure a loader implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the loader implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("loader", name, args, kwargs)


def ipld_publisher(name: str, *args, **kwargs) -> IPLDPublisher:
    """Get and configure a ipld_publisher implementation by name.

    Parameters
    ----------
    name : str
        The registered name of the ipld_publisher implementation to get and configure.
    **kwargs :
        Any extra keyword arguments are passed to the implementation entry point to get an instance.
    """
    return _get_component("ipld_publisher", name, args, kwargs)
