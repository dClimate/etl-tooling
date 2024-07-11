import orjson

from kerchunk import hdf

from dc_etl.extract import Extractor
from dc_etl.filespec import FileSpec


class NetCDFExtractor(Extractor):
    """Extractor for NetCDF files.

    Parameters
    ----------
    output_folder : FileSpec | None
        Folder to write output to. If not specified, the same folder as the source is used. Either way, the name of the
        output file is the same as the input file with the file extension changed to '.json'.

    inline_threshold : typing.Optional[int]
        Include chunks smaller than this number of bytes directly in the output. Set to zero or negative to disable
        inline data. Default is 5000.
    """

    def __init__(self, output_folder: FileSpec = None, inline_threshold: int = 5000):
        self.output_folder = output_folder
        self.inline_threshold = inline_threshold

    def __call__(self, source: FileSpec) -> FileSpec:
        """Implementation of :meth:`Extractor.extract`"""
        if self.output_folder:
            dest = (self.output_folder / source.name).with_suffix("json")
        else:
            dest = source.with_suffix("json")

        with source.open() as f_in:
            zarr_json = hdf.SingleHdf5ToZarr(f_in, source.path, inline_threshold=self.inline_threshold)
            with dest.open("wb") as f_out:
                f_out.write(orjson.dumps(zarr_json.translate()))

        return dest
