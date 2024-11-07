from __future__ import annotations

import functools
import re

from typing import Generator

import fsspec
import xarray

from dc_etl.errors import MissingConfigurationError
from dc_etl.fetch import Fetcher, Timespan
from dc_etl.filespec import FileSpec

_GLOB = {
    "global_precip": ["/Datasets/cpc_global_precip/precip.*.nc"],
    "global_temp_max": ["/Datasets/cpc_global_temp/tmax.*.nc"],
    "global_temp_min": ["/Datasets/cpc_global_temp/tmin.*.nc"],
    # Oh come on, really?
    "us_precip": ["/Datasets/cpc_us_precip/precip.V1.0.*.nc", "/Datasets/cpc_us_precip/RT/precip.V1.0.*.nc"],
}

_DATA_FILE = re.compile(r".+\.\d\d\d\d.nc")


class CPCFetcher(Fetcher):
    """Fetches source data files for CPC datasets.

    Parameters
    ----------
    dataset: str
        Which CPC dataset to fetch, eg "precip_global", "precip_us", etc...
    cache: FileSpec | None
        Optionally, a writable folder where downloaded files can be cached.
    """

    def __init__(self, dataset: str, cache: FileSpec | None = None):
        glob = _GLOB.get(dataset)
        if glob is None:
            raise MissingConfigurationError(f"Unrecognized dataset: {dataset}, valid values are {', '.join(_GLOB)}")

        self._glob = glob
        self._cache = cache

    @property
    @functools.cache
    def _fs(self):
        """Get the FTP filesystem lazily.

        Instantiating the fsspec filesystem creates a network connection, so it's better to this lazily."""
        return fsspec.filesystem("ftp", host="ftp.cdc.noaa.gov")

    @functools.cache
    def _get_remote_files(self):
        seen_years = set()
        files = []
        for glob in self._glob:
            for file in self._fs.glob(glob):
                if not _DATA_FILE.match(file):
                    continue

                year = _year(file)
                if year in seen_years:
                    continue

                files.append(file)
                seen_years.add(year)

        return sorted(files, key=_year)

    @functools.cache
    def get_remote_timespan(self, **kwargs) -> Timespan:
        """Implementation of :meth:`Fetcher.get_remote_timespan`"""
        files = self._get_remote_files()
        first = xarray.open_dataset(self._get_file_by_path(files[0]).open())
        last = xarray.open_dataset(self._get_file_by_path(files[-1]).open())
        return Timespan(first.time[0].values, last.time[-1].values)

    def prefetch(self, span: Timespan, **kwargs):
        """Implementation of :meth:`Fetcher.pre_fetch`"""
        if self._cache:
            for _ in self.fetch(span):
                pass

    def fetch(self, span: Timespan, **kwargs) -> Generator[FileSpec, None, None]:
        """Implementation of :meth:`Fetcher.fetch`"""
        start = span.start.astype("<M8[ms]").astype(object).year
        end = span.end.astype("<M8[ms]").astype(object).year

        for year in range(start, end + 1):
            yield self._get_file_by_year(year)

    def _get_file_by_path(self, path):
        """Get a FileSpec for the path, using the cache if configured."""
        # Not using cache
        if not self._cache:
            return FileSpec(self._fs, path)

        # Check cache
        cache_path = self._cache_path(path)
        if not cache_path.exists():
            # Download it to the cache
            self._fs.get_file(path, cache_path.open("wb"))

        # Return the cached file
        return cache_path

    def _get_file_by_year(self, year):
        """Get a FileSpec for the year, using the cache if configured."""
        # Not using cache
        if not self._cache:
            return FileSpec(self._fs, self._year_to_path(year))

        # Check cache
        if self._cache.exists():
            for path in self._cache.fs.ls(self._cache.path):
                if _DATA_FILE.match(path) and _year(path) == year:
                    return self._cache_path(path)

        # Download it to the cache
        path = self._year_to_path(year)
        cache_path = self._cache_path(path)
        self._fs.get_file(path, cache_path.open("wb"))

        return cache_path

    def _cache_path(self, path):
        """Compute a file's path in the cache."""
        filename = path.split("/")[-1]
        return self._cache / filename

    def _year_to_path(self, year):
        for path in self._get_remote_files():
            if _year(path) == year:
                return path

        raise KeyError(year)


def _year(path: str) -> int:
    """Given a file path for a CPC data file, return the year from the filename."""
    return int(path[-7:-3])  # "...YYYY.nc"
