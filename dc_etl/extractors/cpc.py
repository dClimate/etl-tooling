import numpy as np
from dc_etl.extract import Extractor
from dc_etl.extract import Timespan
from urllib.request import urlretrieve

def timespan_2024_till_now() -> Timespan:
    return Timespan(start=np.datetime64("2024-01-01"), end=np.datetime64("now"))

class CPCPrecipGlobalDaily(Extractor):
    def get_remote_timespan(self) -> Timespan:
        return timespan_2024_till_now()

    def download(self, span: Timespan):
        url = "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_precip/precip.2024.nc"
        filename = "precip.global.2024.nc"
        urlretrieve(url, filename)

class CPCPrecipCONUSDaily(Extractor):
    def get_remote_timespan(self) -> Timespan:
        return timespan_2024_till_now()

    def download(self, span: Timespan):
        url = "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_us_precip/RT/precip.V1.0.2024.nc"
        filename = "precip.conus.2024.nc"
        urlretrieve(url, filename)

class CPCTempMaxDaily(Extractor):
    def get_remote_timespan(self) -> Timespan:
        return timespan_2024_till_now()

    def download(self, span: Timespan):
        url = "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmax.2024.nc"
        filename = "tmax.2024.nc"
        urlretrieve(url, filename)

class CPCTempMinDaily(Extractor):
    def get_remote_timespan(self) -> Timespan:
        return timespan_2024_till_now()

    def download(self, span: Timespan):
        url = "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmin.2024.nc"
        filename = "tmin.2024.nc"
        urlretrieve(url, filename)
