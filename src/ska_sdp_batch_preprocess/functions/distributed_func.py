# see license in parent directory

import numpy as np
import xarray as xr
from dask.distributed import Client
from numpy.typing import NDArray
from typing import Any

from ska_sdp_func_python.preprocessing.averaging import (
    averaging_frequency,
    averaging_time
)
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks
from ska_sdp_func_python.preprocessing.flagger import rfi_flagger


class Distribute:
    """
    Class to easily chunk and divide on frequency and time axis. 
    
    Defines the chunking of the measurement set dependent on the configuration defined by the user.
    Then distributes the specific processing functions called using DASK on the defined parameters.
    """
    
    def __init__(self, vis: xr.Dataset, axis, chunksize, client: Client):
        """
        Initiates the Distribute class by checking if the user has defined the 
        chunking axis to be frequency or time and divides the xarray dataset based 
        on the axis and the chunksize

        Raises
        ------
        KeyError
        """
        chunked_axis = {
            key: chunksize if key == axis else -1
            for key in [
                "baseline", "frequency", "polarisation", "time", "spatial"
            ]
        }

        self._vis = vis.chunk(chunked_axis)
        self.client = client
    
    def __setattr__(self, key: str, value: Any) -> None:
        """
        The setter method of this class is amended here to inhibit external
        manipulation of private attributes (i.e., those starting with '_').
        """
        if hasattr(self, key) and key[0] == '_':
            return
        self.__dict__[f"{key}"] = value       

    def avg_time(self, timestep, threshold: float) -> xr.Dataset:
        """
        Distributes the input visibilties on time and averages them.
        
        :param: timestep, integer value of the number of timesamples to be averaged
        :param: threshold,

        :return: Time averaged Xarray dataset complying to the visibility datamodel
        """
        return self.client.submit(averaging_time, self._vis, timestep, threshold).result()


    def avg_freq(self, freqstep, threshold: float) -> xr.Dataset:
        """
        Distributes the input visibilties on freq and averages them.
        
        :param: freqstep, integer value of the number of frequency channels to be averaged
        :param: threshold, 

        :return: Freq averaged Xarray dataset complying to the visibility datamodel
        """

        return self.client.submit(averaging_frequency, self._vis, freqstep, threshold).result()

    def rfi_masking(self, masks: NDArray[np.float64]) -> xr.Dataset:
        """
        Distributes the input visibilities on freq and applies masks to them

        :param: masks, N*2 numpy array, with n pairs of frequency ranges to be masked

        :return: Xarray dataset complying to the visibility datamodel with masked frequencies
        """

        return self.client.submit(apply_rfi_masks, self._vis, masks).result()

    def flagger(self,
                *,
                alpha = 0.5,
                threshold_magnitude = 3.5,
                threshold_variation = 3.5,
                threshold_broadband = 3.5,
                sampling = 8,
                window = 0,
                window_median_history = 10) -> xr.Dataset:
        """
        Distributes the input visibilities on time and apllies FluctuFlagger RFI flagger
        :param vis: xarray dataset complying to the visibility datamodel
        :param alpha: historical memory coefficient
        :param threshold_magnitude: threshold for the magnitude
        :param threshold_variation: threshold for the variations
        :param threshold_broadband: threshold for the broadband RFI
        :param sampling: sampling step
        :param wibdow: window for side channels
        :param window_median_history: window for broadband

        :return: Xarray dataset complying to the visibility datamodel with flags
        """

        return self.client.submit(rfi_flagger, self._vis, alpha, threshold_magnitude, threshold_variation, threshold_broadband, sampling, window, window_median_history).result()

    def ao_rfi_flagger(self, path=None):
        """
        Distributes the input visibilities on time and apllies AOFlagger RFI flagger
        :param vis: xarray dataset complying to the visibility datamodel
        :param path: location of the Lua strategy file
        
        :return: Xarray dataset complying to the visibility datamodel with flags 
        """

        #TODO: Fix aoflagger dependency issue
        pass

