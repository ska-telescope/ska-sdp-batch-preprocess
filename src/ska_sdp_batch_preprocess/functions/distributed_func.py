# see license in parent directory
import numpy as np
import xarray as xr
from dask.distributed import Client
from numpy.typing import NDArray
from ska_sdp_func_python.preprocessing.averaging import (
    averaging_frequency,
    averaging_time,
)
from ska_sdp_func_python.preprocessing.flagger import rfi_flagger
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks


class Distribute:
    """
    Class to easily chunk and divide on frequency and time axis.

    Defines the chunking of the measurement set dependent on the configuration
    defined by the user. Then distributes the specific processing functions
    called using DASK on the defined parameters.
    """

    def __init__(self, vis: xr.Dataset, axis, chunksize, client: Client):
        """
        Initiates the Distribute class by checking if the user has defined the
        chunking axis to be frequency or time and divides xarray dataset based
        on the axis and the chunksize

        Raises
        ------
        KeyError
        """
        chunked_axis = {
            key: chunksize if key == axis else -1
            for key in [
                "baselines",
                "frequency",
                "polarisation",
                "time",
                "spatial",
            ]
        }

        self.vis = vis.chunk(chunked_axis)
        self.client = client

    def avg_time(self, timestep, threshold: float) -> xr.Dataset:
        """
        Distributes the input visibilties on time and averages them.

        :param: timestep, integer value of num timesamples to be averaged
        :param: threshold,

        :return: Time averaged Xarray dataset complying to vis datamodel
        """

        self.vis = self.client.submit(
            averaging_time, self.vis, timestep, threshold
        ).result()
        return self.vis

    def avg_freq(self, freqstep, threshold: float) -> xr.Dataset:
        """
        Distributes the input visibilties on freq and averages them.

        :param: freqstep, integer value of num frequency channels to average
        :param: threshold,

        :return: Freq averaged Xarray dataset complying to vis datamodel
        """

        self.vis = self.client.submit(
            averaging_frequency, self.vis, freqstep, threshold
        ).result()
        return self.vis

    def rfi_masking(self, masks: NDArray[np.float64]) -> xr.Dataset:
        """
        Distributes input visibilities on freq and applies masks to them

        :param: masks, N*2 numpy array; n pairs of freq ranges to be masked

        :return: Xarray dataset complying to visibility datamodel
        with masked frequencies
        """

        self.vis = self.client.submit(
            apply_rfi_masks, self.vis, masks
        ).result()
        return self.vis

    def flagger(
        self,
        *,
        alpha=0.5,
        threshold_magnitude=3.5,
        threshold_variation=3.5,
        threshold_broadband=3.5,
        sampling=8,
        window=0,
        window_median_history=10
    ) -> xr.Dataset:
        """
        Distributes input visibilities on time and applies FluctuFlagger
        :param vis: xarray dataset complying to the visibility datamodel
        :param alpha: historical memory coefficient
        :param threshold_magnitude: threshold for the magnitude
        :param threshold_variation: threshold for the variations
        :param threshold_broadband: threshold for the broadband RFI
        :param sampling: sampling step
        :param wibdow: window for side channels
        :param window_median_history: window for broadband

        :return: Xarray dataset complying to visibility datamodel with flags
        """

        self.vis = self.client.submit(
            rfi_flagger,
            self.vis,
            alpha,
            threshold_magnitude,
            threshold_variation,
            threshold_broadband,
            sampling,
            window,
            window_median_history,
        ).result()
        return self.vis

    def ao_rfi_flagger(self, path=None):
        """
        Distributes the input visibilities on time and apllies AOFlagger
        :param vis: xarray dataset complying to the visibility datamodel
        :param path: location of the Lua strategy file

        :return: Xarray dataset complying to visibility datamodel with flags
        """

        # TODO: Fix aoflagger dependency issue
        pass
