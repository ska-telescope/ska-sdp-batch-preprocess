#import ska-sdp-python_proc
from numpy.typing import NDArray
import xarray as xr
import numpy as np
from ska_sdp_func_python.preprocessing.averaging import averaging_time, averaging_frequency
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks

def mapped_time_averager(vis : xr.Dataset, timestep, threshold) -> xr.Dataset:
    """
    Maps the real time processing time averager accross chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of time samples to average

    :return: Time averaged Xarray dataset complying to the visibility datamodel
    """
    return vis.map_blocks(averaging_time, args=(timestep,), kwargs={'flag_threshold':threshold})

def mapped_freq_averager(vis: xr.Dataset, freqstep, threshold) -> xr.Dataset:
    """
    Maps the real time processing frequency averager to accross chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep,integer value of the number of frequency channels to average

    :return: Freq averaged Xarray dataset complying to the visibility datamodel
    """

    return vis.map_blocks(averaging_frequency, args=(freqstep,), kwargs={'flag_threshold':threshold})


def mapped_rfi_mask(vis: xr.Dataset, masks: NDArray[np.float64]) -> xr.Dataset:
    """
    Wraps the real time processing rfi masking to work with chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: masks, N*2 numpy array, with n pairs of frequency ranges to be masked
    
    :return: Xarray dataset complying to the visibility datamodel with masked frequencies
    """

    return vis.map_blocks(apply_rfi_masks, args=(masks,))