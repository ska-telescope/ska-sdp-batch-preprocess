#import ska-sdp-python_proc
from numpy.typing import NDArray
import xarray as xr
import numpy as np
from ska_sdp_func_python.preprocessing.averaging import averaging_time, averaging_frequency
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks
from ska_sdp_func_python.preprocessing.flagger import rfi_flagger
from ska_sdp_func_python.preprocessing.ao_flagger import ao_flagger

def mapped_averaging_time(vis : xr.Dataset, timestep, threshold) -> xr.Dataset:
    """
    Maps the real time processing time averager accross chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of time samples to average

    :return: Time averaged Xarray dataset complying to the visibility datamodel
    """
    return vis.map_blocks(averaging_time, args=(timestep,), kwargs={'flag_threshold':threshold})

def mapped_averaging_frequency(vis: xr.Dataset, freqstep, threshold) -> xr.Dataset:
    """
    Maps the real time processing frequency averager to accross chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep,integer value of the number of frequency channels to average

    :return: Freq averaged Xarray dataset complying to the visibility datamodel
    """

    return vis.map_blocks(averaging_frequency, args=(freqstep,), kwargs={'flag_threshold':threshold})


def mapped_rfi_masking(vis: xr.Dataset, masks: NDArray[np.float64]) -> xr.Dataset:
    """
    Wraps the real time processing rfi masking to work with chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: masks, N*2 numpy array, with n pairs of frequency ranges to be masked
    
    :return: Xarray dataset complying to the visibility datamodel with masked frequencies
    """

    return vis.map_blocks(apply_rfi_masks, args=(masks,))


def mapped_rfi_flagger(vis: xr.Dataset, alpha, threshold_magnitudes, threshold_variations, threshold_broadband, sampling, window, window_median_history) -> xr.Dataset:
    """
    Maps RFI flagger (FluctuFlagger) across chunked visibilities

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

    return vis.map_blocks(rfi_flagger, kwargs={'alpha': alpha,
                                               'threshold_magnitudes': threshold_magnitudes,
                                               'threshold_variations': threshold_variations, 
                                               'threshold_broadband': threshold_broadband,
                                               'sampling': sampling,
                                               'window': window,
                                               'window_median_history': window_median_history })


def mapped_ao_flagger(vis: xr.Dataset, path) -> xr.Dataset:
    """
    Maps RFI flagger (AOFlagger) across chunked visibilities

    :param vis: xarray dataset complying to the visibility datamodel
    :param path: location of the Lua strategy for AOFlagger

    :return: Xarray dataset complying to the visibility datamodel with flags
    """

    return vis.map_blocks(ao_flagger, kwargs={'path':path})




