import numpy as np
import xarray as xr
from dask.distributed import Client
from numpy.typing import NDArray
from ska_sdp_func_python.preprocessing.averaging import averaging_time, averaging_frequency
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks
from ska_sdp_func_python.preprocessing.flagger import rfi_flagger

def distribute_averaging_time(vis: xr.Dataset, timestep, chunksize, client: Client, threshold: float) -> xr.Dataset:
    """
    Distributes the input visibilties on time and averages them.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of timesamples to be averaged

    :return: Time averaged Xarray dataset complying to the visibility datamodel
    """

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':chunksize, 'spatial':-1})

    processed = client.submit(averaging_time, chunked_vis, timestep, threshold)

    return processed.result()


#TODO: Test scaling of distribution on time instead of frequency
def distribute_averaging_freq(vis: xr.Dataset, freqstep, chunksize, client: Client, threshold: float) -> xr.Dataset:
    """
    Distributes the input visibilties on freq and averages them.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep, integer value of the number of frequency channels to be averaged

    :return: Freq averaged Xarray dataset complying to the visibility datamodel
    """

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':chunksize, 'polarisation':-1, 'time':-1, 'spatial':-1})

    processed = client.submit(averaging_frequency, chunked_vis, freqstep, threshold)

    return processed.result()


def distribute_rfi_masking(vis: xr.Dataset, masks: NDArray[np.float64], chunksize, client:Client) -> xr.Dataset:
    """
    Distributes the input visibilities on freq and applies masks to them

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: masks, N*2 numpy array, with n pairs of frequency ranges to be masked
    :param: freqstep, integer value of the number of frequency channels to be distributed

    :return: Xarray dataset complying to the visibility datamodel with masked frequencies
    """

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':chunksize, 'polarisation':-1, 'time':-1, 'spatial':-1})

    processed = client.submit(apply_rfi_masks, chunked_vis, masks)        
    
    return processed.result()


def distribute_rfi_flagger(vis: xr.Dataset,
                           chunksize,
                           client: Client,
                           alpha=0.5,
                           threshold_magnitude=3.5,
                           threshold_variation=3.5,
                           threshold_broadband=3.5,
                           sampling = 8,
                           window=0,
                           window_median_history=10) -> xr.Dataset:
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

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':chunksize, 'spatial':-1})

    processed = client.submit(rfi_flagger, chunked_vis, alpha, threshold_magnitude, threshold_variation, threshold_broadband, sampling, window, window_median_history)

    return processed.result()


def distribute_ao_flagger(vis:xr.Dataset, chunksize, client:Client, path=None):
    """
    Distributes the input visibilities on time and apllies AOFlagger RFI flagger
    :param vis: xarray dataset complying to the visibility datamodel
    :param path: location of the Lua strategy file
    
    :return: Xarray dataset complying to the visibility datamodel with flags 
    """

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':chunksize, 'spatial':-1})

    processed = client.submit(ao_flagger, chunked_vis, path)

    return processed.result()


