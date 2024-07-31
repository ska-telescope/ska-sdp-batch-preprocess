import numpy as np
import xarray as xr
from dask.distributed import Client
from numpy.typing import NDArray
from ska_sdp_func_python.preprocessing.averaging import averaging_time, averaging_frequency
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks

def distribute_time_averaging(vis: xr.Dataset, timestep, chunksize, client: Client, threshold: float) -> xr.Dataset:
    """
    Distributes the input visibilties on time and averages them.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of timesamples to be averaged

    :return: Time averaged Xarray dataset complying to the visibility datamodel
    """

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':chunksize, 'spatial':-1})

    processed = client.submit(averaging_time, chunked_vis, timestep, threshold)

    output = processed.result()

    return output


#TODO: Test scaling of distribution on time instead of frequency
def distribute_freq_averaging(vis: xr.Dataset, freqstep, chunksize, client: Client, threshold: float) -> xr.Dataset:
    """
    Distributes the input visibilties on freq and averages them.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep, integer value of the number of frequency channels to be averaged

    :return: Freq averaged Xarray dataset complying to the visibility datamodel
    """

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':chunksize, 'polarisation':-1, 'time':-1, 'spatial':-1})

    processed = client.submit(averaging_frequency, chunked_vis, freqstep, threshold)

    output = processed.result()

    return output


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
    
    output = processed.result()

    return output