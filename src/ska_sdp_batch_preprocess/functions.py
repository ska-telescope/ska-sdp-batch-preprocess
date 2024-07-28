#import ska-sdp-python_proc
from numpy.typing import NDArray
import xarray as xr
import numpy as np
from ska_sdp_func_python.preprocessing import averaging,rfi_masks

def dummy_function_np(vis : NDArray[np.float64]) -> NDArray[np.float64]:
    """
    Dummy function that does point wise multiplication on numpy arrays (visibilities)

    :param: Numpy Array with 4-dimensional visibilities in double
    
    :return: Point wise multiplied numpy array
    """
    matrix = np.random.rand(100, 10, 10, 10)
    vis = matrix * vis
    return vis

def wrap_time_averager(vis : xr.Dataset, timestep) -> xr.Dataset:
    """
    Wraps the real time processing time averager to work with chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of time samples to average

    :return: Time averaged Xarray dataset complying to the visibility datamodel
    """
    averaging.averaging_time(vis, timestep, 0.5)

def wrap_freq_averager(vis: xr.Dataset, freqstep) -> xr.Dataset:
    """
    Wraps the real time processing frequency averager to work with chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep,integer value of the number of frequency channels to average

    :return: Freq averaged Xarray dataset complying to the visibility datamodel
    """

    averaging.averaging_frequency(vis, freqstep, 0.5)

def wrap_mask(vis: xr.Dataset, masks: NDArray[np.float64]) -> xr.Dataset:
    """
    Wraps the real time processing rfi masking to work with chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: masks, N*2 numpy array, with n pairs of frequency ranges to be masked
    
    :return: Xarray dataset complying to the visibility datamodel with masked frequencies
    """

    rfi_masks.apply_rfi_masks(vis, masks)