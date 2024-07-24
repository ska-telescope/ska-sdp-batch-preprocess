#import ska-sdp-python_proc
from numpy.typing import NDArray
import xarray as xr
import numpy as np

def dummy_function_np(vis : NDArray[np.float64]) -> NDArray[np.float64]:
    """
    Dummy function that does point wise multiplication on numpy arrays (visibilities)

    Parameters:
    Numpy Array with 4-dimensional visibilities in double
    
    Returns:
    Point wise multiplied numpy array
    """
    matrix = np.random.rand(100, 10, 10, 10)
    vis = matrix * vis
    return vis

def dummy_function_xr(vis: xr.Dataset) -> NDArray[np.float64]:
    """
    Dummy function that does point wise multiplication on Xarray datasets (visibilities)

    Parameters:
    Xarray dataset with 4-dimensional visibilities in double

    Returns:
    Point wise multiplied numpy array
    """
    matrix = np.random.rand(100, 10, 10, 10)
    vis_in  = vis["VISIBILITY"].values
    vis_out = vis_in * matrix
    return vis_out