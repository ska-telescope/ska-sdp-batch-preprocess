import numpy as np
import xarray as xr
import dask.array as da
import functions
from dask.distributed import Client, LocalCluster
from numpy.typing import NDArray

#Process 100 time samples for each node
chunk_size = (100, -1 ,-1, -1)

def distribute_dummy_np(vis: NDArray[np.float64]) -> NDArray[np.float64]:
    """
    Distributes the input visibilties on time and processes them.
    Currently only works on local clusters.

    Parameters:
    Numpy Array with 4-dimensional visibilities in double
    
    Returns:
    Processed in a distributed setting Numpy Array with 4-dimensional visibilities in double
    """

    cluster = LocalCluster()
    client = Client(cluster)

    vis_dask = da.from_array(vis, chunks=chunk_size)

    processed = vis_dask.map_blocks(functions.dummy_function_np)

    output = processed.compute()

    client.close()
    cluster.close()

    return output

def distribute_xr_time_averaging(vis: xr.Dataset, timestep) -> xr.Dataset:
    """
    Distributes the input visibilties on time and processes them.
    Currently only works on local clusters.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of timesamples to be averaged

    Returns:
    Visibility datamodel as an Xarray Dataset
    """

    cluster = LocalCluster()
    client = Client(cluster)

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':100, 'uvw_index':-1})

    processed = chunked_vis.map_blocks(functions.wrap_time_averager, kwargs={'timestep': timestep})

    output = processed.compute()

    client.close()
    cluster.close()

    return output

#TODO: Test scaling of distribution on frequency instead of time
def distribute_xr_freq_averaging(vis: xr.Dataset, freqstep) -> xr.Dataset:
    """
    Distributes the input visibilties on time and processes them.
    Currently only works on local clusters.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep, integer value of the number of frequency channels to be averaged

    Returns:
    Visibility datamodel as an Xarray Dataset
    """

    cluster = LocalCluster()
    client = Client(cluster)

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':100, 'uvw_index':-1})

    processed = chunked_vis.map_blocks(functions.wrap_freq_averager, kwargs={'freqstep': freqstep})

    output = processed.compute()

    client.close()
    cluster.close()

    return output


