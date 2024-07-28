import numpy as np
import xarray as xr
import dask.array as da
import functions
from dask.distributed import Client, LocalCluster
from numpy.typing import NDArray


def distribute_xr_time_averaging(vis: xr.Dataset, timestep) -> xr.Dataset:
    """
    Distributes the input visibilties on time and averages them.
    Currently only works on local clusters.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of the number of timesamples to be averaged

    :return: Time averaged Xarray dataset complying to the visibility datamodel
    """

    cluster = LocalCluster()
    client = Client(cluster)

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':-1, 'polarisation':-1, 'time':timestep, 'uvw_index':-1})

    processed = chunked_vis.map_blocks(functions.wrap_time_averager, kwargs={'timestep': timestep})

    output = processed.compute()

    client.close()
    cluster.close()

    return output

#TODO: Test scaling of distribution on time instead of frequency
def distribute_xr_freq_averaging(vis: xr.Dataset, freqstep) -> xr.Dataset:
    """
    Distributes the input visibilties on freq and averages them.
    Currently only works on local clusters.
    
    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep, integer value of the number of frequency channels to be averaged

    :return: Freq averaged Xarray dataset complying to the visibility datamodel
    """

    cluster = LocalCluster()
    client = Client(cluster)

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':freqstep, 'polarisation':-1, 'time':-1, 'uvw_index':-1})

    processed = chunked_vis.map_blocks(functions.wrap_freq_averager, kwargs={'freqstep': freqstep})

    output = processed.compute()

    client.close()
    cluster.close()

    return output

def distribute_xr_masking(vis: xr.Dataset, masks: NDArray[np.float64], freqstep) -> xr.Dataset:
    """
    Distributes the input visibilities on freq and applies masks to them
    Currently only works on local clusters.

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: masks, N*2 numpy array, with n pairs of frequency ranges to be masked
    :param: freqstep, integer value of the number of frequency channels to be distributed

    :return: Xarray dataset complying to the visibility datamodel with masked frequencies
    """
    
    cluster = LocalCluster()
    client = Client(cluster)

    chunked_vis = vis.chunk({'baselines':-1, 'frequency':freqstep, 'polarisation':-1, 'time':-1, 'uvw_index':-1})

    processed = chunked_vis.map_blocks(functions.wrap_mask, kwargs={'masks': masks})
    output = processed.compute()

    client.close()
    cluster.close()

    return output