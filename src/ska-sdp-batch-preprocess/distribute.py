import numpy as np
import dask.array as da
import functions
from dask.distributed import Client, LocalCluster
from numpy.typing import NDArray

#Process 100 time samples for each node
chunk_size = (100, -1 ,-1, -1)

def Distribute(vis: NDArray[np.float64]) -> NDArray[np.float64]:
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
