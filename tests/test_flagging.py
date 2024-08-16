from numpy.typing import NDArray
import xarray as xr
import numpy as np
from ska_sdp_func_python.preprocessing.flagger import rfi_flagger
from ska_sdp_func_python.preprocessing.ao_flagger import ao_flagger
from ska_sdp_batch_preprocess.functions.distributed_func import distribute_rfi_flagger , distribute_ao_flagger
from dask.distributed import Client

def test_distributed_rfi_flagger(test_data:xr.Dataset, client:Client):
    """
    Tests the distributed rfi flagger against the single node rfi flagger
    """
    chunksize = 4
    dist_result = distribute_rfi_flagger(test_data, chunksize,client)
    result = rfi_flagger(test_data)
    assert dist_result == result

#TODO: Implement a quick test for aoflagger with a simple lua strategy?
def test_distributed_aoflagger():
    """
    """