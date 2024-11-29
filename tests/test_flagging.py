import xarray as xr
from dask.distributed import Client
from ska_sdp_func_python.preprocessing.flagger import rfi_flagger

from ska_sdp_batch_preprocess.functions.distributed_func import Distribute


def test_distributed_rfi_flagger(test_data: xr.Dataset, client: Client):
    """
    Tests the distributed rfi flagger against the non-distributed rfi flagger
    """
    time_distributor = Distribute(test_data, "time", 4, client)
    freq_distributor = Distribute(test_data, "frequency", 4, client)

    assert time_distributor.flagger() == rfi_flagger(test_data)
    assert freq_distributor.flagger() == rfi_flagger(test_data)


# TODO: Implement a quick test for aoflagger with a simple lua strategy?
# Once aoflagger dependency is resolved
def test_distributed_aoflagger():
    """
    Tests for aoflagger
    """
    pass
