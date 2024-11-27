import xarray as xr
from dask.distributed import Client
from ska_sdp_func_python.preprocessing import averaging_time

from ska_sdp_batch_preprocess.functions.distributed_func import Distribute
from ska_sdp_batch_preprocess.functions.mapped_func import (
    mapped_averaging_time,
)


def test_distributed_t_avg(test_data: xr.Dataset, client: Client):
    """
    Test distributed time averaging against single node averaging
    """
    time_distributor = Distribute(test_data, "time", 4, client)
    freq_distributor = Distribute(test_data, "frequency", 4, client)

    assert time_distributor.avg_time(5, 0.5).equals(
        averaging_time(test_data, 5, flag_threshold=0.5)
    )
    assert freq_distributor.avg_time(5, 0.5).equals(
        averaging_time(test_data, 5, flag_threshold=0.5)
    )


def test_mapped_t_avg(test_data: xr.Dataset):
    """
    Test mapped time averaging against single node averaging
    """
    timestep = 3
    threshold = 0.5
    dis_result = mapped_averaging_time(test_data, timestep, threshold)
    result = averaging_time(test_data, timestep, flag_threshold=threshold)

    assert dis_result.equals(result)
