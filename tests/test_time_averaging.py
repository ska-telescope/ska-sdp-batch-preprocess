from ska_sdp_func_python.preprocessing import averaging_time
from ska_sdp_batch_preprocess.distributed_func import distribute_time_averaging
from ska_sdp_batch_preprocess.mapped_func import mapped_time_averager
import xarray as xr
from dask.distributed import Client


def test_distributed_t_avg(test_data: xr.Dataset, client: Client):
    """
    Test distributed time averaging against single node averaging
    """
    timestep = 3
    chunksize = 8
    threshold = 0.5

    dis_result  = distribute_time_averaging(test_data,timestep,chunksize,client,threshold)
    result = averaging_time(test_data, timestep, flag_threshold=threshold)
    
    assert dis_result.equals(result)


def test_mapped_t_avg(test_data: xr.Dataset):
    """
    Test mapped time averaging against single node averaging
    """
    timestep = 3
    threshold = 0.5
    dis_result = mapped_time_averager(test_data, timestep,threshold)
    result = averaging_time(test_data, timestep, flag_threshold=threshold)

    assert dis_result.equals(result)

