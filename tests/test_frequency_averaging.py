import xarray as xr
from dask.distributed import Client
from ska_sdp_batch_preprocess.functions.distributed_func import distribute_averaging_freq
from ska_sdp_batch_preprocess.functions.mapped_func import  mapped_averaging_frequency
from ska_sdp_func_python.preprocessing import averaging_frequency

def test_distributed_f_avg(test_data: xr.Dataset, client: Client):
    """
    Test distributed frequency averaging against single node averaging
    """

    freqstep = 3
    chunksize = 6
    threshold = 0.5

    dis_result = distribute_averaging_freq(test_data, freqstep, chunksize, client, threshold)
    result = averaging_frequency(test_data, freqstep, flag_threshold=threshold)

    assert dis_result.equals(result)


def test_mapped_f_avg(test_data: xr.Dataset):
    """
    Test mapped frequency averaging against single node averaging 
    """
    freqstep = 3
    threshold = 0.5

    dis_result =  mapped_averaging_frequency(test_data, freqstep, threshold)
    result = averaging_frequency(test_data, freqstep, flag_threshold=threshold)

    assert dis_result.equals(result)


