import xarray as xr
from dask.distributed import Client
from ska_sdp_func_python.preprocessing import averaging_frequency

from ska_sdp_batch_preprocess.functions.distributed_func import Distribute
from ska_sdp_batch_preprocess.functions.mapped_func import (
    mapped_averaging_frequency,
)


def test_distributed_f_avg(test_data: xr.Dataset, client: Client):
    """
    Test distributed frequency averaging against single node averaging
    """
    time_distributor = Distribute(test_data, "time", 4, client)
    freq_distributor = Distribute(test_data, "frequency", 4, client)

    assert time_distributor.avg_freq(10, 0.5).equals(
        averaging_frequency(test_data, 10, flag_threshold=0.5)
    )
    assert freq_distributor.avg_freq(10, 0.5).equals(
        averaging_frequency(test_data, 10, flag_threshold=0.5)
    )


def test_mapped_f_avg(test_data: xr.Dataset):
    """
    Test mapped frequency averaging against single node averaging
    """
    freqstep = 3
    threshold = 0.5

    dis_result = mapped_averaging_frequency(test_data, freqstep, threshold)
    result = averaging_frequency(test_data, freqstep, flag_threshold=threshold)

    assert dis_result.equals(result)
