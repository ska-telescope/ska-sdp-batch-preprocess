from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks
from ska_sdp_batch_preprocess.functions.distributed_func import distribute_rfi_masking
from ska_sdp_batch_preprocess.functions.mapped_func import mapped_rfi_masking
import xarray as xr
from dask.distributed import Client
import numpy as np

def test_distributed_rfi_mask(test_data: xr.Dataset, client: Client, mask_pairs):
    """
    Test distributed masking against single node masking
    """

    chunksize = 4
    dis_result = distribute_rfi_masking(test_data, mask_pairs, chunksize,client)
    result = apply_rfi_masks(test_data, mask_pairs)

    assert dis_result.equals(result)    

def test_mapped_rfi_mask(test_data: xr.Dataset, mask_pairs):
    """
    Test mapped masking aginst single node masking
    """

    dis_result = mapped_rfi_masking(test_data, mask_pairs)
    result = apply_rfi_masks(test_data, mask_pairs)

    assert dis_result.equals(result)