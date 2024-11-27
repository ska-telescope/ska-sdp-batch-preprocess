import numpy as np
import xarray as xr
from dask.distributed import Client
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks

from ska_sdp_batch_preprocess.functions.distributed_func import Distribute
from ska_sdp_batch_preprocess.functions.mapped_func import mapped_rfi_masking


def test_distributed_rfi_mask(
    test_data: xr.Dataset, client: Client, mask_pairs
):
    """
    Test distributed masking against non-distributed masking
    """

    time_distributor = Distribute(test_data, "time", 4, client)
    freq_distributor = Distribute(test_data, "frequency", 4, client)

    assert time_distributor.rfi_masking(mask_pairs).equals(
        apply_rfi_masks(test_data, mask_pairs)
    )
    assert freq_distributor.rfi_masking(mask_pairs).equals(
        apply_rfi_masks(test_data, mask_pairs)
    )


def test_mapped_rfi_mask(test_data: xr.Dataset, mask_pairs):
    """
    Test mapped masking aginst single node masking
    """

    dis_result = mapped_rfi_masking(test_data, mask_pairs)
    result = apply_rfi_masks(test_data, mask_pairs)

    assert dis_result.equals(result)
