# see license in parent directory

import numpy as np
import xarray as xr
from numpy.typing import NDArray
from ska_sdp_func_python.preprocessing.averaging import (
    averaging_frequency,
    averaging_time,
)
from ska_sdp_func_python.preprocessing.rfi_masks import apply_rfi_masks


def mapped_averaging_time(vis: xr.Dataset, timestep, threshold) -> xr.Dataset:
    """
    Maps real time processing time averager across chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: timestep, integer value of num time samples to average

    :return: Time averaged Xarray dataset complying to visibility datamodel
    """
    return vis.map_blocks(
        averaging_time, args=(timestep,), kwargs={"flag_threshold": threshold}
    )


def mapped_averaging_frequency(
    vis: xr.Dataset, freqstep, threshold
) -> xr.Dataset:
    """
    Maps real time processing frequency averager across chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: freqstep,integer value of num frequency channels to average

    :return: Freq averaged Xarray dataset complying to visibility datamodel
    """

    return vis.map_blocks(
        averaging_frequency,
        args=(freqstep,),
        kwargs={"flag_threshold": threshold},
    )


def mapped_rfi_masking(
    vis: xr.Dataset, masks: NDArray[np.float64]
) -> xr.Dataset:
    """
    Wraps real time processing rfi masking to work with chunked visibilties

    :param: vis, xarray dataset complying to the visibility datamodel
    :param: masks, N*2 numpy array; n pairs of frequency ranges to be masked

    :return: Xarray dataset complying to vis datamodel with masked frequencies
    """

    return vis.map_blocks(apply_rfi_masks, args=(masks,))
