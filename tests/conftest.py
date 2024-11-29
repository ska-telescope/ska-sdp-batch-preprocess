import numpy as np
import pytest
import xarray as xr
from dask.distributed import Client, LocalCluster
from ska_sdp_datamodels import configuration
from ska_sdp_datamodels.science_data_model import PolarisationFrame
from ska_sdp_datamodels.visibility.vis_model import Visibility


@pytest.fixture
def test_data() -> xr.Dataset:
    """
    Generate visibility data for testing
    """
    num_baselines = 8
    num_pols = 1
    low = configuration.create_named_configuration("LOW")
    time = np.array(
        [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]
    )
    num_times = time.shape[0]
    frequency = np.array(
        [
            1e8,
            1.1e8,
            1.2e8,
            1.3e7,
            1.4e7,
            1.5e7,
            1.6e7,
            1.7e7,
            1.8e7,
            1.9e7,
            2e7,
            2.1e7,
        ]
    )
    num_channels = frequency.shape[0]
    channel_bandwidth = np.array(
        [1e7, 1e7, 1e7, 1e7, 1e7, 1e7, 1e7, 1e7, 1e7, 1e7, 1e7, 1e7]
    )
    uvw = np.zeros((num_times, num_baselines, 3))
    vis = (1 + 2j) * np.ones(
        shape=(num_times, num_baselines, num_channels, num_pols)
    ).astype(complex)
    weights = np.ones_like(vis)
    integration_time = np.full(num_times, 10)
    flags = np.zeros(
        shape=(num_times, num_baselines, num_channels, num_pols)
    ).astype(bool)
    flags[2:4, :, 4:7, :] = True
    polarisation_frame = PolarisationFrame("stokesI")
    baselines = np.ones(num_baselines)
    source = "anonymous"
    low_precision = "float64"
    scan_id = 1
    scan_intent = "target"
    execblock_id = 1

    visibility = Visibility.constructor(
        frequency,
        channel_bandwidth,
        phasecentre=0,
        configuration=low,
        uvw=uvw,
        time=time,
        vis=vis,
        weight=weights,
        integration_time=integration_time,
        flags=flags,
        baselines=baselines,
        polarisation_frame=polarisation_frame,
        source=source,
        scan_id=scan_id,
        scan_intent=scan_intent,
        execblock_id=execblock_id,
        meta=None,
        low_precision=low_precision,
    )

    return visibility


@pytest.fixture
def client():
    """
    Generate dask client connected to local cluster for testing
    """

    cluster = LocalCluster(n_workers=2)
    client = Client(cluster)

    return client


@pytest.fixture
def mask_pairs():
    """
    Generate frequency pairs for rfi masking
    """

    masks = np.array([[1.3e7, 1.4e7], [1.6e7, 1.7e7]])

    return masks
