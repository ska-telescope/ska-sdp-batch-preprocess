import shutil
from pathlib import Path

import pytest
from dask.distributed import LocalCluster

from ..h5parm_generation import create_diagonal_complex_identity_h5parm
from ..ms_reading import load_msv2_antenna_names


@pytest.fixture
def yaml_config() -> Path:
    """
    YAML config file path for the end-to-end test.
    """
    return Path(__file__).parent / "config.yaml"


@pytest.fixture
def yaml_config_trivial() -> Path:
    """
    Trivial YAML config file path, just to test some error paths of the CLI
    app.
    """
    return Path(__file__).parent / "config_trivial.yaml"


@pytest.fixture
def extra_inputs_dir(
    tmp_path_factory: pytest.TempPathFactory,
    input_ms: Path,
    sky_model: Path,
) -> Path:
    """
    Extra inputs directory with all necessary files required to run the
    pipeline with the end-to-end test config.
    """
    tempdir = tmp_path_factory.mktemp("extra_inputs_dir")
    antenna_names = load_msv2_antenna_names(input_ms)
    path = tempdir / "diagonal.h5"
    create_diagonal_complex_identity_h5parm(path, antenna_names)
    shutil.copy(sky_model, tempdir / sky_model.name)
    return tempdir


@pytest.fixture
def dask_cluster() -> LocalCluster:
    """
    Dask cluster used for distribution tests.
    """
    return LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        resources={"process": 1},
    )


@pytest.fixture
def input_ms_list(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
) -> list[Path]:
    """
    List of measurement sets obtained by copying the test MS multiple times.
    Used for distribution tests.
    """
    num_copies = 2
    tempdir = tmp_path_factory.mktemp("distributed_test_data")
    paths = [tempdir / f"input_{index:02d}.ms" for index in range(num_copies)]
    for path in paths:
        shutil.copytree(input_ms, path)
    return paths
