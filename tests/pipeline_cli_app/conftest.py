import shutil
from pathlib import Path

import pytest
from dask.distributed import LocalCluster

from ..h5parm_generation import create_diagonal_complex_identity_h5parm
from ..ms_reading import load_antenna_names_from_msv2


@pytest.fixture
def dask_cluster() -> LocalCluster:
    """
    Dask cluster used for distribution tests.
    """
    return LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        resources={"subprocess": 1},
    )


@pytest.fixture
def input_ms_list(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
) -> list[Path]:
    """
    List of measurement sets obtained by copying the test MS multiple times.
    Used for distribution tests.
    """
    num_copies = 8
    tempdir = tmp_path_factory.mktemp("distributed_test_data")
    paths = [tempdir / f"input_{index:02d}.ms" for index in range(num_copies)]
    for path in paths:
        shutil.copytree(input_ms, path)
    return paths


@pytest.fixture
def diagonal_identity_h5parm(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
) -> Path:
    """
    Complex diagonal identity H5Parm file. Its name must match what is in the
    config file's applycal step.
    """
    solutions_dir = tmp_path_factory.mktemp("solutions_dir")
    antenna_names = load_antenna_names_from_msv2(input_ms)
    path = solutions_dir / "diagonal.h5"
    create_diagonal_complex_identity_h5parm(path, antenna_names)
    return path
