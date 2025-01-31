import shutil
from pathlib import Path

import pytest
from dask.distributed import LocalCluster


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
    paths = []
    for _ in range(num_copies):
        tempdir = tmp_path_factory.mktemp("duplicated_data", numbered=True)
        destination = tempdir / input_ms.name
        shutil.copytree(input_ms, destination)
        paths.append(destination)
    return paths
