from pathlib import Path

import pytest
from pytest import TempPathFactory


@pytest.fixture(scope="session")
def input_ms_path_list(tmp_path_factory: TempPathFactory) -> list[Path]:
    """
    Creates a list of input file paths to pass to the pipeline CLI app.
    """
    tempdir = tmp_path_factory.mktemp("input")
    paths = []

    for index in range(2):
        path = tempdir / f"data_{index}.ms"
        with open(path, "w", encoding="utf-8") as file:
            file.write(f"Data {index}")
        paths.append(path)

    return paths
