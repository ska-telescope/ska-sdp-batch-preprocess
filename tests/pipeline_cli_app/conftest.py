import uuid
from pathlib import Path

import pytest
from pytest import TempPathFactory


@pytest.fixture(scope="session")
def input_ms_paths(tmp_path_factory: TempPathFactory) -> list[Path]:
    """
    Creates a list of input file paths to pass to the pipeline CLI app.
    """
    tempdir = tmp_path_factory.mktemp("input_dir")
    return [
        create_fake_ms_directory(tempdir, f"data_{index}.ms")
        for index in range(2)
    ]


def create_fake_ms_directory(parent_dir: Path, name: str) -> Path:
    """
    Create an MS directory called `name` as a sub-directory of `parent_dir`.
    """
    assert parent_dir.is_dir()

    base_dir = parent_dir / name
    base_dir.mkdir()

    table_path = base_dir / "table.f0"
    with open(table_path, "w", encoding="utf-8") as file:
        file.write(str(uuid.uuid4()))

    return base_dir
