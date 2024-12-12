import zipfile
from pathlib import Path

import pytest


@pytest.fixture(name="input_ms", scope="module")
def fixture_input_ms(
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """
    A very small MeerKAT dataset observed at L-Band.
    The dataset has 38 time samples, 4 freq channels and the 4 linear
    polarisation channels.
    """
    archive = Path(__file__).parent / "mkt_ecdfs25_nano.ms.zip"
    datasets_tmpdir = Path(tmp_path_factory.mktemp("xradio_datasets"))
    # pylint: disable=consider-using-with
    zipfile.ZipFile(archive).extractall(datasets_tmpdir)
    return datasets_tmpdir / "mkt_ecdfs25_nano.ms"


@pytest.fixture(name="yaml_config", scope="session")
def fixture_yaml_config() -> Path:
    """
    YAML config file path.
    """
    return Path(__file__).parent / ".." / ".." / "config" / "config.yaml"
