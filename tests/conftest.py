import zipfile
from pathlib import Path

import pytest


@pytest.fixture(name="input_ms", scope="session")
def fixture_input_ms(
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """
    A very small MeerKAT dataset observed at L-Band.
    The dataset has 38 time samples, 4 freq channels and the 4 linear
    polarisation channels.
    """
    archive = Path(__file__).parent / "data" / "mkt_ecdfs25_nano.ms.zip"
    datasets_tmpdir = Path(tmp_path_factory.mktemp("datasets"))
    # pylint: disable=consider-using-with
    zipfile.ZipFile(archive).extractall(datasets_tmpdir)
    return datasets_tmpdir / "mkt_ecdfs25_nano.ms"
