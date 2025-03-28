import zipfile
from pathlib import Path

import pytest

from ska_sdp_batch_preprocess.config import Step
from ska_sdp_batch_preprocess.pipeline import Pipeline


@pytest.fixture(name="sky_model", scope="session")
def fixture_sky_model() -> Path:
    """
    Path to the sky model file used to generate the test MS.
    """
    path = Path(__file__).parent / "data" / "sky_model.txt"
    return path.resolve()


@pytest.fixture(name="template_ms", scope="session")
def fixture_template_ms(
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """
    A very small MeerKAT dataset observed at L-Band which serves at a
    template to generate another MS where we control which sources we inject.
    The dataset has 38 time samples, 4 freq channels and the 4 linear
    polarisation channels.
    """
    archive = Path(__file__).parent / "data" / "mkt_ecdfs25_nano.ms.zip"
    datasets_tmpdir = Path(tmp_path_factory.mktemp("datasets"))
    # pylint: disable=consider-using-with
    zipfile.ZipFile(archive).extractall(datasets_tmpdir)
    return datasets_tmpdir / "mkt_ecdfs25_nano.ms"


@pytest.fixture(name="input_ms", scope="session")
def fixture_input_ms(
    template_ms: Path,
    sky_model: Path,
) -> Path:
    """
    Generates a small MeerKAT dataset from the sky model using DP3.
    This serves as the input to most of our tests.

    Notes:
      - All flags set to False (perfect data).
      - The dataset has 38 time samples, 4 freq channels and the 4 linear
        polarisation channels.
    """
    steps = [
        Step("preflagger", params={"mode": "clear", "chan": "[0..nchan]"}),
        Step("predict", params={"sourcedb": str(sky_model)}),
    ]
    pipeline = Pipeline(steps)
    predicted_ms = template_ms.parent / "predicted.ms"
    pipeline.run(template_ms, predicted_ms)
    return predicted_ms
