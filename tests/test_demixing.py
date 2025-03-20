from pathlib import Path

import numpy as np
import pytest

from ska_sdp_batch_preprocess.config import Step
from ska_sdp_batch_preprocess.pipeline import Pipeline

from .dp3_availability import skip_unless_dp3_available
from .ms_reading import load_msv2_flags, load_msv2_visibilities


@pytest.fixture(name="sky_model", scope="session")
def fixture_sky_model() -> Path:
    """
    Path to the sky model file used for the demixing test.
    """
    path = Path(__file__).parent / "data" / "sky_model_demixing.txt"
    return path.resolve()


@pytest.fixture(name="predicted_ms", scope="session")
def fixture_predicted_ms(
    tmp_path_factory: pytest.TempPathFactory,
    input_ms: Path,
    sky_model: Path,
) -> Path:
    """
    Path to a measurement set with identical structure to the usual test
    measurement set, but with its DATA column replaced by visibilities
    predicted from the given sky model, and its flags all set to False.
    """
    tmpdir = tmp_path_factory.mktemp("predicted_ms")
    steps = [
        Step("preflagger", params={"mode": "clear", "chan": "[0..nchan]"}),
        Step("predict", params={"sourcedb": sky_model}),
    ]
    pipeline = Pipeline(steps)
    output_ms = tmpdir / "predicted.ms"
    pipeline.run(input_ms, output_ms)
    return output_ms


def compute_msv2_visibility_rms_power(mset: Path) -> float:
    """
    Self-explanatory. Flagged visibility samples and NaNs are excluded from the
    calculation.
    """
    vis = load_msv2_visibilities(mset)
    flag = load_msv2_flags(mset)
    vis[flag] = 0.0
    rms_power = (np.abs(vis) ** 2).mean() ** 0.5
    return float(rms_power)


@skip_unless_dp3_available
# pylint:disable=line-too-long
def test_demixing_predicted_visibilities_with_same_sky_model_yields_zero_visibilities(  # noqa: E501
    tmp_path_factory: pytest.TempPathFactory,
    predicted_ms: Path,
    sky_model: Path,
):
    """
    Self-explanatory.
    """
    tmpdir = tmp_path_factory.mktemp("demixing_test")
    demixer_step = Step(
        "demixer",
        params={
            "skymodel": sky_model,
            "subtractsources": ["bright_a", "bright_b"],
            "instrumentmodel": tmpdir / "instrument",
        },
    )
    output_ms = tmpdir / "demixed.ms"
    pipeline = Pipeline([demixer_step])
    pipeline.run(predicted_ms, output_ms)

    input_rms_power = compute_msv2_visibility_rms_power(predicted_ms)
    output_rms_power = compute_msv2_visibility_rms_power(output_ms)
    assert output_rms_power < 1.0e-7 * input_rms_power
