from pathlib import Path

import numpy as np
import pytest

from ska_sdp_batch_preprocess.config import Step
from ska_sdp_batch_preprocess.pipeline import Pipeline

from .ms_reading import load_msv2_flags, load_msv2_visibilities


def compute_msv2_visibility_rms_power(mset: Path) -> float:
    """
    Self-explanatory. Flagged visibility samples are excluded from the
    calculation.
    """
    vis = load_msv2_visibilities(mset)
    flag = load_msv2_flags(mset)
    vis[flag] = 0.0
    rms_power = (np.abs(vis) ** 2).mean() ** 0.5
    return float(rms_power)


# pylint:disable=line-too-long
def test_demixing_predicted_visibilities_with_same_sky_model_yields_zero_visibilities(  # noqa: E501
    tmp_path_factory: pytest.TempPathFactory,
    input_ms: Path,
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
        },
    )
    output_ms = tmpdir / "demixed.ms"
    pipeline = Pipeline([demixer_step])
    pipeline.run(input_ms, output_ms)

    input_rms_power = compute_msv2_visibility_rms_power(input_ms)
    output_rms_power = compute_msv2_visibility_rms_power(output_ms)
    assert output_rms_power < 1.0e-7 * input_rms_power
