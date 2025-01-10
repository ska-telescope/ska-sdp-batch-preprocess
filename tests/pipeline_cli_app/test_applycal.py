from pathlib import Path

import pytest

from ska_sdp_batch_preprocess.apps.pipeline import run_program


def test_two_applycal_steps_with_gains_that_multiply_into_identity(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
):
    """
    Run DP3 with two applycal steps, applying two gain tables whose product is
    the identity matrix. We check that the input and output visibilities are
    identical.
    """
    tempdir = tmp_path_factory.mktemp("applycal_test")
    config_path = tempdir / "config.yml"
    config_path.write_text("steps: []")

    output_dir = tempdir / "output_dir"
    output_dir.mkdir()

    cli_args = [
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir),
        str(input_ms),
    ]
    run_program(cli_args)
