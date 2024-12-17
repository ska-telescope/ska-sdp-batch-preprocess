import os
import subprocess
from pathlib import Path

import pytest

from ska_sdp_batch_preprocess.apps.pipeline import run_program


def dp3_available() -> bool:
    """
    True if DP3 is available to run via CLI.
    """
    try:
        subprocess.check_call(
            ["DP3"], env=os.environ | {"OPENBLAS_NUM_THREADS": "1"}
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def test_pipeline_cli_app_entry_point_exists():
    """
    Check that the pipeline CLI app is available to run with the expected name.
    """
    exit_code = subprocess.check_call(["ska-sdp-batch-preprocess", "--help"])
    assert exit_code == 0


@pytest.mark.skipif(not dp3_available(), reason="DP3 not available")
def test_pipeline_cli_app(
    tmp_path_factory: pytest.TempPathFactory, yaml_config: Path, input_ms: Path
):
    """
    Test the pipeline CLI app on a small Measurement Set.
    """
    output_dir = tmp_path_factory.mktemp("output_dir")
    cli_args = [
        "--config",
        str(yaml_config),
        "--output-dir",
        str(output_dir),
        str(input_ms),
    ]

    run_program(cli_args)

    expected_output_ms_path = output_dir / input_ms.name
    assert expected_output_ms_path.is_dir()
