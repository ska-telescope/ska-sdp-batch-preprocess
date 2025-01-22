import subprocess
from pathlib import Path

import pytest

from ska_sdp_batch_preprocess.apps.pipeline import run_program

from ..common import skip_unless_dp3_available


@pytest.fixture(name="yaml_config")
def fixture_yaml_config() -> Path:
    """
    YAML config file path for the end-to-end test.
    """
    return Path(__file__).parent / "config.yaml"


def test_pipeline_cli_app_entry_point_exists():
    """
    Check that the pipeline CLI app is available to run with the expected name.
    """
    exit_code = subprocess.check_call(["ska-sdp-batch-preprocess", "--help"])
    assert exit_code == 0


@skip_unless_dp3_available
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
        "--dask-scheduler",
        "localhost:8786",
        str(input_ms),
    ]

    run_program(cli_args)

    expected_output_ms_path = output_dir / input_ms.name
    assert expected_output_ms_path.is_dir()
