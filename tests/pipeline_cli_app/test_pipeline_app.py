import subprocess
from pathlib import Path

from pytest import TempPathFactory

from ska_sdp_batch_preprocess.apps.pipeline import run_program


def test_pipeline_cli_app_entry_point_exists():
    """
    Check that the pipeline CLI app is available to run with the expected name.
    """
    exit_code = subprocess.check_call(["ska-sdp-batch-preprocess", "--help"])
    assert exit_code == 0


def test_pipeline_cli_app(
    tmp_path_factory: TempPathFactory, input_ms_paths: list[Path]
):
    """
    Test the pipeline CLI app.
    """
    output_dir = tmp_path_factory.mktemp("output_dir")
    cli_args = [
        "--output-dir",
        str(output_dir),
    ]
    cli_args.extend([str(path) for path in input_ms_paths])

    run_program(cli_args)

    expected_output_ms_paths = [
        output_dir / path.name for path in input_ms_paths
    ]
    for output_ms in expected_output_ms_paths:
        assert output_ms.is_dir()
