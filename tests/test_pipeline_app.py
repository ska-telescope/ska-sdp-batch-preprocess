import subprocess

from ska_sdp_batch_preprocess.apps.pipeline import run_program


def test_pipeline_cli_app_entry_point_exists():
    """
    Check that the pipeline app is available to run with the expected name.
    """
    exit_code = subprocess.check_call(["ska-sdp-batch-preprocess", "--help"])
    assert exit_code == 0


def test_pipeline_cli_app():
    """
    Test the pipeline CLI app.
    """
    cli_args = [
        "--output-dir",
        "/path/to/outdir",
        "input_1.ms",
        "input_2.ms",
    ]
    run_program(cli_args)
