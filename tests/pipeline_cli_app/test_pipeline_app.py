import subprocess
from pathlib import Path

import pytest
from dask.distributed import LocalCluster

from ska_sdp_batch_preprocess.apps.batch_preprocessing import run_program

from ..dp3_availability import skip_unless_dp3_available


def test_pipeline_cli_app_entry_point_exists():
    """
    Check that the pipeline CLI app is available to run with the expected name.
    """
    exit_code = subprocess.check_call(["ska-sdp-batch-preprocess", "--help"])
    assert exit_code == 0


@skip_unless_dp3_available
def test_pipeline_cli_app_produces_output_ms_without_errors_in_sequential_mode(
    tmp_path_factory: pytest.TempPathFactory,
    yaml_config: Path,
    extra_inputs_dir: Path,
    input_ms: Path,
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
        "--extra-inputs-dir",
        str(extra_inputs_dir),
        str(input_ms),
    ]

    run_program(cli_args)

    expected_output_ms_path = output_dir / input_ms.name
    assert expected_output_ms_path.is_dir()


def test_pipeline_cli_app_raises_value_error_if_duplicate_input_ms_names(
    tmp_path_factory: pytest.TempPathFactory,
    yaml_config_trivial: Path,
    input_ms: Path,
):
    """
    Self-explanatory.
    """
    output_dir = tmp_path_factory.mktemp("output_dir")
    cli_args = [
        "--config",
        str(yaml_config_trivial),
        "--output-dir",
        str(output_dir),
        str(input_ms),
        str(input_ms),
    ]

    with pytest.raises(
        ValueError, match="There are duplicate input MS path stems"
    ):
        run_program(cli_args)


@skip_unless_dp3_available
# pylint:disable=line-too-long
def test_pipeline_cli_app_produces_output_mses_without_errors_in_distributed_mode(  # noqa: E501
    tmp_path_factory: pytest.TempPathFactory,
    yaml_config: Path,
    extra_inputs_dir: Path,
    dask_cluster: LocalCluster,
    input_ms_list: list[Path],
):
    """
    Test the pipeline CLI app in distributed mode on multiple copies of the
    test measurement set.
    """
    output_dir = tmp_path_factory.mktemp("output_dir")

    cli_args = [
        "--config",
        str(yaml_config),
        "--extra-inputs-dir",
        str(extra_inputs_dir),
        "--output-dir",
        str(output_dir),
        "--dask-scheduler",
        str(dask_cluster.scheduler_address),
    ] + list(map(str, input_ms_list))

    run_program(cli_args)

    expected_output_paths = [output_dir / path.name for path in input_ms_list]
    for path in expected_output_paths:
        assert path.is_dir()
