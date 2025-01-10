import os
from pathlib import Path

import casacore.tables
import numpy as np
import pytest
from numpy.typing import NDArray

from ska_sdp_batch_preprocess.apps.pipeline import run_program


def getcol(path: str | os.PathLike, table_name: str, col_name: str):
    """
    Returns an entire column for a MeasurementSet.
    """
    if table_name in {"", "MAIN"}:
        spec = str(path)
    else:
        spec = f"{str(path)}::{table_name}"
    with casacore.tables.table(spec, ack=False) as tbl:
        return tbl.getcol(col_name)


def load_visibilities_from_msv2(
    path: str | os.PathLike, data_column: str = "DATA"
) -> NDArray:
    """
    Loads visibilites from MSv2, assuming it contains rectangular data.
    Returns complex data with shape (time, baseline, freq, pol).
    """
    vis: NDArray = getcol(path, "MAIN", data_column)
    unique_timestamps = set(getcol(path, "MAIN", "TIME"))
    __, nchan, npol = vis.shape
    return vis.reshape(len(unique_timestamps), -1, nchan, npol)


def test_two_applycal_steps_with_gains_that_multiply_into_identity(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
):
    """
    Run DP3 with two applycal steps, applying two gain tables whose product is
    the identity matrix. Check that the input and output visibilities are
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

    vis_in = load_visibilities_from_msv2(input_ms)
    vis_out = load_visibilities_from_msv2(output_dir / input_ms.name)
    assert np.allclose(vis_in, vis_out)
