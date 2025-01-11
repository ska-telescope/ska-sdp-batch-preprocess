import os
from pathlib import Path
from typing import Sequence

import casacore.tables
import h5py
import numpy as np
import pytest
import yaml
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


def create_scalaramplitude_h5parm(
    path: Path, *, antenna_names: Sequence[str], amplitude: float
):
    """
    Create an h5parm file containing scalar amplitude gains with a constant
    value across all axes. We generate a gain table with only the 'ant' axis
    (antenna names); DP3 then broadcasts the values across all timestamps and
    frequency channels.
    """

    def _ndarray_of_null_terminated_bytes(strings: Sequence[str]) -> NDArray:
        return np.asarray([s.encode() + b"\0" for s in strings])

    num_ants = len(antenna_names)

    with h5py.File(path, mode="w") as file:
        # The name of the solset is arbitrary
        group = file.create_group("sol000/amplitude000")
        # Cast to np.bytes_ because all text attributes must be written as
        # fixed-length string, otheriwse DP3 complains.
        group.attrs["TITLE"] = np.bytes_("amplitude")

        # NOTE: We need to null-terminate the antenna names, otherwise DP3
        # pretends it cannot find them in the H5Parm. I think this is because
        # it must be loading the antenna names from the MS first, as
        # null-terminated strings.
        group.create_dataset(
            "ant", data=_ndarray_of_null_terminated_bytes(antenna_names)
        )

        axes = np.bytes_("ant")
        data_shape = (num_ants,)
        val_dataset = group.create_dataset(
            "val", data=np.full(data_shape, amplitude)
        )
        val_dataset.attrs["AXES"] = axes
        weight_dataset = group.create_dataset(
            "weight", data=np.full(data_shape, 1.0)
        )
        weight_dataset.attrs["AXES"] = axes


def make_yaml_config_with_applycal_steps(h5parm_paths: list[Path]) -> str:
    """
    Self-explanatory.
    """
    steps = [
        {
            "ApplyCal": {
                "parmdb": str(path.resolve()),
                "correction": "amplitude000",
            }
        }
        for path in h5parm_paths
    ]
    return yaml.safe_dump({"steps": steps})


def test_two_applycal_steps_with_gains_that_multiply_into_identity(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
):
    """
    Run DP3 with two applycal steps, applying two gain tables whose product is
    the identity matrix. Check that the input and output visibilities are
    identical.
    """
    tempdir = tmp_path_factory.mktemp("applycal_test")

    antenna_names = getcol(input_ms, "ANTENNA", "NAME")
    table_twos_path = tempdir / "table_twos.h5parm"
    create_scalaramplitude_h5parm(
        table_twos_path, antenna_names=antenna_names, amplitude=2.0
    )
    table_halves_path = tempdir / "table_halves.h5parm"
    create_scalaramplitude_h5parm(
        table_halves_path, antenna_names=antenna_names, amplitude=0.5
    )

    config_path = tempdir / "config.yml"
    config_path.write_text(
        make_yaml_config_with_applycal_steps(
            [table_twos_path, table_halves_path]
        )
    )

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
