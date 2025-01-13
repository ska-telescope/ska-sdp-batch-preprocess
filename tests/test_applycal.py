import os
from pathlib import Path
from typing import Sequence

import casacore.tables
import h5py
import numpy as np
import pytest
from numpy.typing import NDArray

from ska_sdp_batch_preprocess.config import parse_config
from ska_sdp_batch_preprocess.pipeline import Pipeline

from .common import skip_unless_dp3_available
from .h5parm import create_fulljones_h5parm


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


def make_config_with_applycal_steps(h5parm_paths: list[Path]) -> dict:
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
    return {"steps": steps}


@skip_unless_dp3_available
def test_two_applycal_steps_with_gains_that_multiply_into_identity(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
):
    """
    Run a pipeline with two applycal steps, applying two gain tables whose
    product is the identity matrix. Check that the input and output
    visibilities are identical.
    """
    antenna_names = getcol(input_ms, "ANTENNA", "NAME")
    tempdir = tmp_path_factory.mktemp("applycal_test")

    table_twos_path = tempdir / "table_twos.h5parm"
    create_scalaramplitude_h5parm(
        table_twos_path, antenna_names=antenna_names, amplitude=2.0
    )

    table_halves_path = tempdir / "table_halves.h5parm"
    create_scalaramplitude_h5parm(
        table_halves_path, antenna_names=antenna_names, amplitude=0.5
    )

    conf = make_config_with_applycal_steps(
        [table_twos_path, table_halves_path]
    )
    steps = parse_config(conf)
    pipeline = Pipeline(steps)

    output_ms = tmp_path_factory.mktemp("applycal_test") / "output.ms"
    pipeline.run(input_ms, output_ms)

    vis_in = load_visibilities_from_msv2(input_ms)
    vis_out = load_visibilities_from_msv2(output_ms)
    assert np.allclose(vis_in, vis_out)


@skip_unless_dp3_available
def test_two_fulljones_applycal_steps_with_mutually_cancelling_gains(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
):
    """
    Self-explanatory.
    """
    antenna_names = getcol(input_ms, "ANTENNA", "NAME")
    tempdir = tmp_path_factory.mktemp("applycal_test")

    # TODO: choose a reasonable jones matrix close enough to identity
    jones = np.eye(2, dtype="complex") * 2 * np.exp(1.0j * np.pi / 3)
    jones_path = tempdir / "jones.h5parm"
    create_fulljones_h5parm(
        jones_path, antenna_names=antenna_names, complex_gains_2x2=jones
    )

    jones_inv = np.linalg.inv(jones)
    jones_inv_path = tempdir / "jones_inv.h5parm"
    create_fulljones_h5parm(
        jones_inv_path,
        antenna_names=antenna_names,
        complex_gains_2x2=jones_inv,
    )

    # NOTE: "soltab" must be given in [amplitude, phase] order.
    conf = {
        "steps": [
            {
                "ApplyCal": {
                    "parmdb": str(jones_path.resolve()),
                    "correction": "fulljones",
                    "soltab": ["amplitude000", "phase000"],
                }
            },
            {
                "ApplyCal": {
                    "parmdb": str(jones_inv_path.resolve()),
                    "correction": "fulljones",
                    "soltab": ["amplitude000", "phase000"],
                }
            },
        ]
    }

    steps = parse_config(conf)
    pipeline = Pipeline(steps)

    output_ms = tempdir / "output.ms"
    pipeline.run(input_ms, output_ms)

    vis_in = load_visibilities_from_msv2(input_ms)
    vis_out = load_visibilities_from_msv2(output_ms)
    assert np.allclose(vis_in, vis_out)
