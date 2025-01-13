from pathlib import Path
from typing import Literal, Sequence

import h5py
import numpy as np
from numpy.typing import NDArray


def create_fulljones_h5parm(
    path: Path, *, antenna_names: Sequence[str], complex_gains_2x2: NDArray
):
    """
    Create an H5Parm file containing the same Jones matrix (in a linear pol
    frame) repeated for all antennas. The created H5Parm contains two soltabs
    'amplitude000' and 'phase000' with two axes 'ant' and 'pol'.
    """
    complex_gains_2x2 = np.asarray(complex_gains_2x2, dtype="complex").reshape(
        2, 2
    )

    with h5py.File(path, "w") as file:
        solset_group = file.create_group("sol000")  # name is arbitrary
        _write_soltab(
            solset_group, "amplitude", antenna_names, np.abs(complex_gains_2x2)
        )
        _write_soltab(
            solset_group, "phase", antenna_names, np.angle(complex_gains_2x2)
        )


def _write_soltab(
    solset_group: h5py.Group,
    soltab_type: Literal["amplitude", "phase"],
    antenna_names: Sequence[str],
    values_2x2: NDArray,
):
    group = solset_group.create_group(soltab_type + "000")
    # NOTE: For fulljones H5Parms, TITLE must exist for both soltabs but its
    # value is ignored.
    group.attrs["TITLE"] = np.bytes_(soltab_type)

    _write_ant(group, antenna_names)
    _write_pol(group)

    shape = (len(antenna_names), 4)
    _write_val(group, shape, values_2x2)
    _write_weight(group, shape)


def _write_val(group: h5py.Group, shape: tuple[int], values: NDArray):
    dataset = group.create_dataset("val", shape=shape)
    dataset.attrs["AXES"] = np.bytes_("ant,pol")
    dataset[:] = values.ravel()


def _write_weight(group: h5py.Group, shape: tuple[int]):
    dataset = group.create_dataset("weight", shape=shape)
    dataset.attrs["AXES"] = np.bytes_("ant,pol")
    dataset[:] = 1.0


def _write_ant(group: h5py.Group, antenna_names: Sequence[str]):
    group.create_dataset(
        "ant", data=_ndarray_of_null_terminated_bytes(antenna_names)
    )


def _write_pol(group: h5py.Group):
    pol_names = ["XX", "XY", "YX", "YY"]
    group.create_dataset(
        "pol", data=_ndarray_of_null_terminated_bytes(pol_names)
    )


def _ndarray_of_null_terminated_bytes(strings: Sequence[str]) -> NDArray:
    return np.asarray([s.encode() + b"\0" for s in strings])
