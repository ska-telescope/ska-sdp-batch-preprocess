from pathlib import Path
from typing import Iterable, Literal

import h5py
import numpy as np
from numpy.typing import NDArray


def create_scalarphase_identity_h5parm(
    path: Path, *, antenna_names: Iterable[str]
):
    """
    Create an H5Parm file containing a 'scalarphase' table where all the phases
    are uniformly zero.
    """
    with h5py.File(path, mode="w") as file:
        solset = file.create_group("sol000")
        create_scalarphase_zeros_soltab(solset, antenna_names=antenna_names)


def create_diagonal_complex_identity_h5parm(
    path: Path, *, antenna_names: Iterable[str]
):
    """
    Create an H5Parm file containing one diagonal phase and one diagonal
    amplitude soltab such that the whole H5Parm represents the identity
    matrix.
    """
    with h5py.File(path, mode="w") as file:
        solset = file.create_group("sol000")
        create_diagonal_phase_zeros_soltab(solset, antenna_names=antenna_names)
        create_diagonal_amplitude_ones_soltab(
            solset, antenna_names=antenna_names
        )


def create_fulljones_identity_h5parm(
    path: Path, *, antenna_names: Iterable[str]
):
    """
    Create an H5Parm file representing a full Jones identity matrix.
    """
    with h5py.File(path, mode="w") as file:
        solset = file.create_group("sol000")
        create_fulljones_phase_zeros_soltab(
            solset, antenna_names=antenna_names
        )
        create_fulljones_amplitude_ones_soltab(
            solset, antenna_names=antenna_names
        )


def create_scalarphase_zeros_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = create_soltab_group(solset, "phase")
    num_antennas = len(antenna_names)
    write_ant_dataset(soltab, antenna_names)
    write_val_and_weight_datasets(soltab, np.zeros(num_antennas), ["ant"])


def create_diagonal_phase_zeros_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = create_soltab_group(solset, "phase")
    num_antennas = len(antenna_names)
    write_ant_dataset(soltab, antenna_names)
    write_pol_dataset(soltab, ["XX", "YY"])
    write_val_and_weight_datasets(
        soltab, np.zeros((num_antennas, 2)), ["ant", "pol"]
    )


def create_diagonal_amplitude_ones_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = create_soltab_group(solset, "amplitude")
    num_antennas = len(antenna_names)
    write_ant_dataset(soltab, antenna_names)
    write_pol_dataset(soltab, ["XX", "YY"])
    write_val_and_weight_datasets(
        soltab, np.ones((num_antennas, 2)), ["ant", "pol"]
    )


def create_fulljones_phase_zeros_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = create_soltab_group(solset, "phase")
    num_antennas = len(antenna_names)
    write_ant_dataset(soltab, antenna_names)
    write_pol_dataset(soltab, ["XX", "XY", "YX", "YY"])
    write_val_and_weight_datasets(
        soltab, np.zeros((num_antennas, 4)), ["ant", "pol"]
    )


def create_fulljones_amplitude_ones_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = create_soltab_group(solset, "amplitude")
    num_antennas = len(antenna_names)
    write_ant_dataset(soltab, antenna_names)
    write_pol_dataset(soltab, ["XX", "XY", "YX", "YY"])
    data = np.zeros((num_antennas, 4))
    data[:] = (1, 0, 0, 1)
    write_val_and_weight_datasets(soltab, data, ["ant", "pol"])


def create_soltab_group(
    solset: h5py.Group, solution_type: Literal["amplitude", "phase"]
) -> h5py.Group:
    """
    Create soltab group under given solset group. 'solution_type' must be
    'amplitude' or 'phase'.
    """
    soltab = solset.create_group(f"{solution_type}000")
    soltab.attrs["TITLE"] = np.bytes_(solution_type)
    return soltab


def write_ant_dataset(soltab: h5py.Group, antenna_names: Iterable[str]):
    """
    Self-explanatory.
    """
    soltab.create_dataset(
        "ant", data=_ndarray_of_null_terminated_bytes(antenna_names)
    )


def write_pol_dataset(soltab: h5py.Group, pol_codes: Iterable[str]):
    """
    Self-explanatory.
    """
    soltab.create_dataset(
        "pol", data=_ndarray_of_null_terminated_bytes(pol_codes)
    )


def write_val_and_weight_datasets(
    soltab: h5py.Group, val: NDArray, axis_names: Iterable[str]
):
    """
    Write 'val' dataset with the given values and axis names;
    also write a corresponding 'weight' dataset with the same shape, filled
    with ones.
    """
    axes_attribute = np.bytes_(",".join(axis_names))
    val_dataset = soltab.create_dataset("val", data=val)
    val_dataset.attrs["AXES"] = axes_attribute
    weight_dataset = soltab.create_dataset(
        "weight", data=np.ones(shape=val.shape, dtype=float)
    )
    weight_dataset.attrs["AXES"] = axes_attribute


def _ndarray_of_null_terminated_bytes(strings: Iterable[str]) -> NDArray:
    return np.asarray([s.encode() + b"\0" for s in strings])
