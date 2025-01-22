from pathlib import Path
from typing import Iterable

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


def create_scalarphase_zeros_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = solset.create_group("phase000")
    soltab.attrs["TITLE"] = np.bytes_("phase")

    num_antennas = len(antenna_names)

    soltab.create_dataset(
        "ant", data=_ndarray_of_null_terminated_bytes(antenna_names)
    )

    val = soltab.create_dataset("val", data=np.zeros(shape=num_antennas))
    val.attrs["AXES"] = np.bytes_("ant")

    weight = soltab.create_dataset("weight", data=np.ones(shape=num_antennas))
    weight.attrs["AXES"] = np.bytes_("ant")


def create_diagonal_phase_zeros_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = solset.create_group("phase000")
    soltab.attrs["TITLE"] = np.bytes_("phase")

    pol_codes = ["XX", "YY"]

    num_antennas = len(antenna_names)
    num_pols = len(pol_codes)

    soltab.create_dataset(
        "ant", data=_ndarray_of_null_terminated_bytes(antenna_names)
    )
    soltab.create_dataset(
        "pol", data=_ndarray_of_null_terminated_bytes(pol_codes)
    )

    val = soltab.create_dataset(
        "val", data=np.zeros(shape=(num_antennas, num_pols))
    )
    val.attrs["AXES"] = np.bytes_("ant,pol")

    weight = soltab.create_dataset(
        "weight", data=np.ones(shape=(num_antennas, num_pols))
    )
    weight.attrs["AXES"] = np.bytes_("ant,pol")


def create_diagonal_amplitude_ones_soltab(
    solset: h5py.Group, *, antenna_names: Iterable[str]
):
    """
    Self-explanatory.
    """
    soltab = solset.create_group("amplitude000")
    soltab.attrs["TITLE"] = np.bytes_("amplitude")

    pol_codes = ["XX", "YY"]

    num_antennas = len(antenna_names)
    num_pols = len(pol_codes)

    soltab.create_dataset(
        "ant", data=_ndarray_of_null_terminated_bytes(antenna_names)
    )
    soltab.create_dataset(
        "pol", data=_ndarray_of_null_terminated_bytes(pol_codes)
    )

    val = soltab.create_dataset(
        "val", data=np.ones(shape=(num_antennas, num_pols))
    )
    val.attrs["AXES"] = np.bytes_("ant,pol")

    weight = soltab.create_dataset(
        "weight", data=np.ones(shape=(num_antennas, num_pols))
    )
    weight.attrs["AXES"] = np.bytes_("ant,pol")


def _ndarray_of_null_terminated_bytes(strings: Iterable[str]) -> NDArray:
    return np.asarray([s.encode() + b"\0" for s in strings])
