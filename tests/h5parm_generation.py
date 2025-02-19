from pathlib import Path
from typing import Iterable

import numpy as np

from ska_sdp_batch_preprocess.h5parm import H5Parm


def create_scalarphase_identity_h5parm(
    path: Path, antenna_names: Iterable[str]
):
    """
    Create an H5Parm file containing one scalar phase soltab where all the
    phases are uniformly zero.
    """
    parm = H5Parm.from_complex_gain_data(
        axes={"ant": antenna_names},
        val=np.ones(len(antenna_names), dtype=complex),
        weight=np.ones(len(antenna_names)),
    )
    parm.save(path)


def create_diagonal_complex_identity_h5parm(
    path: Path, antenna_names: Iterable[str]
):
    """
    Create an H5Parm file containing one diagonal phase and one diagonal
    amplitude soltab such that the whole H5Parm represents the identity
    matrix.
    """
    parm = H5Parm.from_complex_gain_data(
        axes={"ant": antenna_names, "pol": ["XX", "YY"]},
        val=np.ones(shape=(len(antenna_names), 2), dtype=complex),
        weight=np.ones(shape=(len(antenna_names), 2)),
    )
    parm.save(path)


def create_fulljones_identity_h5parm(path: Path, antenna_names: Iterable[str]):
    """
    Create an H5Parm file representing a full Jones identity matrix.
    """
    shape = (len(antenna_names), 4)
    weight = np.ones(shape)
    val = np.zeros(shape, dtype=complex)
    val[:, (0, 3)] = 1

    parm = H5Parm.from_complex_gain_data(
        axes={"ant": antenna_names, "pol": ["XX", "XY", "YX", "YY"]},
        val=val,
        weight=weight,
    )
    parm.save(path)
