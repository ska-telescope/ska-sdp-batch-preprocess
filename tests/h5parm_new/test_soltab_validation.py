import numpy as np
import pytest

from ska_sdp_batch_preprocess.h5parm_new import InvalidH5Parm, Soltab


def create_empty_soltab(soltype: str, axes: dict) -> Soltab:
    """
    Create a Soltab with all zero values and all weights one.
    """
    shape = tuple(len(data) for data in axes.values())
    return Soltab(
        soltype=soltype,
        axes=axes,
        val=np.zeros(shape),
        weight=np.ones(shape),
    )


def test_creating_soltab_with_invalid_soltype_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    with pytest.raises(InvalidH5Parm, match="Invalid solution type"):
        create_empty_soltab("zzz", axes={"time": [1]})


def test_creating_soltab_with_invalid_axes_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    with pytest.raises(
        InvalidH5Parm, match="Soltab contains invalid axis names"
    ):
        create_empty_soltab("phase", axes={"zzz": [1]})


def test_creating_soltab_with_multidimensional_axes_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    with pytest.raises(
        InvalidH5Parm, match="Soltab axis .+ not 1-dimensional"
    ):
        create_empty_soltab("phase", axes={"time": [(1, 2)]})


def test_creating_soltab_with_invalid_pol_codes_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    with pytest.raises(InvalidH5Parm, match="Soltab pol axis data is invalid"):
        create_empty_soltab("phase", axes={"pol": ["zzz"]})


def test_creating_soltab_with_differently_shaped_val_and_weight_raises_invalid_h5parm():  # noqa: E501
    """
    Self-explanatory.
    """
    regex = "Soltab values and weights have different dimensions"

    with pytest.raises(InvalidH5Parm, match=regex):
        val = np.zeros(shape=(1,))
        weight = np.zeros(shape=(1, 2))
        Soltab("phase", axes={"time": [1]}, val=val, weight=weight)


def test_creating_soltab_with_axes_length_inconsistent_with_array_shapes_raises_invalid_h5parm():  # noqa: E501
    """
    Self-explanatory.
    """
    regex = "Soltab .+ inconsistent with the shape implied by the axes lengths"

    with pytest.raises(InvalidH5Parm, match=regex):
        val = np.zeros(shape=(1, 2, 3, 4))
        weight = np.ones_like(val)
        Soltab("phase", axes={"time": [1]}, val=val, weight=weight)
