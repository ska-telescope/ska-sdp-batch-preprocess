import os
from typing import Iterable, Literal, Optional, get_args

import h5py
import numpy as np
from numpy.typing import ArrayLike, NDArray

from .assertions import assert_or_invalid_h5parm, assert_or_value_error

SoltabAxisName = Literal["time", "freq", "ant", "pol", "dir"]
VALID_AXIS_NAMES = get_args(SoltabAxisName)
STRING_TYPED_AXIS_NAMES = {"ant", "pol", "dir"}

SoltabType = Literal["amplitude", "phase"]  # TODO: add all others
VALID_SOLTYPES = get_args(SoltabType)

VALID_DATASET_NAMES = {"val", "weight"}
VALID_POL_AXIS_DATA = {
    ("XX", "YY"),
    ("XX", "XY", "YX", "YY"),
}

# These are the data types used in the schaapcommon code that writes H5Parm
# Schaapcommon might support reading other types, but we're not risking it yet
FLOAT_AXES_DTYPE = np.float64
VALUES_DTYPE = np.float64
WEIGHTS_DTYPE = np.float32


class Soltab:
    """
    Represents the data of an H5Parm solution table with axes, values, and
    weights.
    """

    def __init__(
        self,
        soltype: SoltabType,
        axes: dict[SoltabAxisName, ArrayLike],
        values: ArrayLike,
        weights: ArrayLike,
        name: Optional[str] = None,
    ):
        """
        Initializes a Soltab instance.

        Args:
            soltype: A string that indicates the type of solution table; this
                maps to the `TITLE` attribute of the Soltab in an H5Parm file.
            axes: Mapping axis name to axis values, which describes the axes
                of the `values` and `weights` arrays. Key order is important
                and must match the dimension order in those arrays.
                Allowed keys are: `time, freq, ant, pol, dir`.
                Values should be numpy arrays or array-likes.
            values: The main data values, as a numpy array or array-like.
            weights: The data weights associated with the values, as a numpy
                array or array-like; must have the same shape as `values`.
            name: Internal use only, this is the HDF5 group name of the soltab
                if it has been loaded from an H5Parm file.
        """
        self.__soltype = str(soltype)
        self.__axes = prepare_axes_dict(axes)
        self.__values = np.asarray(values, dtype=VALUES_DTYPE)
        self.__weights = np.asarray(weights, dtype=WEIGHTS_DTYPE)
        self.__name = str(name) if name is not None else None
        validate_soltab(self)

    @property
    def name(self) -> Optional[str]:
        """
        HDF5 group name of the soltab if it was loaded from an H5Parm file,
        otherwise `None`.
        """
        return self.__name

    @property
    def soltype(self) -> SoltabType:
        """
        A string that indicates the type of solution table; this maps to the
        `TITLE` attribute of the Soltab in an H5Parm file.
        """
        return self.__soltype

    @property
    def axes(self) -> dict[SoltabAxisName, NDArray]:
        """
        Mapping axis name to data values; keys are in the same order as the
        dimension order in the `values` and `weights` arrays.
        """
        # Allow mutation of the axis values, but not of the member dict itself
        return dict(self.__axes)

    @property
    def values(self) -> NDArray:
        """
        Array of values.
        """
        return self.__values

    @property
    def weights(self) -> NDArray:
        """
        Array of weights associated with the values, with the same shape.
        """
        return self.__weights

    @property
    def dimensions(self) -> dict[SoltabAxisName, int]:
        """
        Mapping axis name to axis length; keys are in the same order as the
        dimension order in the `values` and `weights` arrays.
        """
        return {key: len(arr) for key, arr in self.axes.items()}

    def __str__(self) -> str:
        clsname = type(self).__name__
        return (
            f"{clsname}(name={self.name!r}, soltype={self.soltype!r}, "
            f"dimensions={self.dimensions!r})"
        )

    def __repr__(self) -> str:
        return str(self)


def validate_soltab(soltab: Soltab):
    """
    Check that a Soltab instance is valid, raise an ValueError otherwise.
    """
    assert_or_value_error(
        soltab.soltype in VALID_SOLTYPES,
        f"Invalid solution type: {soltab.soltype!r}",
    )

    axis_names = set(soltab.axes.keys())
    assert_or_value_error(
        set(soltab.axes.keys()).issubset(VALID_AXIS_NAMES),
        f"Soltab contains invalid axis names: {axis_names!r}",
    )

    for key, data in soltab.axes.items():
        assert_or_value_error(
            data.ndim == 1, f"Soltab axis {key!r} is not 1-dimensional"
        )

    pols = soltab.axes.get("pol", None)
    if pols is not None:
        pols_str_tuple = tuple(map(str, pols))
        assert_or_value_error(
            pols_str_tuple in VALID_POL_AXIS_DATA,
            f"Soltab pol axis data is invalid: {pols_str_tuple!r}, "
            f"data should be one of {VALID_POL_AXIS_DATA!r}",
        )

    assert_or_value_error(
        soltab.values.shape == soltab.weights.shape,
        "Soltab values and weights have different dimensions",
    )

    axes_shape = tuple(soltab.dimensions.values())
    assert_or_value_error(
        axes_shape == soltab.values.shape,
        f"Soltab values and weights shape {soltab.values.shape!r} "
        "is inconsistent with the shape implied by the axes lengths "
        f"{axes_shape!r}",
    )


def read_soltab_from_hdf5_group(group: h5py.Group) -> Soltab:
    """
    Read Soltab from the associated HDF5 group; underlying file must be open
    for reading.
    """
    name = os.path.basename(group.name)
    title = read_title_attribute(group)
    axes = read_soltab_axes(group)
    values, values_axes = read_dataset_with_named_axes(group, "val")
    weights, weight_axes = read_dataset_with_named_axes(group, "weight")
    assert_or_invalid_h5parm(
        values_axes == weight_axes,
        f"val and weight datasets under soltab {group.name!r} "
        "have metadata specifying different axes",
    )
    # Reorder "axes" dict to match order specified by datasets
    # Checking consistency between axis order in "axes" dict and the
    # dimensions of the datasets is left to __init__
    axes = {key: axes[key] for key in values_axes}
    return Soltab(title, axes, values, weights, name=name)


def write_soltab_to_hdf5_group(soltab: Soltab, group: h5py.Group) -> "Soltab":
    """
    Write to HDF5 group that represents a soltab (i.e. a member of
    a solset group); underlying file must be open for writing.
    Any pre-existing contents of the group are deleted.
    """
    # Re-validate because axis values are mutable
    validate_soltab(soltab)
    group.clear()
    group.attrs["TITLE"] = np.bytes_(soltab.soltype)

    for name, data in soltab.axes.items():
        if name in STRING_TYPED_AXIS_NAMES:
            data = _ndarray_of_null_terminated_bytes(data)
        group.create_dataset(name, data=data)

    axes_attr = np.bytes_(",".join(soltab.axes.keys()))
    val = group.create_dataset("val", data=soltab.values)
    val.attrs["AXES"] = axes_attr

    weight = group.create_dataset("weight", data=soltab.weights)
    weight.attrs["AXES"] = axes_attr


def prepare_axes_dict(axes: dict[str, ArrayLike]) -> dict[str, NDArray]:
    """
    Convert all dict values to numpy arrays with the appropriate dtype.
    Ensure that the axes arrays containing string values are of unicode type.
    We do this to avoid internally dealing with `np.bytes_` arrays loaded from
    H5Parm files.
    """
    # NOTE: preserving key order is important
    return {
        key: np.asarray(
            arr,
            dtype=np.str_ if key in STRING_TYPED_AXIS_NAMES else np.float64,
        )
        for key, arr in axes.items()
    }


def read_bytes_attribute_as_string(obj: h5py.HLObject, attr_name: str) -> str:
    """
    Used to validate and read either the TITLE attribute of a soltab, or the
    AXES attribute of a val or weight dataset.
    """
    attr: np.bytes_ = obj.attrs.get(attr_name, None)
    assert_or_invalid_h5parm(
        attr, f"Node {obj.name!r} has no attribute {attr_name!r}"
    )
    assert_or_invalid_h5parm(
        isinstance(attr, np.bytes_),
        f"The attribute {attr_name!r} of node {obj.name!r} should be "
        "of type np.bytes_",
    )
    return attr.decode()


def read_title_attribute(obj: h5py.HLObject) -> str:
    """
    Self-explanatory.
    """
    return read_bytes_attribute_as_string(obj, "TITLE")


def read_axes_attribute(dataset: h5py.Dataset) -> tuple[str]:
    """
    Read `AXES` attribute of the `val` or `weight` array into a tuple of
    strings with axis names.
    """
    return tuple(read_bytes_attribute_as_string(dataset, "AXES").split(","))


def read_dataset(node: h5py.Dataset) -> NDArray:
    """
    Read an HDF5 dataset node into a numpy array.
    """
    assert_or_invalid_h5parm(
        isinstance(node, h5py.Dataset),
        f"Node {node.name} is not an h5py.Dataset",
    )
    return node[:]


def read_dataset_with_named_axes(
    soltab: h5py.Group, name: str
) -> tuple[NDArray, tuple[str]]:
    """
    Read 'val' or 'weight' dataset
    """
    node = soltab.get(name, None)
    assert_or_invalid_h5parm(
        node is not None, f"Soltab {soltab.name!r} has no member {name!r}"
    )
    return read_dataset(node), read_axes_attribute(node)


def read_soltab_axes(group: h5py.Group) -> dict[str, NDArray]:
    """
    Read the datasets corresponding to the axes in a soltab group.
    """
    axis_keys = set(group.keys()).difference(VALID_DATASET_NAMES)
    return {key: read_dataset(group[key]) for key in axis_keys}


def _ndarray_of_null_terminated_bytes(strings: Iterable[str]) -> NDArray:
    # NOTE: we have to make the antenna names in the H5Parm one character
    # longer, otherwise DP3 throws an error along the lines of:
    # "SolTab has no element <ANTENNA_NAME> in ant"
    # Adding any character (not just the null terminator) is a valid
    # fix to the problem; I don't know why.
    return np.asarray([s.encode("ascii") + b"\0" for s in strings])
