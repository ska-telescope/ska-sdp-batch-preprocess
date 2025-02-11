import os
from typing import Iterable

import h5py
import numpy as np
from numpy.typing import NDArray

VALID_AXIS_NAMES = {"time", "freq", "ant", "pol", "dir"}
VALID_DATASET_NAMES = {"val", "weight"}
VALID_SOLTAB_TITLES = {"amplitude", "phase"}
STRING_TYPED_AXIS_NAMES = {"ant", "pol", "dir"}


class InvalidH5Parm(Exception):
    """
    Raised when the schema/layout of an H5Parm file does not conform to
    expectations.
    """


def _assert(condition: bool, error_message: str):
    if not condition:
        raise InvalidH5Parm(error_message)


class Soltab:
    """
    TODO.
    """

    def __init__(
        self,
        title: str,
        axes: dict[str, NDArray],
        values: NDArray,
        weights: NDArray,
    ):
        # NOTE: we could add lazy loading of values and weights later on,
        # by allowing to pass a Callable instead of NDArray for 'values'
        # and 'weights'
        self.__title = title
        self.__axes = dict(axes)
        self.__values = values
        self.__weights = weights
        self.__convert_string_typed_axes_to_str_type()
        self.__validate()

    def __convert_string_typed_axes_to_str_type(self):
        newaxes = {}
        # NOTE: preserving key order is important
        for key, arr in self.__axes.items():
            if key in STRING_TYPED_AXIS_NAMES:
                arr = np.asarray(arr, dtype=np.str_)
            newaxes[key] = arr
        self.__axes = newaxes

    def __validate(self):
        # TODO: check consistency between axes and datasets
        pass

    @property
    def title(self) -> str:
        return self.__title

    @property
    def axes(self) -> dict[str, NDArray]:
        # Ensure the axes member dict can't be mutated; we allow edition of
        # the data though.
        return dict(self.__axes)

    @property
    def axis_order(self) -> tuple[str]:
        return tuple(self.__axes.keys())

    @property
    def values(self) -> NDArray:
        return self.__values

    @property
    def weights(self) -> NDArray:
        return self.__weights

    @property
    def dimensions(self) -> dict[str, int]:
        return {key: len(arr) for key, arr in self.axes.items()}

    @classmethod
    def from_hdf5_group(cls, group: h5py.Group) -> "Soltab":
        """
        Load from HDF5 group.
        """
        title = read_title_attribute(group)
        axes = read_soltab_axes(group)
        values, values_axes = read_dataset_with_named_axes(group, "val")
        weights, weight_axes = read_dataset_with_named_axes(group, "weight")
        _assert(
            values_axes == weight_axes,
            f"val and weight datasets under soltab {group.name!r} "
            "have metadata specifying different axes",
        )
        # Reorder "axes" dict to match order specified by datasets
        # Checking consistency between axis order in "axes" dict and the
        # dimensions of the datasets is left to __init__
        axes = {key: axes[key] for key in values_axes}
        return cls(title, axes, values, weights)

    def to_hdf5_group(self, group: h5py.Group) -> "Soltab":
        """
        Write to HDF5 group; any pre-existing contents of the group are
        deleted.
        """
        group.clear()
        group.attrs["TITLE"] = np.bytes_(self.title)

        for name, data in self.axes.items():
            if name in STRING_TYPED_AXIS_NAMES:
                data = _ndarray_of_null_terminated_bytes(data)
            group.create_dataset(name, data=data)

        axes_attr = np.bytes_(",".join(self.axes.keys()))
        val = group.create_dataset("val", data=self.values)
        val.attrs["AXES"] = axes_attr

        weight = group.create_dataset("weight", data=self.weights)
        weight.attrs["AXES"] = axes_attr

    def __str__(self) -> str:
        clsname = type(self).__name__
        return (
            f"{clsname}(title={self.title!r}, dimensions={self.dimensions!r})"
        )

    def __repr__(self) -> str:
        return str(self)


def read_bytes_attribute_as_string(obj: h5py.HLObject, attr_name: str) -> str:
    """
    Used to validate and read either the TITLE attribute of a soltab, or the
    AXES attribute of a val or weight dataset.
    """
    attr: np.bytes_ = obj.attrs.get(attr_name, None)
    _assert(attr, f"Node {obj.name!r} has no attribute {attr_name!r}")
    _assert(
        isinstance(attr, np.bytes_),
        f"The attribute {attr_name!r} of node {obj.name!r} should be "
        "of type np.bytes_",
    )
    return attr.decode()


def read_title_attribute(obj: h5py.HLObject) -> str:
    title = read_bytes_attribute_as_string(obj, "TITLE")
    _assert(title in VALID_SOLTAB_TITLES, f"Invalid soltab title: {title!r}")
    return title


def read_axes_attribute(dataset: h5py.Dataset) -> tuple[str]:
    axes = tuple(read_bytes_attribute_as_string(dataset, "AXES").split(","))
    _assert(
        set(axes).issubset(VALID_AXIS_NAMES),
        f"Dataset {dataset.name!r} has the following axes, "
        f"some of which are invalid: {axes!r}",
    )
    return axes


def read_dataset(node: h5py.Dataset) -> NDArray:
    """
    Read an HDF5 dataset node into a numpy array.
    """
    _assert(
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
    _assert(node is not None, f"Soltab {soltab.name!r} has no member {name!r}")
    return read_dataset(node), read_axes_attribute(node)


def read_soltab_axes(group: h5py.Group) -> dict[str, NDArray]:
    """
    Read the datasets corresponding to the axes in a soltab group.
    """
    axis_keys = set(group.keys()).difference(VALID_DATASET_NAMES)
    _assert(
        axis_keys.issubset(VALID_AXIS_NAMES),
        f"Soltab {group.name!r} has the following axis names, "
        f"some of which are invalid: {axis_keys!r}",
    )
    return {key: read_dataset(group[key]) for key in axis_keys}


def _ndarray_of_null_terminated_bytes(strings: Iterable[str]) -> NDArray:
    # NOTE: we have to make the antenna names in the H5Parm one character
    # longer, otherwise DP3 throws an error along the lines of:
    # "SolTab has no element <ANTENNA_NAME> in ant"
    # Adding any character (not just the null terminator) is a valid
    # fix to the problem; I don't know why.
    return np.asarray([s.encode("ascii") + b"\0" for s in strings])


class H5Parm:
    def __init__(self, soltabs: Iterable[Soltab]):
        self.__soltabs = tuple(soltabs)
        self.__validate()

    def __validate(self):
        # NOTE: soltabs are assumed to be validated already
        # Refuse nsoltabs not in (1, 2)
        # if nsoltabs == 1, it can't be phase or amplitude with 4 pols
        # if nsoltabs == 2, check they are phase and amplitude, and consistent
        # with each other
        pass

    @property
    def soltabs(self) -> tuple[Soltab]:
        return self.__soltabs

    def save(self, path: str | os.PathLike):
        pass

    @classmethod
    def load(cls, path: str | os.PathLike) -> "H5Parm":
        with h5py.File(path, "r") as file:
            _assert(
                len(file.keys()) == 1,
                f"H5Parm {file.name!r} should have exactly one top-level "
                "member (solset)",
            )
            solset: h5py.Group = next(iter(file.values()))
            soltabs = list(map(Soltab.from_hdf5_group, solset.values()))
        return cls(soltabs)

    @classmethod
    def from_complex_gain_data(
        cls, axes: dict[str, NDArray], values: NDArray, weights: NDArray
    ) -> "H5Parm":
        """
        Convenience method to create an H5Parm with an amplitude and phase
        soltab, given complex-valued gains and associated metadata.
        """
        pass


if __name__ == "__main__":
    fname = "/home/vince/work/bpp/solutions/bandpass-e2e-feb7.h5parm"

    with h5py.File(fname, "r") as file:
        soltab = Soltab.from_hdf5_group(file["sol000"]["amplitude000"])

    print(soltab.title)
    print(soltab.axes)
    print(soltab.values.shape)
    print(soltab.weights.shape)

    print(80 * "=")

    fname = "/home/vince/work/bpp/solutions/test.h5parm"
    with h5py.File(fname, "w") as file:
        group = file.create_group("test")
        soltab.to_hdf5_group(group)

    with h5py.File(fname, "r") as file:
        soltab.from_hdf5_group(file["test"])

    print(soltab.title)
    print(soltab.axes)
    print(soltab.values.shape)
    print(soltab.weights.shape)

    print(80 * "=")
    fname = "/home/vince/work/bpp/solutions/bandpass-e2e-feb7.h5parm"
    parm = H5Parm.load(fname)
    print(parm.soltabs)
