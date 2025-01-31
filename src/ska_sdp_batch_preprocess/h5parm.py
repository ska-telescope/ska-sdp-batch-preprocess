import os
from dataclasses import dataclass
from typing import Optional

import h5py
import numpy as np

RESERVED_SOLSET_TOP_LEVEL_KEYS = {"antenna", "source"}
VALID_SOLTAB_TITLES = {"amplitude", "phase"}
VALID_DATASET_NAMES = {"val", "weight"}
VALID_AXIS_NAMES = {"time", "freq", "ant", "pol", "dir"}
VALID_POL_AXIS_LENGTHS = {1, 2, 4}


class InvalidH5Parm(Exception):
    """
    Raised when the schema/layout of an H5Parm file does not conform to
    expectations.
    """


def _assert(condition: bool, error_message: str):
    if not condition:
        raise InvalidH5Parm(error_message)


@dataclass
class Dataset:
    """
    Holds the attributes of either the "val" or "weight" dataset in a Soltab.
    """

    shape: tuple[int]
    axis_names: tuple[str]


@dataclass
class Soltab:
    """
    Holds the attributes of a soltab.
    """

    name: str
    title: str
    dimensions: dict[str, int]
    val: Dataset
    weight: Dataset

    @property
    def num_pols(self) -> Optional[int]:
        """
        Length of the pol dimension. Returns None if the dataset has no pol
        axis.
        """
        return self.dimensions.get("pol", None)


@dataclass
class H5Parm:
    """
    Class that reads and validates the schema of a single-solset H5Parm.
    """

    soltabs: tuple[Soltab]

    @classmethod
    def from_file(cls, path: str | os.PathLike) -> "H5Parm":
        """
        Initialise from h5parm file.
        """
        with h5py.File(path, "r") as file:
            soltabs = read_soltabs_of_single_solset_h5parm(file)
            return cls(soltabs)

    @property
    def is_fulljones(self) -> bool:
        """
        True if the H5Parm represents a full Jones solution.
        """
        titles = set(tab.title for tab in self.soltabs)
        num_pols = tuple(tab.num_pols for tab in self.soltabs)
        return titles == {"amplitude", "phase"} and num_pols == (4, 4)


def read_soltab_from_h5py_group(group: h5py.Group) -> Soltab:
    """
    Self-explanatory.
    """
    _, name = os.path.split(group.name)
    title = read_soltab_title(group)
    dimensions = read_dimensions(group)
    val = read_dataset(group, "val")
    weight = read_dataset(group, "weight")

    _assert(
        val.shape == weight.shape,
        f"The val and weight datasets of Soltab {group.name!r} have "
        "different shapes",
    )
    _assert(
        val.axis_names == weight.axis_names,
        f"The val and weight datasets of Soltab {group.name!r} have "
        "different axes",
    )

    metadata_shape = tuple(dimensions[key] for key in val.axis_names)
    _assert(
        val.shape == metadata_shape,
        f"Soltab {group.name!r} has val and weight datasets of shape "
        f"{val.shape!r} with axes {val.axis_names!r}; this is "
        f"inconsistent with the length of the axes which specify a shape "
        f"of {metadata_shape!r}",
    )

    pol_dim = dimensions.get("pol", 1)
    _assert(
        pol_dim in VALID_POL_AXIS_LENGTHS,
        f"pol dimension length is {pol_dim} but should be one of "
        f"{VALID_POL_AXIS_LENGTHS}",
    )
    return Soltab(name, title, dimensions, val, weight)


def read_bytes_attribute_as_string(obj: h5py.HLObject, attr_name: str) -> str:
    """
    Used to validate and read either the TITLE attribute of a soltab, or the
    AXES attribute of a val or weight dataset.
    """
    attr: np.bytes_ = obj.attrs.get(attr_name, None)
    _assert(attr, f"Group/Dataset {obj.name} has no {attr_name} attribute")
    _assert(
        isinstance(attr, np.bytes_),
        f"The attribute {attr_name} of Group/Dataset {obj.name} should be"
        "of type np.bytes_",
    )
    return attr.decode()


def read_soltab_title(group: h5py.Group) -> str:
    """
    Validate and read a soltab's TITLE attribute.
    """
    title = read_bytes_attribute_as_string(group, "TITLE")
    _assert(
        title in VALID_SOLTAB_TITLES,
        f"Soltab {group.name!r} has invalid TITLE {title!r}",
    )
    return title


def read_dimensions(group: h5py.Group) -> dict[str, int]:
    """
    Validate and read all soltab axes, return the dimensions specified by the
    axes as a dictionary {axis_name: length of axis}.
    """
    axis_keys = set(group.keys()).difference(VALID_DATASET_NAMES)
    _assert(
        axis_keys.issubset(VALID_AXIS_NAMES),
        f"Soltab {group.name!r} has the following axis names, "
        f"some of which are invalid: {axis_keys!r}",
    )

    axes = {}
    for key in axis_keys:
        member: h5py.Dataset = group[key]
        _assert(
            isinstance(member, h5py.Dataset),
            f"Axis {key!r} in soltab {group.name} is not an HDF5 Dataset",
        )
        _assert(
            member.ndim == 1,
            f"Axis {key!r} in soltab {group.name} should have 1 dimension",
        )
        axes[key] = member.size
    return axes


def read_dataset(group: h5py.Group, key: str) -> Dataset:
    """
    Validate and read the "val" or "weight" Dataset.
    """
    _assert(
        key in group.keys(), f"Soltab {group.name!r} has no {key!r} member"
    )

    member: h5py.Dataset = group[key]
    _assert(
        isinstance(member, h5py.Dataset),
        f"{key!r} in soltab {group.name!r} is not an HDF5 Dataset",
    )

    axis_names = read_dataset_axis_names(member)
    _assert(
        len(axis_names) == member.ndim,
        f"{key!r} dataset in soltab {group.name!r} has {member.ndim} "
        f"dimensions, but its AXES attribute specifies {len(axis_names)} "
        "dimensions.",
    )
    return Dataset(member.shape, axis_names)


def read_dataset_axis_names(dataset: h5py.Dataset) -> tuple[str]:
    """
    Read the axis names of dataset "val" or "weight" from their AXES attribute.
    """
    axes = read_bytes_attribute_as_string(dataset, "AXES")
    axis_names = tuple(axes.split(","))
    _assert(
        set(axis_names).issubset(VALID_AXIS_NAMES),
        f"Dataset {dataset.name} has an AXES attribute that contains invalid "
        f"axis names: {axis_names!r}",
    )
    return axis_names


def read_soltabs_of_single_solset_h5parm(file: h5py.File) -> tuple[Soltab]:
    """
    Validate and read a single-solset H5Parm from an open h5py.File object.
    """
    keys = file.keys()
    _assert(
        len(keys) == 1,
        f"H5Parm file has multiple top-level keys (solsets): {keys!r}",
    )

    solset: h5py.Group = next(iter(file.values()))
    _assert(
        isinstance(solset, h5py.Group),
        "H5Parm top-level member objects must be HDF5 groups",
    )

    soltab_groups = [
        member
        for key, member in solset.items()
        if (key not in RESERVED_SOLSET_TOP_LEVEL_KEYS)
        and isinstance(member, h5py.Group)
    ]
    soltabs = tuple(map(read_soltab_from_h5py_group, soltab_groups))

    num_tabs = len(soltabs)
    _assert(num_tabs in (1, 2), f"H5Parm has {num_tabs}, expected 1 or 2")

    # Forbid single-soltab fulljones
    if num_tabs == 1:
        tab = soltabs[0]
        _assert(
            tab.num_pols in (None, 1, 2),
            f"H5Parm contains a single soltab with an invalid number of pols "
            f"({tab.num_pols}); it must have either no pol axis, 1, or 2 pols",
        )

    if num_tabs == 2:
        first, second = soltabs
        _assert(
            {first.title, second.title} == {"amplitude", "phase"},
            "H5Parm has 2 soltabs, but their titles are not 'amplitude' and "
            "'phase' as expected",
        )
        _assert(
            first.num_pols == second.num_pols,
            "H5Parm has 2 soltabs but they have different numbers of pols",
        )
    return soltabs
