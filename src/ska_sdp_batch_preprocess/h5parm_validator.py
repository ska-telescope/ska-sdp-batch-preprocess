"""
Solset
    Soltab
        TITLE 
        axes (time, freq, ant, pol, dir)
        val
            AXES
        weight
            AXES


Dataset
    shape
    axes

"""

import os
from dataclasses import dataclass

import h5py
import numpy as np

VALID_DATASET_NAMES = {"val", "weight"}
VALID_AXIS_NAMES = {"time", "freq", "ant", "pol", "dir"}
RESERVED_SOLSET_TOP_LEVEL_KEYS = {"antenna", "source"}


@dataclass
class Dataset:
    name: str
    shape: tuple[int]
    axes: tuple[str]


@dataclass
class Axis:
    name: str
    length: int


@dataclass
class Soltab:
    name: str
    title: str
    axes: dict[str, Axis]
    val: Dataset
    weight: Dataset


    @classmethod
    def _from_h5py_group(cls, group: h5py.Group) -> "Soltab":
        _, name = os.path.split(group.name)
        title = read_soltab_title(group)
        axes = read_all_axes(group)
        val = read_dataset(group, "val")
        weight = read_dataset(group, "weight")
        return Soltab(name, title, axes, val, weight)


def read_soltab_title(group: h5py.Group) -> str:
    """
    Validate and read TITLE attribute as a string.
    """
    title: np.bytes_ = group.attrs.get("TITLE", None)
    _assert(title, f"Soltab {group.name} has no TITLE attribute")
    _assert(
        isinstance(title, np.bytes_),
        f"Soltab {group.name} has a TITLE that isn't of type np.bytes_",
    )
    return title.decode()


def read_all_axes(group: h5py.Group) -> dict[str, Axis]:
    """
    Validate and read all soltab axes.
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
        axes[key] = Axis(key, member.size)
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
    return Dataset(member.name, member.shape, axis_names)


def read_dataset_axis_names(ds: h5py.Dataset) -> tuple[str]:
    """
    Read the axis names of dataset "val" or "weight" from their AXES attribute.
    """
    axes: np.bytes_ = ds.attrs.get("AXES", None)
    _assert(axes, f"Dataset {ds.name} has no AXES attribute")
    _assert(
        isinstance(axes, np.bytes_),
        f"Dataset {ds.name} has an AXES attribute that isn't of type "
        "np.bytes_",
    )
    axis_names = tuple(axes.decode().split(","))
    _assert(
        set(axis_names).issubset(VALID_AXIS_NAMES),
        f"Dataset {ds.name} has an AXES attribute that contains invalid axis "
        f"names: {axes!r}",
    )
    return axis_names


@dataclass
class H5Parm:
    """
    Class that reads and validates the schema of a single-solset H5Parm.
    """

    soltabs: tuple[Soltab]

    def __post_init__(self):
        pass

    @classmethod
    def from_file(cls, path: str | os.PathLike) -> "H5Parm":
        with h5py.File(path, "r") as file:
            return cls._from_h5py_file(file)

    @classmethod
    def _from_h5py_file(cls, file: h5py.File) -> "H5Parm":
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

        soltab_items: dict[str, h5py.Group] = {
            key: group
            for key, group in solset.items()
            if key not in RESERVED_SOLSET_TOP_LEVEL_KEYS
        }
        _assert(soltab_items, f"Solset {solset.name!r} contains no soltabs")

        soltabs = tuple(map(Soltab._from_h5py_group, soltab_items.values()))
        return cls(soltabs)

    @property
    def is_fulljones(self) -> bool:
        pass


class InvalidH5Parm(Exception):
    """
    Raised when the schema/layout of an H5Parm file does not conform to
    expectations.
    """


def _assert(condition: bool, error_message: str):
    if not condition:
        raise InvalidH5Parm(error_message)
