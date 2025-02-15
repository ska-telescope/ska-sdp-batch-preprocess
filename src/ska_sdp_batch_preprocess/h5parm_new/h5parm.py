import copy
import os
from typing import Iterable

import h5py
import numpy as np
from numpy.typing import ArrayLike

from .assertions import InvalidH5Parm, assert_or_invalid_h5parm
from .soltab import (
    WEIGHTS_DTYPE,
    Soltab,
    SoltabAxisName,
    read_soltab_from_hdf5_group,
    write_soltab_to_hdf5_group,
)

RESERVED_SOLSET_KEYS = {"antenna", "source"}


class H5Parm:
    """
    In-memory representation of an H5Parm file with additional restrictions:
    - Only one solset
    - Only one or two soltabs; if there are two soltabs, they must be of type
      "amplitude" and "phase".
    - The "antenna" and "source" datasets
    """

    def __init__(self, soltabs: Iterable[Soltab]):
        """
        Create a new H5Parm instance.

        Args:
            soltabs: Iterable containing Soltab instances.
        """
        self.__soltabs = tuple(soltabs)
        validate_h5parm(self)

    @property
    def soltabs(self) -> tuple[Soltab]:
        """
        Member soltabs, as a tuple.
        """
        return self.__soltabs

    def save(self, path: str | os.PathLike):
        """
        Save to given file path.
        """
        with h5py.File(path, "w") as file:
            # Group names for solsets and soltabs should not matter,
            # but let's follow the LOFAR convention
            solset = file.create_group("sol000")
            for soltab in self.soltabs:
                group = solset.create_group(f"{soltab.soltype}000")
                write_soltab_to_hdf5_group(soltab, group)

    @classmethod
    def load(cls, path: str | os.PathLike) -> "H5Parm":
        """
        Load from an existing h5parm file.
        """
        with h5py.File(path, "r") as file:
            assert_or_invalid_h5parm(
                len(file.keys()) == 1,
                f"H5Parm {file.name!r} should have exactly one top-level "
                "member (solset)",
            )
            solset: h5py.Group = next(iter(file.values()))
            soltab_groups = [
                group
                for key, group in solset.items()
                if key not in RESERVED_SOLSET_KEYS
            ]
            return cls(map(read_soltab_from_hdf5_group, soltab_groups))

    @classmethod
    def from_complex_gain_data(
        cls,
        axes: dict[SoltabAxisName, ArrayLike],
        val: ArrayLike,
        weight: ArrayLike,
    ) -> "H5Parm":
        """
        Convenience method to create an H5Parm with an amplitude and phase
        soltab, given complex-valued gains and associated weights + metadata.
        """
        amp = Soltab(
            soltype="amplitude",
            axes=axes,
            val=np.abs(val),
            weight=weight,
        )
        # Avoid sharing the same underlying numpy arrays between soltabs,
        # otherwise mutating one will silently mutate the other
        phase = Soltab(
            soltype="phase",
            axes=copy.deepcopy(axes),
            val=np.angle(val),
            weight=np.asarray(weight, copy=True, dtype=WEIGHTS_DTYPE),
        )
        return cls([amp, phase])

    def __str__(self) -> str:
        clsname = type(self).__name__
        indent = 4 * " "
        lines = [indent + str(tab) + "," for tab in self.soltabs]
        lines = [f"{clsname}(", *lines, ")"]
        return "\n".join(lines)

    def __repr__(self) -> str:
        return str(self)


def validate_h5parm(parm: H5Parm):
    """
    Self-explanatory.
    """

    def _pols(tab: Soltab) -> tuple[str]:
        return tuple(tab.axes.get("pol", []))

    if len(parm.soltabs) == 1:
        (tab,) = parm.soltabs
        incomplete_full_jones = (
            tab.soltype in {"amplitude", "phase"} and len(_pols(tab)) == 4
        )
        assert_or_invalid_h5parm(
            not incomplete_full_jones,
            f"H5Parm contains only the {tab.soltype} part of a "
            "full Jones solution",
        )

    elif len(parm.soltabs) == 2:
        first, second = parm.soltabs
        assert_or_invalid_h5parm(
            {first.soltype, second.soltype} == {"amplitude", "phase"},
            "The solution types of a 2-Soltab H5Parm must be amplitude and "
            "phase",
        )
        assert_or_invalid_h5parm(
            _pols(first) == _pols(second),
            "Soltabs in H5Parm must have the same pol axis",
        )

    else:
        raise InvalidH5Parm("Number of soltabs must be 1 or 2")
