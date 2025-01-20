import os
from pathlib import Path
from typing import Optional

import h5py

ALLOWED_SOLTAB_TITLES = {"amplitude", "phase"}


class InvalidH5Parm(Exception):
    """
    Raised when the schema/layout of an H5Parm file does not conform to
    expectations.
    """


def _assert(condition: bool, error_message: str):
    if not condition:
        raise InvalidH5Parm(error_message)


def _read_soltab_dimensions(soltab_group: h5py.Group) -> dict[str, int]:
    val = soltab_group["val"]
    axis_names = val.attrs["AXES"].decode().split(",")
    _assert(
        len(axis_names) == val.ndim,
        "Number of dimensions of 'val' dataset does not match the number of "
        "axes in AXES attribute",
    )
    return dict(zip(axis_names, val.shape))


class Soltab:
    """
    Represents one Soltab in an H5Parm file.
    """

    def __init__(self, group: h5py.Group):
        self._name = Path(group.name).name

        _assert(
            "TITLE" in group.attrs,
            f"Soltab {self.name!r} does not have a TITLE attribute",
        )
        self._solution_type = group.attrs["TITLE"].decode()

        _assert(
            self.solution_type in ALLOWED_SOLTAB_TITLES,
            f"Soltab {self.name!r} has invalid TITLE {self.solution_type!r}",
        )

        self._dimensions = _read_soltab_dimensions(group)

    @property
    def name(self) -> str:
        """
        Name of the soltab, e.g. 'amplitude000'.
        """
        return self._name

    @property
    def solution_type(self) -> str:
        """
        Type of solution, e.g. 'phase' or 'amplitude'. This is the content of
        the TITLE attribute of the soltab.
        """
        return self._solution_type

    @property
    def num_pols(self) -> Optional[int]:
        """
        Length of the pol dimension. Returns None if the dataset has no pol
        axis.
        """
        return self._dimensions.get("pol", None)


class H5Parm:
    """
    Reads the layout of an H5Parm with a single solset.
    """

    def __init__(self, file: h5py.File):
        """
        Use from_file() instead.
        """
        solset_names = file.keys()
        _assert(
            len(solset_names) == 1,
            f"H5Parm {file.filename!r} has {len(solset_names)} "
            "solsets; expected 1",
        )

        (solset_name,) = list(solset_names)
        solset = file[solset_name]
        soltab_names = [
            key for key in solset.keys() if key not in ("antenna", "source")
        ]
        _assert(
            len(soltab_names) in (1, 2),
            f"Solset {solset_name!r} of H5Parm {file.filename!r} has "
            f"{len(soltab_names)} soltabs; expected 1 or 2",
        )

        self._soltabs = [Soltab(solset[name]) for name in soltab_names]

        # Forbid single-soltab fulljones
        if len(self.soltabs) == 1:
            soltab = self.soltabs[0]
            _assert(
                soltab.num_pols in (None, 1, 2),
                f"H5Parm {file.filename!r} has a single soltab with "
                f"{soltab.num_pols} pols; a single-soltab H5Parm must have "
                "either no pol axis, 1, or 2 pols.",
            )

        if len(self.soltabs) == 2:
            first, second = self.soltabs
            _assert(
                first.num_pols == second.num_pols,
                f"H5Parm {file.filename!r} has 2 soltabs but they have "
                "different numbers of pols",
            )
            _assert(
                {first.solution_type, second.solution_type}
                == {"amplitude", "phase"},
                f"H5Parm {file.filename!r} has 2 soltabs, but their types are "
                "not 'amplitude' and 'phase' as expected",
            )

    @classmethod
    def from_file(cls, path: str | os.PathLike) -> "H5Parm":
        """
        Initialise from h5parm file.
        """
        with h5py.File(path, "r") as file:
            return cls(file)

    @property
    def soltabs(self) -> list[Soltab]:
        """
        Soltabs present in the H5Parm.
        """
        return list(self._soltabs)

    @property
    def is_fulljones(self) -> bool:
        """
        True if the H5Parm represents a full Jones solution.
        """
        return set(s.solution_type for s in self.soltabs) == {
            "amplitude",
            "phase",
        } and all(s.num_pols == 4 for s in self.soltabs)
