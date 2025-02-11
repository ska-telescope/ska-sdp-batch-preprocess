import os
from typing import Iterable

import h5py
from numpy.typing import NDArray

from .exceptions import _assert
from .soltab import Soltab


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
        with h5py.File(path, "w") as file:
            # NOTE: group names for solsets and soltabs should not matter,
            # but let's follow the LOFAR convention
            solset = file.create_group("sol000")
            for soltab in self.soltabs:
                group = solset.create_group(f"{soltab.title}000")
                soltab.to_hdf5_group(group)

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
