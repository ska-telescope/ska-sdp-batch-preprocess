import os

import casacore.tables
from numpy.typing import NDArray


def getcol(path: str | os.PathLike, table_name: str, col_name: str):
    """
    Returns an entire column for a MeasurementSet.
    """
    if table_name in {"", "MAIN"}:
        spec = str(path)
    else:
        spec = f"{str(path)}::{table_name}"
    with casacore.tables.table(spec, ack=False) as tbl:
        return tbl.getcol(col_name)


def load_msv2_visibilities(
    path: str | os.PathLike, data_column: str = "DATA"
) -> NDArray:
    """
    Loads visibilites from MSv2, assuming it contains rectangular data.
    Returns complex data with shape (nrows, nchan, npol).
    """
    return getcol(path, "MAIN", data_column)


def load_msv2_flags(path: str | os.PathLike) -> NDArray:
    """
    Load the FLAG column from an MSv2.
    """
    return getcol(path, "MAIN", "FLAG")


def load_msv2_antenna_names(path: str | os.PathLike) -> list[str]:
    """
    Load the list of antenna names from an MSv2.
    """
    return getcol(path, "ANTENNA", "NAME")
