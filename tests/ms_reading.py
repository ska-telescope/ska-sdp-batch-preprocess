import os
from typing import Any, Iterable

import casacore.tables
from numpy.typing import NDArray


def make_config(h5parm_filenames: Iterable[str]) -> dict[str, Any]:
    """
    Make a pipeline configuration dictionary with one ApplyCal step per given
    h5parm file.
    """
    steps = [{"applycal": {"parmdb": fname}} for fname in h5parm_filenames]
    return {"steps": steps}


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


def load_visibilities_from_msv2(
    path: str | os.PathLike, data_column: str = "DATA"
) -> NDArray:
    """
    Loads visibilites from MSv2, assuming it contains rectangular data.
    Returns complex data with shape (time, baseline, freq, pol).
    """
    vis: NDArray = getcol(path, "MAIN", data_column)
    unique_timestamps = set(getcol(path, "MAIN", "TIME"))
    __, nchan, npol = vis.shape
    return vis.reshape(len(unique_timestamps), -1, nchan, npol)
