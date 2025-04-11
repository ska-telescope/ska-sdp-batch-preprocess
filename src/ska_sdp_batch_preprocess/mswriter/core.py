import os
from pathlib import Path
from xarray import DataTree
import yaml
import numpy as np

from typing import Any, Type

from casacore.tables import table, maketabdesc, makescacoldesc, makearrcoldesc

"""
Preconditions:
- Only one MSv4 in the processing set
- Fixed phase centre
- Four linear pols
"""


def _load_schema_file() -> dict:
    path = Path(__file__).with_name("schema_generated.yml")
    return yaml.safe_load(path.read_text())


SCHEMA: dict[str, dict] = _load_schema_file()


def write_msv2_template_matching_xradio_processing_set(
    root: DataTree, output_path: str | os.PathLike
):
    """
    TODO

    root: Root node of the xradio processing set
    """
    # TODO: Figure out what to do if there are data groups, e.g.
    # CORRECTED_DATA and the like
    if not len(root.children) == 1:
        raise ValueError("Processing set contains more than one MSv4")

    (msv4,) = root.children.values()
    write_msv2_template_matching_xradio_msv4(msv4, output_path)


def write_msv2_template_matching_xradio_msv4(
    msv4: DataTree, output_path: str | os.PathLike
):
    """
    TODO

    msv4: Root node of one MSv4
    """
    # NOTE: MAIN table must be written first, otherwise casacore complains
    ordered_names = ["MAIN"] + sorted(set(SCHEMA.keys()) - {"MAIN"})

    for table_name in ordered_names:
        write_empty_table_template(msv4, table_name, output_path)

    add_subtable_and_version_keywords(output_path)

    # Fix data type of TabRefCodes
    # TODO: it's most likely NOT necessary
    path = Path(output_path) / "SPECTRAL_WINDOW"
    tbl = table(str(path), readonly=False)

    kw = tbl.getcolkeyword("CHAN_FREQ", "MEASINFO")
    kw["TabRefCodes"] = np.asarray(kw["TabRefCodes"], dtype="uint32")
    tbl.putcolkeyword("CHAN_FREQ", "MEASINFO", kw)

    kw = tbl.getcolkeyword("REF_FREQUENCY", "MEASINFO")
    kw["TabRefCodes"] = np.asarray(kw["TabRefCodes"], dtype="uint32")
    tbl.putcolkeyword("REF_FREQUENCY", "MEASINFO", kw)

    tbl.close()


def add_subtable_and_version_keywords(output_path: str | os.PathLike):
    # Add keyword entries in MAIN table that specify the presence of sub-tables
    # Maybe I should use `default_ms_subtable()`
    tbl = table(output_path, readonly=False)
    tbl.putkeyword("MS_VERSION", 2.0)

    subtable_names = set(SCHEMA.keys()) - {"MAIN"}
    for subtable_name in subtable_names:
        subtable_path_str = str(Path(output_path).absolute() / subtable_name)
        tbl.putkeyword(subtable_name, f"Table: {subtable_path_str}")


def make_column_description(
    table_name: str, column_name: str, shape_dict: dict[str, int]
) -> dict[str, Any]:
    """
    Make a column description dictionary with default keywords. Make sure to
    edit keywords as necessary.
    """
    schema = SCHEMA[table_name][column_name]
    kind = schema["kind"]
    vtype: Type = eval(schema["type"])

    def make_shape(shape_spec: list[str | int]) -> tuple[int]:
        return tuple(x if isinstance(x, int) else shape_dict[x] for x in shape_spec)

    def tiled_data_manager_group_name(colname: str) -> str:
        return "Tiled" + "".join(map(str.capitalize, colname.split("_")))

    if kind == "scalar":
        return makescacoldesc(
            column_name,
            vtype(),
            datamanagergroup=column_name,
            keywords=schema.get("keywords", {}),
        )
    elif kind == "array":
        return makearrcoldesc(
            column_name,
            vtype(),
            shape=make_shape(schema["shape"]),
            datamanagertype="TiledColumnStMan",
            datamanagergroup=tiled_data_manager_group_name(column_name),
            keywords=schema.get("keywords", {}),
        )
    else:
        raise ValueError(f"Invalid column kind {kind!r}")


def write_empty_table_template(
    msv4: DataTree, table_name: str, output_path: str | os.PathLike
):
    """
    TODO

    msv4: Root node of one MSv4
    table_name: Name of the MSv2 table, e.g. "MAIN" or "ANTENNA"
    output_path: Path to base directory of the output MS
    """
    print(f"Writing table: {table_name}")

    shape_dict = {
        "nchan": msv4["frequency"].size,
        "npol": 4,
    }

    column_descriptors_by_name = {
        column_name: make_column_description(table_name, column_name, shape_dict)
        for column_name in SCHEMA[table_name]
    }

    tdesc = maketabdesc(column_descriptors_by_name.values())
    output_path = Path(output_path).absolute()
    table_path = output_path if table_name == "MAIN" else output_path / table_name
    ms_table = table(str(table_path), tdesc, nrow=0)
    ms_table.close()
