import os
from pathlib import Path
from xarray import DataTree
import yaml
import copy

from typing import Any, Type

from casacore.tables import table, maketabdesc, makescacoldesc, makearrcoldesc


"""
Preconditions:
- Only one MSv4 in the processing set
- Fixed phase centre
- Four linear pols
"""


def _load_schema_file() -> dict:
    path = Path(__file__).with_name("schema.yml")
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
    write_main_table_template(msv4, output_path)
    write_antenna_table(msv4, output_path)


def make_column_description(
    table_name: str, column_name: str, shape_dict: dict[str, int]
) -> dict[str, Any]:
    """
    TODO
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


def write_main_table_template(msv4: DataTree, output_path: str | os.PathLike):
    """
    TODO

    msv4: Root node of one MSv4
    output_path: Path to base directory of the output MS
    """
    shape_dict = {
        "nchan": msv4["frequency"].size,
        "npol": 4,
    }

    column_descriptors = [
        make_column_description("MAIN", column_name, shape_dict=shape_dict)
        for column_name in SCHEMA["MAIN"]
    ]

    tdesc = maketabdesc(column_descriptors)
    ms_table = table(output_path, tdesc, nrow=0)
    ms_table.close()


def write_antenna_table(msv4: DataTree, output_path: str | os.PathLike):
    """
    TODO

    msv4: Root node of one MSv4
    output_path: Path to base directory of the output MS
    """
