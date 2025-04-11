import os
from pathlib import Path
from xarray import DataTree
import yaml

from typing import Any, Type

from casacore.tables import table, maketabdesc, makescacoldesc, makearrcoldesc


def _load_schema_file() -> dict:
    path = Path(__file__).with_name("schema.yml")
    return yaml.safe_load(path.read_text())


SCHEMA: dict[str, dict] = _load_schema_file()

TYPE_MAPPING = {
    "boolean": bool,
    "double": float,
    "float": float,
    "int": int,
    "string": str,
    "complex": complex,
}


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
    subtable_names = sorted(set(SCHEMA.keys()) - {"MAIN"})
    ordered_names = ["MAIN"] + subtable_names

    # TODO: We are missing the PHASED_ARRAY table
    # TODO: Some tables, like PHASED_ARRAY, should only be written under
    # certain conditions, we are currently writing everything in the schema
    for table_name in ordered_names:
        write_empty_table_template(msv4, table_name, output_path)

    with table(output_path, readonly=False) as tbl:
        tbl.putkeyword("MS_VERSION", 2.0)
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
    python_type: Type = TYPE_MAPPING[schema["valuetype"]]

    def make_shape(shape_spec: list[str | int]) -> tuple[int]:
        return tuple(x if isinstance(x, int) else shape_dict[x] for x in shape_spec)

    coldesc_arguments = dict(
        columnname=column_name,
        value=python_type(),
        datamanagertype=schema["dataManagerType"],
        datamanagergroup=schema["dataManagerGroup"],
        options=schema.get("options", 0),
        comment=schema["comment"],
        valuetype=schema["valuetype"],
        keywords=schema.get("keywords", {}),
    )

    if kind == "scalar":
        return makescacoldesc(**coldesc_arguments)
    elif kind == "array":
        shape = make_shape(schema["shape"])
        kwargs = coldesc_arguments | dict(shape=shape)
        return makearrcoldesc(**kwargs)
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
    table(str(table_path), tdesc, nrow=0).close()
