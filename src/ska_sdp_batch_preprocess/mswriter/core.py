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
    path = Path(__file__).with_name("newschema.yml")
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
    ordered_names = ["MAIN"] + sorted(set(SCHEMA.keys()) - {"MAIN"})

    for table_name in ordered_names:
        write_empty_table_template(msv4, table_name, output_path)

    add_version_keyword(output_path)
    add_subtable_keywords(output_path)


def rascil_write_msv2_template_matching_xradio_processing_set(
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
    rascil_write_msv2_template_matching_xradio_msv4(msv4, output_path)


def rascil_write_msv2_template_matching_xradio_msv4(
    msv4: DataTree, output_path: str | os.PathLike
):
    rascil_write_main_table(msv4, output_path)
    add_version_keyword(output_path)


def add_version_keyword(output_path: Path):
    tbl = table(output_path, readonly=False)
    tbl.putkeyword("MS_VERSION", 2.0)
    tbl.close()


def add_subtable_keywords(output_path: str | os.PathLike):
    tbl = table(output_path, readonly=False)
    subtable_names = set(SCHEMA.keys()) - {"MAIN"}
    for subtable_name in subtable_names:
        subtable_path_str = str(Path(output_path).absolute() / subtable_name)
        tbl.putkeyword(subtable_name, f"Table: {subtable_path_str}")
    tbl.close()


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

    # NOTE: MUST PASS THE CORRECT "valuetype"
    if kind == "scalar":
        return makescacoldesc(
            column_name,
            python_type(),
            datamanagertype=schema["dataManagerType"],
            datamanagergroup=schema["dataManagerGroup"],
            options=schema["options"],
            comment=schema["comment"],
            valuetype=schema["valuetype"],
            keywords=schema.get("keywords", {}),
        )
    elif kind == "array":
        return makearrcoldesc(
            column_name,
            python_type(),
            shape=make_shape(schema["shape"]),
            datamanagertype=schema["dataManagerType"],
            datamanagergroup=schema["dataManagerGroup"],
            options=schema["options"],
            comment=schema["comment"],
            valuetype=schema["valuetype"],
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


def rascil_write_main_table(msv4: DataTree, output_path: str | os.PathLike):
    nchan = msv4["frequency"].size
    ncorr = 4
    col1 = makearrcoldesc(
        "UVW",
        0.0,
        1,
        shape=[3],
        options=0,
        comment="Vector with uvw coordinates (in meters)",
        datamanagergroup="UVW",
        datamanagertype="TiledColumnStMan",
        keywords={
            "QuantumUnits": ["m", "m", "m"],
            "MEASINFO": {"type": "uvw", "Ref": "ITRF"},
        },
    )
    col2 = makearrcoldesc(
        "FLAG",
        False,
        2,
        shape=[nchan, ncorr],
        options=4,
        datamanagertype="TiledColumnStMan",
        datamanagergroup="Data",
        comment="The data flags, array of bools with same" + " shape as data",
    )
    col3 = makearrcoldesc(
        "FLAG_CATEGORY",
        False,
        3,
        shape=[1, nchan, ncorr],
        options=4,
        datamanagertype="TiledColumnStMan",
        datamanagergroup="FlagCategory",
        comment="The flag category, NUM_CAT flags for each datum",
        keywords={
            "CATEGORY": [
                "",
            ]
        },
    )
    col4 = makearrcoldesc(
        "WEIGHT",
        1.0,
        1,
        shape=[ncorr],
        datamanagertype="TiledColumnStMan",
        datamanagergroup="Weight",
        valuetype="float",
        comment="Weight for each polarization spectrum",
    )
    col_4_5 = makearrcoldesc(
        "WEIGHT_SPECTRUM",
        1.0,
        2,
        shape=[nchan, ncorr],
        options=4,
        datamanagertype="TiledColumnStMan",
        datamanagergroup="WeightSpectrum",
        valuetype="float",
        comment="Weight for each polarization and channel spectrum",
    )
    col5 = makearrcoldesc(
        "SIGMA",
        9999.0,
        1,
        shape=[ncorr],
        options=4,
        datamanagertype="TiledColumnStMan",
        datamanagergroup="Sigma",
        valuetype="float",
        comment="Estimated rms noise for channel with " + "unity bandpass response",
    )
    col6 = makescacoldesc(
        "ANTENNA1", 0, comment="ID of first antenna in interferometer"
    )
    col7 = makescacoldesc(
        "ANTENNA2", 0, comment="ID of second antenna in interferometer"
    )
    col8 = makescacoldesc(
        "ARRAY_ID",
        0,
        datamanagertype="IncrementalStMan",
        comment="ID of array or subarray",
    )
    col9 = makescacoldesc(
        "DATA_DESC_ID",
        0,
        datamanagertype="IncrementalStMan",
        comment="The data description table index",
    )
    col10 = makescacoldesc(
        "EXPOSURE",
        0.0,
        comment="The effective integration time",
        keywords={
            "QuantumUnits": [
                "s",
            ]
        },
    )
    col11 = makescacoldesc(
        "FEED1",
        0,
        datamanagertype="IncrementalStMan",
        comment="The feed index for ANTENNA1",
    )
    col12 = makescacoldesc(
        "FEED2",
        0,
        datamanagertype="IncrementalStMan",
        comment="The feed index for ANTENNA2",
    )
    col13 = makescacoldesc(
        "FIELD_ID",
        0,
        datamanagertype="IncrementalStMan",
        comment="Unique id for this pointing",
    )
    col14 = makescacoldesc(
        "FLAG_ROW",
        False,
        datamanagertype="IncrementalStMan",
        comment="Row flag - flag all data in this row if True",
    )
    col15 = makescacoldesc(
        "INTERVAL",
        0.0,
        datamanagertype="IncrementalStMan",
        comment="The sampling interval",
        keywords={
            "QuantumUnits": [
                "s",
            ]
        },
    )
    col16 = makescacoldesc(
        "OBSERVATION_ID",
        0,
        datamanagertype="IncrementalStMan",
        comment="ID for this observation, index in OBSERVATION table",
    )
    col17 = makescacoldesc(
        "PROCESSOR_ID",
        -1,
        datamanagertype="IncrementalStMan",
        comment="Id for backend processor, index in PROCESSOR table",
    )
    col18 = makescacoldesc(
        "SCAN_NUMBER",
        1,
        datamanagertype="IncrementalStMan",
        comment="Sequential scan number from on-line system",
    )
    col19 = makescacoldesc(
        "STATE_ID",
        -1,
        datamanagertype="IncrementalStMan",
        comment="ID for this observing state",
    )
    col20 = makescacoldesc(
        "TIME",
        0.0,
        comment="Modified Julian Day",
        datamanagertype="IncrementalStMan",
        keywords={
            "QuantumUnits": [
                "s",
            ],
            "MEASINFO": {"type": "epoch", "Ref": "UTC"},
        },
    )
    col21 = makescacoldesc(
        "TIME_CENTROID",
        0.0,
        comment="Modified Julian Day",
        datamanagertype="IncrementalStMan",
        keywords={
            "QuantumUnits": [
                "s",
            ],
            "MEASINFO": {"type": "epoch", "Ref": "UTC"},
        },
    )
    col22 = makearrcoldesc(
        "DATA",
        0j,
        2,
        shape=[nchan, ncorr],
        options=4,
        valuetype="complex",
        keywords={"UNIT": "Jy"},
        datamanagertype="TiledColumnStMan",
        datamanagergroup="Data",
        comment="The data column",
    )

    desc = maketabdesc(
        [
            col1,
            col2,
            col3,
            col4,
            col_4_5,
            col5,
            col6,
            col7,
            col8,
            col9,
            col10,
            col11,
            col12,
            col13,
            col14,
            col15,
            col16,
            col17,
            col18,
            col19,
            col20,
            col21,
            col22,
        ]
    )
    tb = table(str(output_path), desc, nrow=0, ack=False)
    tb.close()
