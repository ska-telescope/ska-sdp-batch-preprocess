import os
from pathlib import Path
from xarray import DataTree
import yaml

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


def write_main_table_template(msv4: DataTree, output_path: str | os.PathLike):
    """
    TODO

    msv4: Root node of one MSv4
    output_path: Path to base directory of the output MS
    """
    scalar_column_definitions = {
        "ANTENNA1": int,
        "ANTENNA2": int,
        "ARRAY_ID": int,
        "DATA_DESC_ID": int,
        "EXPOSURE": float,
        "FEED1": int,
        "FEED2": int,
        "FIELD_ID": int,
        "FLAG_ROW": bool,
        "INTERVAL": float,
        "OBSERVATION_ID": int,
        "PROCESSOR_ID": int,
        "SCAN_NUMBER": int,
        "STATE_ID": int,
        "TIME": float,
        "TIME_CENTROID": float,
    }

    scalar_column_descriptors = [
        makescacoldesc(name, valuetype(), datamanagergroup=name)
        for name, valuetype in scalar_column_definitions.items()
    ]

    nchan: int = msv4["frequency"].size
    npol = 4

    array_column_definitions = {
        "DATA": (complex, (nchan, npol)),
        "FLAG": (bool, (nchan, npol)),
        "FLAG_CATEGORY": (bool, (1, nchan, npol)),  # enforcing one category
        "SIGMA": (float, (nchan,)),
        "UVW": (float, (3,)),
        "WEIGHT": (float, (npol,)),
        "WEIGHT_SPECTRUM": (float, (nchan, npol)),
    }

    ### MeerKAT nano
    # DATA
    # {'UNIT': 'Jy'}

    # EXPOSURE
    # {'QuantumUnits': ['s']}

    # FLAG_CATEGORY
    # {'CATEGORY': []}

    # INTERVAL
    # {'QuantumUnits': ['s']}

    # TIME
    # {'QuantumUnits': ['s'], 'MEASINFO': {'type': 'epoch', 'Ref': 'UTC'}}

    # TIME_CENTROID
    # {'QuantumUnits': ['s'], 'MEASINFO': {'type': 'epoch', 'Ref': 'UTC'}}

    # UVW
    # {'QuantumUnits': ['m', 'm', 'm'], 'MEASINFO': {'type': 'uvw', 'Ref': 'ITRF'}}

    ### Simulated AA2 Mid datset
    # DATA
    # {'UNIT': 'Jy'}

    # EXPOSURE
    # {'QuantumUnits': ['s']}

    # FLAG_CATEGORY
    # {'CATEGORY': []}

    # INTERVAL
    # {'QuantumUnits': ['s']}

    # TIME
    # {'QuantumUnits': ['s'], 'MEASINFO': {'type': 'epoch', 'Ref': 'UTC'}}

    # TIME_CENTROID
    # {'QuantumUnits': ['s'], 'MEASINFO': {'type': 'epoch', 'Ref': 'UTC'}}

    # UVW
    # {'QuantumUnits': ['m', 'm', 'm'], 'MEASINFO': {'type': 'uvw', 'Ref': 'ITRF'}}

    def data_manager_group(colname: str) -> str:
        return "Tiled" + "".join(map(str.capitalize, colname.split("_")))

    # TODO: Add keywords 'QuantumUnits' and 'MEASINFO' for columns:
    # DATA, EXPOSURE, FLAG_CATEGORY, INTERVAL, TIME, TIME_CENTROID
    array_column_descriptors = [
        makearrcoldesc(
            name,
            valuetype(),
            shape=shape,
            datamanagertype="TiledColumnStMan",
            datamanagergroup=data_manager_group(name),
        )
        for name, (valuetype, shape) in array_column_definitions.items()
    ]
    tdesc = maketabdesc(scalar_column_descriptors + array_column_descriptors)
    ms_table = table(output_path, tdesc, nrow=0)
    ms_table.close()


def write_antenna_table(msv4: DataTree, output_path: str | os.PathLike):
    """
    TODO

    msv4: Root node of one MSv4
    output_path: Path to base directory of the output MS
    """
