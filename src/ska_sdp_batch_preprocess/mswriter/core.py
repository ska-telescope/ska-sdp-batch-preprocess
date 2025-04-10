import os
from pathlib import Path
from xarray import DataTree

from casacore.tables import table, maketabdesc, makescacoldesc, makearrcoldesc


"""
Preconditions:
- Only one MSv4 in the processing set
- Fixed phase centre
- Four linear pols
"""


def write_msv2_template_matching_xradio_processing_set(
    root: DataTree, output_path: str | os.PathLike
):
    """
    TODO

    root: Root node of the xradio processing set
    """
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

    # Create subtables
    # TODO


def write_main_table_template(msv4: DataTree, output_path: str | os.PathLike):
    """
    TODO

    msv4: Root node of one MSv4
    """

    # Read from MKT nano MS:
    # [
    #     "ANTENNA1",
    #     "ANTENNA2",
    #     "ARRAY_ID",
    #     "DATA",
    #     "DATA_DESC_ID",
    #     "EXPOSURE",
    #     "FEED1",
    #     "FEED2",
    #     "FIELD_ID",
    #     "FLAG",
    #     "FLAG_CATEGORY",
    #     "FLAG_ROW",
    #     "INTERVAL",
    #     "OBSERVATION_ID",
    #     "PROCESSOR_ID",
    #     "SCAN_NUMBER",
    #     "SIGMA",
    #     "STATE_ID",
    #     "TIME",
    #     "TIME_CENTROID",
    #     "UVW",
    #     "WEIGHT",
    #     "WEIGHT_SPECTRUM",
    # ]

    # What DP3 MSWriter wants to see:
    #
    # [
    #     "ARRAY_ID",
    #     "DATA_DESC_ID",
    #     "EXPOSURE",
    #     "FEED1",
    #     "FEED2",
    #     "FIELD_ID",
    #     "FLAG_CATEGORY",
    #     "FLAG_ROW",
    #     "INTERVAL",
    #     "OBSERVATION_ID",
    #     "PROCESSOR_ID",
    #     "SCAN_NUMBER",
    #     "SIGMA",
    #     "STATE_ID",
    #     "TIME",
    #     "TIME_CENTROID",
    #     "WEIGHT",
    # ]

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
        makescacoldesc(name, valuetype())
        for name, valuetype in scalar_column_definitions.items()
    ]

    array_column_definitions = {
        "UVW": (float, (3,)),
    }

    array_column_descriptors = [
        makearrcoldesc(name, valuetype(), shape=shape, valuetype="double")
        for name, (valuetype, shape) in array_column_definitions.items()
    ]
    tdesc = maketabdesc(scalar_column_descriptors + array_column_descriptors)
    ms_table = table(output_path, tdesc, nrow=0)
    ms_table.close()
