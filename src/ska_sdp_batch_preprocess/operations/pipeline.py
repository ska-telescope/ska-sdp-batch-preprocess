# see license in parent directory

from pathlib import Path

from operations.ms import (
    MeasurementSet, to_msv4
)


def run(
        msin: Path, config: dict
) -> None:
    """
    Principal function in th pipeline where the various
    functionalities are executed based on the YAML file
    instructions.

    Arguments
    ---------
    msin: pathlib.Path
      directory for the input MS (v2 or v4).

    config: dict
      YAML configuration parameters read as Python 
      dictionary.
    """
    if config is not None:
        for func, args in config.items():
            if func.lower() == "convert_msv2_to_msv4":
                to_msv4(msin, args)
            elif func.lower() == "load_msv2":
                MSv2 = MeasurementSet.ver_2(msin)
            elif func.lower() == "load_msv4":
                MSv4 = MeasurementSet.ver_4(msin)
            elif func == "convert_msv2_to_msv4_then_load":
                to_msv4(msin, args)
                MSv4 = MeasurementSet.ver_4(
                    msin.with_suffix(".ms4")
                )