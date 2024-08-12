# see license in parent directory

from logging import Logger
from pathlib import Path

from operations.measurement_set import (
    convert_msv2_to_msv4, MeasurementSet
)


def run(
        msin: Path, config: dict, *, logger: Logger
) -> None:
    """
    Principal function in the pipeline where the various
    functionalities are executed based on the YAML file
    instructions.

    Arguments
    ---------
    msin: pathlib.Path
      directory for the input MS (v2 or v4).

    config: dict
      YAML configuration parameters read as Python 
      dictionary.

    logger: logging.Logger
      logger object to handle pipeline logs.
    """
    if config is not None:
        for func, args in config.items():
            if func.lower() == "convert_msv2_to_msv4":
                logger.info(f"Converting {msin.name} to MSv4")
                convert_msv2_to_msv4(msin, args, logger=logger)
                logger.info("Conversion successful\n  |")

            elif func.lower() == "load_msv2":
                logger.info(f"Loading {msin.name} into memory as MSv2")
                ms = MeasurementSet.ver_2(msin, args, logger=logger)
                logger.info("Load successful\n  |")

            elif func.lower() == "load_msv4":
                logger.info(f"Loading {msin.name} into memory as MSv4")
                ms = MeasurementSet.ver_4(msin, args, logger=logger)
                logger.info("Load successful\n  |")

            elif func.lower() == "export_to_msv2":
                logger.info("Exporting list of processing intents to MSv2")
                ms.export_to_msv2(msin.with_name(f"{msin.stem}-output.ms"), args)
                logger.info(f"{msin.stem}-output.ms generated successfully")