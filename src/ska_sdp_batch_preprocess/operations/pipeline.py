# see license in parent directory

from logging import Logger
from pathlib import Path

from operations.measurement_set import (
    MeasurementSet, to_msv4
)


def run(
        msin: Path, config: dict, *, logger: Logger
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

    logger: logging.Logger
      logger object to handle pipeline logs.
    """
    if config is not None:
        for func, args in config.items():
            if func.lower() == "convert_msv2_to_msv4":
                logger.info(f"Converting {msin.name} to MSv4")
                to_msv4(msin, args, logger=logger)
                logger.info("Conversion successful\n  |")
            
            elif func.lower() == "load_msv2":
                logger.info(
                    f"Loading {msin.name} into memory as MSv2"
                )
                MSv2 = MeasurementSet.ver_2(msin, logger=logger)
                logger.info("Load successful\n  |")
            
            elif func.lower() == "load_msv4":
                logger.info(
                    f"Loading {msin.name} into memory as MSv4"
                )
                MSv4 = MeasurementSet.ver_4(msin, logger=logger)
                logger.info("Load successful\n  |")
            
            elif func == "convert_msv2_to_msv4_then_load":
                logger.info(f"Converting {msin.name} to MSv4")
                to_msv4(msin, args, logger=logger)
                logger.info("Conversion successful\n  |")
                logger.info(
                    f"Loading {msin.with_suffix('.ms4').name} into memory as MSv4"
                )
                MSv4 = MeasurementSet.ver_4(
                    msin.with_suffix(".ms4"), logger=logger
                )
                logger.info("Load successful\n  |")