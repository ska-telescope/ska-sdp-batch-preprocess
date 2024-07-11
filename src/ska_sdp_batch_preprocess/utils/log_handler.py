# see license in parent directory

import logging
import sys


def generate(name: str) -> logging.Logger:
    """
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter(
            " +| %(name)s [%(asctime)s - %(levelname)s]: %(message)s"
        )
    )
    logger.addHandler(stream_handler)
    return logger

def exit_pipeline(
        logger: logging.Logger, *, success: bool=False
) -> None:
    """
    """
    logger.info("Exiting pipeline")
    if success:
        logger.info("Pipeline run - SUCCESS")
    else:
        logger.info("Pipeline run - FAILURE")
    sys.exit()