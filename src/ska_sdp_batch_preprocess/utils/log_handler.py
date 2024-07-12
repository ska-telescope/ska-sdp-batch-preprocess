# see license in parent directory

import contextlib
import logging
import sys


def generate(name: str) -> logging.Logger:
    """
    Generates a new logging.Logger class instance.

    Arguments
    ---------
    name: str
      name given to the new class instance.

    Returns
    -------
    New logging.Logger class instance.
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
    Exits the pipeline gracefully.

    Arguments
    ---------
    logger: logging.Logger
      logger object to exit the pipeline with.

    success: bool=False
      optional argument which if True, the pipeline 
      declares a successful run of as it ends the 
      session, but declares a failed run otherwise.
    """
    logger.info("Exiting pipeline")
    if success:
        logger.info("Pipeline run - SUCCESS")
    else:
        logger.info("Pipeline run - FAILURE")
    sys.exit()

def enable_logs_manually() -> None:
    """
    """
    logging.disable(logging.NOTSET)

@contextlib.contextmanager
def temporary_log_disable():
    """
    """
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)