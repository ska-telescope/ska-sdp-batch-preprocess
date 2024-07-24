# see license in parent directory

import contextlib
import logging
import sys
from datetime import datetime
from pathlib import Path


def generate(
        name: str, *, cmd_logs: bool=False
) -> logging.Logger:
    """
    Generates a new logging.Logger class instance.

    Arguments
    ---------
    name: str
      name given to the new class instance.

    cmd_logs: bool=False
      optional argument which, if True, logs will 
      be generated on the command line in addition 
      to logging into a logfile.

    Returns
    -------
    New logging.Logger class instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    log_folder_name = datetime.now().strftime(
        "%Y-%m-%d_%H-%M-%S"
    )
    Path.cwd().joinpath(
        log_folder_name
    ).mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(
        f"{Path.cwd().joinpath(log_folder_name, 'logfile.log')}"
    )
    file_handler.setFormatter(
        logging.Formatter(
            " +| %(name)s [%(asctime)s - %(levelname)s]: %(message)s"
        )
    )
    logger.addHandler(file_handler)
    
    if cmd_logs:
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
      optional argument which, if True, the pipeline 
      declares a successful run as it ends the job, 
      but declares a failed run otherwise.
    """
    logger.info("Exiting pipeline")
    if success:
        logger.info("Pipeline run - SUCCESS")
    else:
        logger.info("Pipeline run - FAILURE")
    sys.exit()

def enable_logs_manually() -> None:
    """
    Reverts the logging premission to default
    (i.e., logging.NOTSET). Useful where logging
    is temporarily blocked via a context manager
    but an exception occurs while executing.
    """
    logging.disable(logging.NOTSET)

@contextlib.contextmanager
def temporary_log_disable():
    """
    Context manager to disable logging while
    running a piece of code, and subsequently
    reverting to default behaviour.
    """
    logging.disable(logging.CRITICAL)
    yield
    enable_logs_manually()