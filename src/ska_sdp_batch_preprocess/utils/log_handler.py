# see license in parent directory

import logging


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