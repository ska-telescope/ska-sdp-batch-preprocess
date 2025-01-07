import logging
import logging.config
import sys
import time

NAME = "ska-sdp-batch-preprocess"

LOGGER = logging.getLogger(NAME)

# pylint: disable=line-too-long
FORMAT = "[%(asctime)s - %(name)s - %(levelname)s - %(filename)s#%(lineno)d] %(message)s"  # noqa: E501


class UTCFormatter(logging.Formatter):
    """
    Ensures we get UTC timestamps.
    """

    converter = time.gmtime


def _make_config() -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": UTCFormatter,
                "format": FORMAT,
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "stdout": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "default",
            },
        },
        "loggers": {
            NAME: {
                "handlers": ["stdout"],
                "level": "DEBUG",
                "propagate": False,
            },
        },
    }


def configure_logger():
    """
    Self-explanatory.
    """
    logging.config.dictConfig(_make_config())
