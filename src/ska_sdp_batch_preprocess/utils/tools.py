# see license in parent directory

import contextlib
import os
import sys


def reinstate_default_stdout() -> None:
    """
    """
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__

@contextlib.contextmanager
def write_to_devnull():
    """
    """
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    yield
    reinstate_default_stdout()