# see license in parent directory

import contextlib
import os
import sys


@contextlib.contextmanager
def write_to_devnull():
    """
    """
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    yield
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__