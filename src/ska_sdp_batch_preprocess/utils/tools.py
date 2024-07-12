# see license in parent directory

import contextlib
import os
import sys


def reinstate_default_stdout() -> None:
    """
    Reverts the command line stdout/stderr to 
    default (i.e., sys.__stdout__ and 
    sys.__stderr__). Useful where writing to the
    command line is temporarily blocked via a 
    context manager but an exception occurs while 
    executing.
    """
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__

@contextlib.contextmanager
def write_to_devnull():
    """
    Context manager to disable sdtout/stderr to
    the command line (via writing to devnull) while
    running an piece of code, and subsequently
    reverting to default behaviour.
    """
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    yield
    reinstate_default_stdout()