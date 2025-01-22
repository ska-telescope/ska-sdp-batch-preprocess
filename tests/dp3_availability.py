import os
import subprocess

import pytest


def dp3_available() -> bool:
    """
    True if DP3 is available to run via CLI.
    """
    try:
        subprocess.check_call(
            ["DP3"], env=os.environ | {"OPENBLAS_NUM_THREADS": "1"}
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


skip_unless_dp3_available = pytest.mark.skipif(
    not dp3_available(), reason="DP3 not available"
)
