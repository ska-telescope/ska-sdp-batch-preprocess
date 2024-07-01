# see license in parent directory

from pathlib import Path

import numpy as np
from casacore.tables import table
from numpy.typing import NDArray


class MeasurementSet:
    """"""

    def __init__(self, dir: Path):
        """
        """
        self.input_dir = dir

    @property
    def data(self) -> NDArray:
        """
        """
        try:
           vis = table(str(self.input_dir)).getcol("DATA")
        except:
            raise FileNotFoundError("expected a 'DATA' column")
        if len(np.asarray(vis).shape) > 4:
            raise ValueError("unsupported DATA with more than 4 dimensions")
        return vis