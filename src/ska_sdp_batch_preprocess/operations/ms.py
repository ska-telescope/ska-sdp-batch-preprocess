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
           output = table(str(self.input_dir)).getcol("DATA")
        except:
            raise FileNotFoundError("expected a 'DATA' column")
        if len(np.asarray(output).shape) > 4:
            raise ValueError(
                "unsupported DATA with more than 4 dimensions"
            )
        return output
    
    @property
    def uvw(self) -> NDArray:
        """
        """
        try:
           output = table(str(self.input_dir)).getcol("UVW")
        except:
            raise FileNotFoundError("expected a 'UVW' column")
        if len(np.asarray(output).shape) != 3:
            raise ValueError(
                "there must be 3 positional coordinates per observation"
            )
        return output