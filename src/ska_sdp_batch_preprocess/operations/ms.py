# see license in parent directory

from pathlib import Path
from typing import Optional, Tuple

import numpy as np
from casacore.tables import table
from numpy.typing import NDArray
from xradio.vis import(
    convert_msv2_to_processing_set
)


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
           output = table(f"{self.input_dir}").getcol("DATA")
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
           output = table(f"{self.input_dir}").getcol("UVW")
        except:
            raise FileNotFoundError("expected a 'UVW' column")
        if len(np.asarray(output).shape) != 3:
            raise ValueError(
                "there must be 3 positional coordinates per observation"
            )
        return output
    
    @property
    def channels(self) -> Tuple[float, float]:
        """
        """
        try:
            chan_freq = table(
                f"{self.input_dir.joinpath('SPECTRAL_WINDOW')}"
            ).getcol("CHAN_FREQ")
        except:
            raise FileNotFoundError(
                "expected a 'SPECTRAL_WINDOW' table with a 'CHAN_FREQ' column"
            )
        chan_freq = chan_freq.flatten()
        if len(chan_freq) == 1:
            return chan_freq[0], 0.
        return (chan_freq[0], chan_freq[1]-chan_freq[0])
    
    def to_processing_set(
            self, args: Optional[dict]=None
    ) -> None:
        """
        """
        if args is None:
            convert_msv2_to_processing_set(
                f"{self.input_dir}", 
                f"{self.input_dir.with_suffix('.ps')}"
            )
        else:
            convert_msv2_to_processing_set(
                f"{self.input_dir}", 
                f"{self.input_dir.with_suffix('.ps')}",
                **args
            )