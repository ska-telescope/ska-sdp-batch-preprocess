# see license in parent directory

from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
from casacore.tables import table
from numpy.typing import NDArray
from xradio.vis import(
    convert_msv2_to_processing_set
)
from xradio.vis._processing_set import (
    processing_set
)


class MeasurementSet:
    """"""

    def __init__(
            self, dataframe: Union[table, processing_set],
            *, v2: bool=False, v4: bool=False
    ):
        """
        """
        if self.v2 and self.v4:
            raise TypeError(
                "your MS cannot be both v2 and v4"
            )
        self.dataframe = dataframe
        self.v2 = v2
        self.v4 = v4

    @property
    def data(self) -> Optional[NDArray]:
        """
        """
        if self.v2:
            try:
                output = self.dataframe.getcol("DATA")
            except FileNotFoundError as e:
                raise e(
                    "expected a 'DATA' column in MSv2"
                )
            if len(np.asarray(output).shape) > 4:
                raise ValueError(
                    "unsupported MSv2 DATA with more than 4 dims"
                )
            return np.asarray(output)
        elif self.v4:
            raise NotImplementedError(
                "MSv4 functionality not yet implemented"
            )
    
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