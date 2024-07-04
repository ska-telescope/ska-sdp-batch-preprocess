# see license in parent directory

from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
from casacore.tables import table
from numpy.typing import NDArray
from xradio.vis import(
    convert_msv2_to_processing_set,
    read_processing_set
)

from operations.processing_intent import (
    ProcessingIntent
)


class MeasurementSet:
    """"""

    def __init__(
            self, 
            dataframe: Union[table, list[ProcessingIntent]],
            *, v2: bool=False, v4: bool=False
    ):
        """
        """
        self.v2 = v2
        self.v4 = v4
        if self.v2 and self.v4:
            raise TypeError(
                "MS cannot be both v2 & v4"
            )
        self.dataframe = dataframe

    @property
    def visibilities(self) -> Optional[NDArray]:
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
    def uvw(self) -> Optional[NDArray]:
        """
        """
        if self.v2:
            try:
                output = self.dataframe.getcol("UVW")
            except FileNotFoundError as e:
                raise e(
                    "expected a 'UVW' column in MSv2"
                )
            if len(np.asarray(output).shape) > 3:
                raise ValueError(
                    "unsupported MSv2 UVW with more than 3 dims"
                )
            return np.asarray(output)
        elif self.v4:
            raise NotImplementedError(
                "MSv4 functionality not yet implemented"
            )
    
    @property
    def channels(self) -> Optional[Tuple[float, float]]:
        """
        """
        if self.v2:
            try:
                chan_freq = self.dataframe.getkeyword(
                    "SPECTRAL_WINDOW"
                ).getcol("CHAN_FREQ")
            except FileNotFoundError as e:
                raise e(
                    "expected a 'SPECTRAL_WINDOW' table with a 'CHAN_FREQ' column in MSv2"
                )
            chan_freq = chan_freq.flatten()
            if len(chan_freq) == 1:
                return chan_freq[0], 0.
            return (chan_freq[0], chan_freq[1]-chan_freq[0])
        elif self.v4:
            raise NotImplementedError(
                "MSv4 functionality not yet implemented"
            )
    
    def to_msv4(self, args: Optional[dict]=None) -> None:
        """
        """
        if self.v4:
            raise TypeError("already MSv4")
        if args is None:
            convert_msv2_to_processing_set(
                f"{self.input_dir}", 
                f"{self.input_dir.with_suffix('.ms4')}"
            )
        else:
            convert_msv2_to_processing_set(
                f"{self.input_dir}", 
                f"{self.input_dir.with_suffix('.ms4')}",
                **args
            )

    @classmethod
    def ver_2(cls, dir: Path):
        """
        """
        return cls(table(dir), v2=True)
    
    @classmethod
    def ver_4(
            cls, dir: Path, *, manual_compute: bool=False
    ):
        """
        """
        if manual_compute:
            return cls([
                ProcessingIntent.manual_compute(intent)
                for intent in read_processing_set(dir).values()
            ], v4=True)
        return cls([
            ProcessingIntent(intent)
            for intent in read_processing_set(dir).values()
        ], v4=True)