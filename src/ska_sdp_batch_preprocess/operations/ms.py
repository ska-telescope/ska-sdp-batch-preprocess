# see license in parent directory

from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
from casacore.tables import table
from numpy.typing import NDArray
from xradio.vis import (
    convert_msv2_to_processing_set,
    read_processing_set
)

from operations.processing_intent import (
    ProcessingIntent
)


class MeasurementSet:
    """
    Class to represent MS in memory.

    Attributes
    ----------
    dataframe: casacore.tables.table | list[ProcessingIntent]
      contains the MS data (casacore table if MSv2 
      or an iterable if MSv4).

    visibilities: NDArray | list[NDArray]
      visibilities as numpy arrays (or list thereof for MSv4).

    uvw: NDArray | list[NDArray]
      uvw data as numpy arrays (or list thereof for MSv4).
    
    weights: NDArray | list[NDArray]
      weights as numpy arrays (or list thereof for MSv4).

    channels: Tuple[float, float] | list[Tuple[float, float]]
      base frequency and frequency increments (or list thereof for MSv4).
    
    Methods
    -------
    ver_2(**args)
      class method to generate an instance with MSv2.

    ver_4(**args)
      class method to generate an instance with MSv4.

    Note: call further casacore/xarray functionalities on the 
      class instance where needed.
    """

    def __init__(
            self, 
            dataframe: Union[table, list[ProcessingIntent]]
    ):
        """
        Initiates the MeasurementSet class.

        Parameters
        ----------
        dataframe: casacore.tables.table | list[ProcessingIntent]
          contains the MS data (casacore table if MSv2 
          or an iterable if MSv4).
        """
        self.dataframe = dataframe

    @property
    def visibilities(self) -> Union[NDArray, list[NDArray]]:
        """
        Visibilities as numpy arrays (or list thereof for MSv4).
        """
        if type(self.dataframe) == table:
            try:
                output = self.dataframe.getcol("DATA")
            except:
                raise RuntimeError(
                    "could not load visibilities from MSv2"
                )
            if len(np.asarray(output).shape) > 4:
                raise ValueError(
                    "unsupported MSv2 DATA with more than 4 dims"
                )
            return np.asarray(output)
        return [
            intent.visibilities
            for intent in self.dataframe
        ]
    
    @property
    def uvw(self) -> Union[NDArray, list[NDArray]]:
        """
        UVW data as numpy arrays (or list thereof for MSv4).
        """
        if type(self.dataframe) == table:
            try:
                output = self.dataframe.getcol("UVW")
            except:
                raise RuntimeError(
                    "could not load uvw from MSv2"
                )
            if len(np.asarray(output).shape) > 3:
                raise ValueError(
                    "unsupported MSv2 UVW with more than 3 dims"
                )
            return np.asarray(output)
        return [
            intent.uvw
            for intent in self.dataframe
        ]
    
    @property
    def weights(self) -> Union[NDArray, list[NDArray]]:
        """
        Weights as numpy arrays (or list thereof for MSv4).
        """
        if type(self.dataframe) == table:
            try:
                output = self.dataframe.getcol("WEIGHT")
            except:
                raise RuntimeError(
                    "could not load weights from MSv2"
                )
            if len(np.asarray(output).shape) > 4:
                raise ValueError(
                    "unsupported MSv2 WEIGHT with more than 4 dims"
                )
            return np.asarray(output)
        return [
            intent.weights
            for intent in self.dataframe
        ]

    @property
    def channels(self) -> Union[Tuple[float, float], list[Tuple[float, float]]]:
        """
        Base frequency and frequency increments (or list thereof for MSv4).
        """
        if type(self.dataframe) == table:
            try:
                chan_freq = table(
                    self.dataframe.getkeyword("SPECTRAL_WINDOW")
                ).getcol("CHAN_FREQ").flatten()
            except:
                raise RuntimeError(
                    "could not load frequency data from MSv2"
                )
            if len(chan_freq) == 1:
                return chan_freq[0], 0.
            return (chan_freq[0], chan_freq[1]-chan_freq[0])
        return [
            intent.channels
            for intent in self.dataframe
        ]
    
    @classmethod
    def ver_2(cls, dir: Path):
        """
        """
        try:
            return cls(table(f"{dir}"))
        except:
            raise RuntimeError(
                "could not load MSv2"
            )
    
    @classmethod
    def ver_4(
            cls, dir: Path, *, manual_compute: bool=False
    ):
        """
        """
        list_of_intents = [
            ProcessingIntent.manual_compute(intent)
            if manual_compute else ProcessingIntent(intent)
            for intent in read_processing_set(f"{dir}").values()
        ]
        if len(list_of_intents) == 0:
            raise ValueError(
                "loaded empty MSv4; check it is not MSv2"
            )
        return cls(list_of_intents)

def to_msv4(
        msin: Path, args: Optional[dict]=None
) -> None:
    """
    """
    try:
        if args is None:
            convert_msv2_to_processing_set(
                f"{msin}", 
                f"{msin.with_suffix('.ms4')}"
            )
        else:
            convert_msv2_to_processing_set(
                f"{msin}", 
                f"{msin('.ms4')}",
                **args
            )
    except:
        raise RuntimeError(
            "conversion not possible; check input type & whether output already exists"
        )