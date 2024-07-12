# see license in parent directory

from logging import Logger
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
from utils import log_handler, tools


class MeasurementSet:
    """
    Class to represent MS in memory.

    Attributes
    ----------
    dataframe: casacore.tables.table | list[ProcessingIntent]
      contains the MS data (casacore table if MSv2 
      or an iterable if MSv4).

    visibilities: NDArray | list[NDArray]
      visibilities as NumPy arrays (or list thereof for MSv4).

    uvw: NDArray | list[NDArray]
      uvw data as NumPy arrays (or list thereof for MSv4).
    
    weights: NDArray | list[NDArray]
      weights as NumPy arrays (or list thereof for MSv4).

    channels: Tuple[float, float] | list[Tuple[float, float]]
      base frequency and frequency increments (or list thereof for MSv4).

    logger: logging.Logger
      logger object to handle pipeline logs.
    
    Methods
    -------
    ver_2(**args)
      class method to generate an instance with MSv2.

    ver_4(**args)
      class method to generate an instance with MSv4.

    Note
    ----
    Call further casacore/xarray functionalities on the 
    class instance where needed.
    """

    def __init__(
            self, 
            dataframe: Union[table, list[ProcessingIntent]],
            *, logger: Logger
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
        self.logger = logger

    @property
    def visibilities(self) -> Union[NDArray, list[NDArray]]:
        """
        Visibilities as NumPy arrays (or list thereof for MSv4).

        Returns
        -------
        NumPy array (or list thereof for MSv4) enclosing visibilities.
        """
        if type(self.dataframe) == table:
            try:
                return np.asarray(
                    self.dataframe.getcol("DATA")
                )
            except:
                self.logger.critical("Could not read visibilities from MSv2")
                log_handler.exit_pipeline(self.logger)
        return [
            intent.visibilities
            for intent in self.dataframe
        ]
    
    @property
    def uvw(self) -> Union[NDArray, list[NDArray]]:
        """
        UVW data as NumPy arrays (or list thereof for MSv4).

        Returns
        -------
        NumPy array (or list thereof for MSv4) enclosing UVW data.
        """
        if type(self.dataframe) == table:
            try:
                return np.asarray(
                    self.dataframe.getcol("UVW")
                )
            except:
                self.logger.critical("Could not read UVW from MSv2")
                log_handler.exit_pipeline(self.logger)
        return [
            intent.uvw
            for intent in self.dataframe
        ]
    
    @property
    def weights(self) -> Union[NDArray, list[NDArray]]:
        """
        Weights as NumPy arrays (or list thereof for MSv4).

        Returns
        -------
        NumPy array (or list thereof for MSv4) enclosing weights.
        """
        if type(self.dataframe) == table:
            try:
                return np.asarray(
                    self.dataframe.getcol("WEIGHT")
                )
            except:
                self.logger.critical("Could not read weights from MSv2")
                log_handler.exit_pipeline(self.logger)
        return [
            intent.weights
            for intent in self.dataframe
        ]

    @property
    def channels(self) -> Union[Tuple[float, float], list[Tuple[float, float]]]:
        """
        Base frequency and frequency increments (or list thereof for MSv4).

        Returns
        -------
        Tuple of base frequency and frequency increments (or list thereof 
        for MSv4) enclosing weights.
        """
        if type(self.dataframe) == table:
            try:
                chan_freq = table(
                    self.dataframe.getkeyword("SPECTRAL_WINDOW")
                ).getcol("CHAN_FREQ").flatten()
            except:
                self.logger.critical("Could not read frequency data from MSv2")
                log_handler.exit_pipeline(self.logger)
            if len(chan_freq) == 1:
                return chan_freq[0], 0.
            return (chan_freq[0], chan_freq[1]-chan_freq[0])
        return [
            intent.channels
            for intent in self.dataframe
        ]
    
    @classmethod
    def ver_2(cls, dir: Path, *, logger: Logger):
        """
        Class method to generate an instance with MSv2.

        Arguments
        ---------
        dir: pathlib.Path
          directory for the input MSv2.

        Returns
        -------
        MeasurementSet class instance.
        """
        try:
            with tools.write_to_devnull():
                dataframe = table(f"{dir}")
            return cls(dataframe, logger=logger)
        except:
            tools.reinstate_default_stdout()
            logger.critical("Could not load MSv2 into memory")
            log_handler.exit_pipeline(logger)
                
    @classmethod
    def ver_4(
            cls, dir: Path, *, manual_compute: bool=False,
            logger: Logger
    ):
        """
        Class method to generate an instance with MSv4.

        Arguments
        ---------
        dir: pathlib.Path
          directory for the input MSv4.

        manual_compute: bool=False
          optional argument which, if true, calls the 
          ProcessingIntent class with the class method
          manual_compute(**args) on the XRadio MSv4 
          processing intent reads. 

        Returns
        -------
        MeasurementSet class instance.
        """
        list_of_intents = [
            ProcessingIntent.manual_compute(intent, logger=logger)
            if manual_compute else ProcessingIntent(intent, logger=logger)
            for intent in read_processing_set(f"{dir}").values()
        ]
        if len(list_of_intents) == 0:
            raise ValueError(
                "loaded empty MSv4; check it is not MSv2"
            )
        return cls(list_of_intents, logger=logger)

def to_msv4(
        msin: Path, args: Optional[dict]=None, 
        *, logger: Logger
) -> None:
    """
    Convert MSv2 to MSv4 on dist using XRadio.
    
    Arguments
    ---------
    msin: pathlib.Path
      directory for the input MSv2.
    
    args: dict | None=None
      Dictionary for the optional XRadio conversion 
      function arguments. 
    
    logger: logging.Logger
      logger object to handle pipeline logs.
    """
    try:
        with log_handler.temporary_log_disable():
            if args is None:
                convert_msv2_to_processing_set(
                    f"{msin}", 
                    f"{msin.with_suffix('.ms4')}"
                )
            else:
                convert_msv2_to_processing_set(
                    f"{msin}", 
                    f"{msin.with_suffix('.ms4')}",
                    **args
                )
    except:
        log_handler.enable_logs_manually()
        logger.critical(f"Cannot convert {msin.name} to MSv4")
        log_handler.exit_pipeline(logger)