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
 
from ska_sdp_datamodels.visibility import (
    create_visibility_from_ms
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
    dataframe: list[ProcessingIntent]
      contains the MS data as a list of ProcessingIntent objects.

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
    The ProcessingIntent class is designed to incorporate MS data as
    both SKA-datamodel (Visibility) and XRadio-datamodel (VisibilityXds).
    Both datamodels are schemas of the xarray.Dataset type. However, 
    prior to calling as further XArray functionalities on the dataframe,
    check the relevant documentation of XRadio and SKA-SDP-Datamodels.
    """

    def __init__(
            self, dataframe: list[ProcessingIntent], *, logger: Logger
    ):
        """
        Initiates the MeasurementSet class.

        Parameters
        ----------
        dataframe: list[ProcessingIntent]
          contains the MS data.
        """
        self.dataframe = dataframe
        self.logger = logger

        if len(self.dataframe) == 0:
            logger.warning("Loaded an empty MS into memory")

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
                self.logger.critical("Could not read visibilities from MSv2\n  |")
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
                self.logger.critical("Could not read UVW from MSv2\n  |")
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
                self.logger.critical("Could not read weights from MSv2\n  |")
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
                with tools.write_to_devnull():
                    chan_freq = table(
                        self.dataframe.getkeyword("SPECTRAL_WINDOW")
                    ).getcol("CHAN_FREQ").flatten()
            except:
                tools.reinstate_default_stdout()
                self.logger.critical(
                    "Could not read frequency data from MSv2\n  |"
                )
                log_handler.exit_pipeline(self.logger)
            if len(chan_freq) == 1:
                return chan_freq[0], 0.
            return (chan_freq[0], chan_freq[1]-chan_freq[0])
        return [
            intent.channels
            for intent in self.dataframe
        ]
    
    @classmethod
    def ver_2(
            cls, dir: Path, *, logger: Logger, manual_compute: bool=False
    ):
        """
        Class method to generate an instance with MSv2.

        Arguments
        ---------
        dir: pathlib.Path
          directory for the input MSv2.

        logger: logging.Logger
          logger object to handle pipeline logs.

        manual_compute: bool=False
          optional argument which, if True, the MSv2 data get
          loaded as ProcessingIntent objects while calling the 
          XArray compute() method on them.

        Returns
        -------
        MeasurementSet class instance.
        """
        try:
            with log_handler.temporary_log_disable() and tools.write_to_devnull():
                dataframe = [
                    ProcessingIntent.load_ska_vis(
                        item, logger=logger, manual_compute=manual_compute
                    ) for item in create_visibility_from_ms(f"{dir}")
                ]
        except:
            log_handler.enable_logs_manually()
            tools.reinstate_default_stdout()
            logger.critical(f"Could not load {dir.name} as MSv2\n  |")
            log_handler.exit_pipeline(logger)
        return cls(dataframe, logger=logger)
     
    @classmethod
    def ver_4(
            cls, dir: Path, *, logger: Logger, manual_compute: bool=False
    ):
        """
        Class method to generate an instance with MSv4.

        Arguments
        ---------
        dir: pathlib.Path
          directory for the input MSv4.

        logger: logging.Logger
          logger object to handle pipeline logs.

        manual_compute: bool=False
          optional argument which, if True, loads MSv4 data as
          ProcessingIntent objects while calling the XArray 
          compute() method on them.

        Returns
        -------
        MeasurementSet class instance.
        """
        try:
            with log_handler.temporary_log_disable() and tools.write_to_devnull():
                dataframe = [
                    ProcessingIntent.load_xradio_vis(
                        item, logger=logger, manual_compute=manual_compute
                    ) for item in read_processing_set(f"{dir}").values()
                ]
        except:
            log_handler.enable_logs_manually()
            tools.reinstate_default_stdout()
            logger.critical(f"Could not load {dir.name} as MSv4\n  |")
            log_handler.exit_pipeline(logger)
        return cls(dataframe, logger=logger)

def to_msv4(
        msin: Path, args: Optional[dict]=None, 
        *, logger: Logger
) -> None:
    """
    Converts MSv2 to MSv4 on disk using XRadio.
    
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
        with log_handler.temporary_log_disable() and tools.write_to_devnull():
            if args is None:
                convert_msv2_to_processing_set(
                    f"{msin}", f"{msin.with_suffix('.ms4')}"
                )
            else:
                convert_msv2_to_processing_set(
                    f"{msin}", f"{msin.with_suffix('.ms4')}", **args
                )
    except:
        log_handler.enable_logs_manually()
        tools.reinstate_default_stdout()
        logger.critical(f"Could not convert {msin.name} to MSv4\n  |")
        log_handler.exit_pipeline(logger)