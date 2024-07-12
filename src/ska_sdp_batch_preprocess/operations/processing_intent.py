# see license in parent directory

from logging import Logger
from typing import Tuple

from numpy.typing import NDArray
from xarray.core.dataset import Dataset

from ska_sdp_datamodels.visibility import (
    Visibility
)
from ska_sdp_datamodels.visibility.vis_xradio import (
    create_visibility_from_xradio_xds
)

from utils import log_handler


class ProcessingIntent:
    """
    Class to represent a processing set of MSv4
    in memory.

    Attributes
    ----------
    data_as_xarray: xarray.Dataset
      XArray representation of the processing set data.

    data_as_ska_vis: ska_sdp_datamodels.visibility.Visibility
      SKA-Visibility representation of the processing set data.

    visibilities: NDArray
      visibilities as NumPy arrays.

    uvw: NDArray
      uvw data as NumPy arrays

    weights: NDArray
      weights as NumPy arrays.

    channels: Tuple[float, float]
      base frequency and frequency increments.

    logger: logging.Logger
      logger object to handle pipeline logs.

    Methods
    -------
    manual_compute(**args)
      class method to generate an instance with
      the data manually loaded into memory as XArrays
      using compute() method.
    """

    def __init__(
            self, data_as_xarray: Dataset, *, logger: Logger
    ):
        """
        Initiates the ProcessingIntent class.

        Parameters
        ----------
        data: xarray.Dataset
          contains the processing set data.
        """
        self.data_as_xarray = data_as_xarray
        self.logger = logger

    @property
    def data_as_ska_vis(self) -> Visibility:
        """
        SKA-Visibility representation of the processing set data.

        Returns
        -------
        SKA-Visibility class instance.
        """
        try:
            with log_handler.temporary_log_disable():
                return create_visibility_from_xradio_xds(
                    self.data_as_xarray
                )
        except:
            log_handler.enable_logs_manually()
            self.logger.critical(
                "Could not convert XArray data to SKA-Visibility object\n  |"
            )
            log_handler.exit_pipeline(self.logger)

    @property
    def visibilities(self) -> NDArray:
        """
        Visibilities as NumPy arrays.

        Returns
        -------
        NumPy array enclosing visibilities.
        """
        try:
            return self.data_as_xarray["VISIBILITY"].values
        except:
            self.logger.critical(
                "Could not read visibilities from MSv4\n  |"
            )
            log_handler.exit_pipeline(self.logger)
        
    @property
    def uvw(self) -> NDArray:
        """
        UVW data as NumPy arrays.

        Returns
        -------
        NumPy array enclosing UVW data.
        """
        try:
            return self.data_as_xarray["UVW"].values
        except:
            self.logger.critical(
                "Could not read UVW data from MSv4\n  |"
            )
            log_handler.exit_pipeline(self.logger)
        
    @property
    def weights(self) -> NDArray:
        """
        Weights as NumPy arrays.

        Returns
        -------
        NumPy array enclosing weights.
        """
        try:
            return self.data_as_xarray["WEIGHT"].values
        except:
            self.logger.critical(
                "Could not read weights from MSv4\n  |"
            )
            log_handler.exit_pipeline(self.logger)
    
    @property
    def channels(self) -> Tuple[float, float]:
        """
        Base frequency and frequency increments.

        Returns
        -------
        Tuple of base frequency and frequency increments.
        """
        try:
            chan_freq = self.data_as_xarray["frequency"].values.flatten()
            if len(chan_freq) == 1:
                return chan_freq[0], 0.
            return (chan_freq[0], chan_freq[1]-chan_freq[0])
        except:
            self.logger.critical(
                "Could not read frequency data from MSv4\n  |"
            )
            log_handler.exit_pipeline(self.logger)

    @classmethod
    def manual_compute(
            cls, data_as_xarray: Dataset, *, logger: Logger
    ):
        """
        Class method to generate an instance with
        the data manually loaded into memory as XArrays
        using the compute() method.

        Arguments
        ---------
        data: xarray.Dataset
          XArray representation of the processing set data,
          which are to be loaded manually.
        
        Returns
        -------
        ProcessingIntent class instance.

        Note
        ----
        This manual_compute() method does not support 
        slicing/partial data loading due to the default XArray
        functionality stipulating that data are normally loaded
        automatically. Hence, this manual_compute() class method
        should not be needed in normal circumstances.
        https://docs.xarray.dev/en/latest/generated/xarray.Dataset.compute.html
        """
        return cls(data_as_xarray.compute(), logger=logger)