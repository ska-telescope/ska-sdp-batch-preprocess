# see license in parent directory

from logging import Logger
from typing import Tuple

from numpy.typing import NDArray
from xradio.vis.schema import VisibilityXds

from ska_sdp_datamodels.visibility import (
    Visibility
)
from ska_sdp_datamodels.visibility.vis_xradio import (
    convert_visibility_xds_to_visibility,
    convert_visibility_to_visibility_xds
)

from utils import log_handler


class ProcessingIntent:
    """
    Class to represent a processing set of MSv4
    in memory.

    Attributes
    ----------
    data_as_xradio_vis: xradio.vis.schema.VisibilityXds
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
      using the compute() method.
    """

    def __init__(
            self, data_as_xradio_vis: VisibilityXds, 
            *, logger: Logger
    ):
        """
        Initiates the ProcessingIntent class.

        Parameters
        ----------
        data: xradio.vis.schema.VisibilityXds
          contains the processing set data.
        """
        self.data_as_xradio_vis = data_as_xradio_vis
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
                return convert_visibility_xds_to_visibility(
                    self.data_as_xradio_vis
                )
        except:
            log_handler.enable_logs_manually()
            self.logger.critical(
                "Could not convert XRadio-Visibility to SKA-Visibility\n  |"
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
            return self.data_as_xradio_vis["VISIBILITY"].values
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
            return self.data_as_xradio_vis["UVW"].values
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
            return self.data_as_xradio_vis["WEIGHT"].values
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
            chan_freq = self.data_as_xradio_vis["frequency"].values.flatten()
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
            cls, data_as_xradio_vis: VisibilityXds, 
            *, logger: Logger
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
        return cls(
            data_as_xradio_vis.compute(), logger=logger
        )
    
def ska_vis_to_xradio_vis(ska_vis: Visibility) -> VisibilityXds:
    """
    Standalone function to convert SKA-Visibility 
    datamodel to XRadio-Visibility datamodel.

    Arguments
    ---------
    ska_vis: ska_sdp_datamodels.visibility.Visibility
       XRadio-Visibility representation of the processing set data.

    Returns
    -------
    SKA-Visibility representation of the processing set data.
    """
    return convert_visibility_to_visibility_xds(ska_vis)