# see license in parent directory

from logging import Logger
from typing import Any, Tuple, Union

from numpy.typing import NDArray
from xradio.vis.schema import VisibilityXds

from ska_sdp_datamodels.visibility import (
    Visibility
)
from ska_sdp_datamodels.visibility.vis_xradio import (
    convert_visibility_xds_to_visibility,
    convert_visibility_to_visibility_xds
)

from utils import log_handler, tools


class ProcessingIntent:
    """
    Class to represent an assemblage of data within the MS in memory,
    which are intended to be processed jointly. In MSv4, data may be 
    partitioned into separate processing sets for discrete processing; 
    this class is designed to efficiently represent such partitioned data 
    in memory. MSv2 is unlikely to be partitioned by default; hence, in such 
    a case the full MS will be represented by a single instance of this class.
    Such representation can aid in future endeavours for partitioning MSv2
    data and/or converting into processing sets while loaded in memory.

    Attributes
    ----------
    _input_data: ska_sdp_datamodels.visibility.Visibility | xradio.vis.schema.VisibilityXds
      THIS IS A PRIVATE CLASS ATTRIBUTE.
      If data are loded as SKA-Visibility they will be automatically made
      available as XRadio-Visibility, and vice versa. This holds true as
      long as the functions 'convert_visibility_xds_to_visibility' and
      'convert_visibility_to_visibility_xds' as operational.
    
    data_as_ska_vis: ska_sdp_datamodels.visibility.Visibility
      SKA-Visibility representation of the processing set data. 
      If the data were loaded by the user as XRadio-Visibility, then this
      attribute will only work if 'convert_visibility_xds_to_visibility'
      is operational.

    data_as_xradio_vis: xradio.vis.schema.VisibilityXds
      XRadio-Visibility representation of the processing set data.
      If the data were loaded by the user as XRadio-Visibility, then this
      attribute will only work if 'convert_visibility_to_visibility_xds'
      is operational.

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
            self, input_data: Union[Visibility, VisibilityXds],
            *, logger: Logger
    ):
        """
        Initiates the ProcessingIntent class.

        Parameters
        ----------
        input_data: ska_sdp_datamodels.visibility.Visibility | xradio.vis.schema.VisibilityXds
          contains the input class data.
          If data are loded as SKA-Visibility they will be automatically made
          available as XRadio-Visibility, and vice versa. This holds true as
          long as the functions 'convert_visibility_xds_to_visibility' and
          'convert_visibility_to_visibility_xds' as operational.

        logger: logging.Logger
          logger object to handle pipeline logs.
        """
        self._input_data = input_data
        self.logger = logger

    def __setattr__(self, key: str, value: Any) -> None:
        """
        The setter method of this class is amended here to inhibit external
        manipulation of private attributes (i.e., those starting with '_').
        """
        if key[0] == '_':
            self.logger.warning(f"Attribute {key} is private and cannot be changed")
            return
        self.__dict__[f"{key}"] = value

    @property
    def data_as_ska_vis(self) -> Visibility:
        """
        SKA-Visibility representation of the class data.
        If data were loded as XRadio-Visibility, this method will only work if  
        'convert_visibility_xds_to_visibility' is operational.

        Returns
        -------
        SKA-Visibility class instance.
        """
        if isinstance(self._input_data, Visibility):
            return self._input_data
        try:
            with log_handler.temporary_log_disable() and tools.write_to_devnull():
                return convert_visibility_xds_to_visibility(self._input_data)
        except:
            log_handler.enable_logs_manually()
            tools.reinstate_default_stdout()
            self.logger.critical(
                "Could not convert XRadio-Visibility to SKA-Visibility\n  |"
            )
            log_handler.exit_pipeline(self.logger)
        
    @property
    def data_as_xradio_vis(self) -> VisibilityXds:
        """
        XRadio-Visibility representation of the class data.
        If data were loded as SKA-Visibility, this method will only work if  
        'convert_visibility_to_visibility_xds' is operational.

        Returns
        -------
        XRadio-Visibility class instance.
        """
        if isinstance(self._input_data, VisibilityXds):
            return self._input_data
        try:
            with log_handler.temporary_log_disable() and tools.write_to_devnull():
                return convert_visibility_to_visibility_xds(self._input_data)
        except:
            log_handler.enable_logs_manually()
            tools.reinstate_default_stdout()
            self.logger.critical(
                "Could not convert SKA-Visibility to XRadio-Visibility\n  |"
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
            cls, input_data: Union[Visibility, VisibilityXds], 
            *, logger: Logger
    ):
        """
        Class method to generate an instance with the data manually loaded 
        into memory using the xarray compute() method.

        Arguments
        ---------
        input_data: ska_sdp_datamodels.visibility.Visibility | xradio.vis.schema.VisibilityXds
          the input data, which are to be loaded manually.

        logger: logging.Logger
          logger object to handle pipeline logs.
        
        Returns
        -------
        ProcessingIntent class instance.

        Note
        ----
        This manual_compute() method does not support slicing/partial data loading 
        due to the default xarray functionality stipulating that data are normally 
        loaded automatically. Hence, this manual_compute() class method should not 
        be needed in normal circumstances.
        https://docs.xarray.dev/en/latest/generated/xarray.Dataset.compute.html
        """
        return cls(input_data.compute(), logger=logger)