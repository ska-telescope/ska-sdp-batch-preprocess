# see license in parent directory

from logging import Logger
from typing import Any, Union

from numpy.typing import NDArray
from xradio.vis import VisibilityXds

from ska_sdp_datamodels.visibility import (
    Visibility
)
from ska_sdp_batch_preprocess.utils import (
    log_handler, tools
)
from ska_sdp_datamodels.visibility.vis_xradio import (
    convert_visibility_xds_to_visibility,
    convert_visibility_to_visibility_xds
)


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
      'convert_visibility_to_visibility_xds' are operational.
    
    data_as_ska_vis: ska_sdp_datamodels.visibility.Visibility
      SKA-Visibility representation of the processing set data. 
      If the data were loaded by the user as XRadio-Visibility, then this
      attribute will only work if 'convert_visibility_xds_to_visibility'
      is operational.

    data_as_xradio_vis: xradio.vis.schema.VisibilityXds
      XRadio-Visibility representation of the processing set data.
      If the data were loaded by the user as SKA-Visibility, then this
      attribute will only work if 'convert_visibility_to_visibility_xds'
      is operational.

    visibilities: NDArray
      visibilities as NumPy arrays.

    uvw: NDArray
      uvw data as NumPy arrays

    weights: NDArray
      weights as NumPy arrays.

    logger: logging.Logger
      logger object to handle pipeline logs.

    Methods
    -------
    manual_compute(**args)
      class method to generate an instance with the data manually loaded into 
      memory as xarrays using the compute() method.

    Notes
    -----
    1- This class avoids checking for VisibilityXds datatype (e.g., using
       isinstance). This is because, the current version of XRadio still 
       outputs xarray.Dataset type instead of the newly developed VisibilityXds 
       schema.
    2- The Visibility <-> VisibilityXds conversion only works as long as the
       relevant imported functions work.
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
        if hasattr(self, key) and key[0] == '_':
            self.logger.warning(f"Attribute '{key}' is private and cannot be changed")
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
            with tools.write_to_devnull():
                return convert_visibility_xds_to_visibility(self._input_data)
        except:
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
        if isinstance(self._input_data, Visibility):
            try:
                with tools.write_to_devnull():
                    return convert_visibility_to_visibility_xds(self._input_data)
            except:
                tools.reinstate_default_stdout()
                self.logger.critical(
                    "Could not convert SKA-Visibility to XRadio-Visibility\n  |"
                )
                log_handler.exit_pipeline(self.logger)
        return self._input_data

    @property
    def visibilities(self) -> NDArray:
        """
        Visibilities as NumPy arrays.

        Returns
        -------
        NumPy array enclosing visibilities.
        """
        try:
            return self._input_data[
                "vis" if isinstance(self._input_data, Visibility)
                else "VISIBILITY"
            ].values
        except:
            self.logger.critical("Could not read visibilities from MSv4\n  |")
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
            return self._input_data[
                "uvw" if isinstance(self._input_data, Visibility)
                else "UVW"
            ].values
        except:
            self.logger.critical("Could not read UVW data from MSv4\n  |")
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
            return self._input_data[
                "weight" if isinstance(self._input_data, Visibility)
                else "WEIGHT"
            ].values
        except:
            self.logger.critical("Could not read weights from MSv4\n  |")
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