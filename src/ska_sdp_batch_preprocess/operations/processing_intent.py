# see license in parent directory

from typing import Tuple

from numpy.typing import NDArray
from xarray.core.dataset import Dataset


class ProcessingIntent:
    """
    Class to represent a processing set of MSv4
    in memory.

    Attributes
    ----------
    data: xarray.Dataset
      XArray representation of the processing set data.

    visibilities: NDArray
      visibilities as NumPy arrays.

    uvw: NDArray
      uvw data as NumPy arrays

    weights: NDArray
      weights as NumPy arrays.

    channels: Tuple[float, float]
      base frequency and frequency increments.

    Methods
    -------
    manual_compute(**args)
      class method to generate an instance with
      the data manually loaded into memory as XArrays
      using compute() method.
    """

    def __init__(self, data: Dataset):
        """
        Initiates the ProcessingIntent class.

        Parameters
        ----------
        data: xarray.Dataset
          contains the processing set data.
        """
        self.data = data

    @property
    def visibilities(self) -> NDArray:
        """
        Visibilities as NumPy arrays.

        Returns
        -------
        NumPy array enclosing visibilities.
        """
        try:
            return self.data["VISIBILITY"].values
        except:
            raise RuntimeError(
                "could not load visibilities from this ProcessingIntent"
            )
        
    @property
    def uvw(self) -> NDArray:
        """
        UVW data as NumPy arrays.

        Returns
        -------
        NumPy array enclosing UVW data.
        """
        try:
            return self.data["UVW"].values
        except:
            raise RuntimeError(
                "could not load uvw from this ProcessingIntent"
            )
        
    @property
    def weights(self) -> NDArray:
        """
        Weights as NumPy arrays.

        Returns
        -------
        NumPy array enclosing weights.
        """
        try:
            return self.data["WEIGHT"].values
        except:
            raise RuntimeError(
                "could not load weights from this ProcessingIntent"
            )
    
    @property
    def channels(self) -> Tuple[float, float]:
        """
        Base frequency and frequency increments.

        Returns
        -------
        Tuple of base frequency and frequency increments.
        """
        try:
            chan_freq = self.data["frequency"].values.flatten()
            if len(chan_freq) == 1:
                return chan_freq[0], 0.
            return (chan_freq[0], chan_freq[1]-chan_freq[0])
        except:
            raise RuntimeError(
                "could not load frequency data from this ProcessingIntent"
            )

    @classmethod
    def manual_compute(cls, data: Dataset):
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
        return cls(data.compute())