# see license in parent directory

from numpy.typing import NDArray
from xarray.core.dataset import Dataset


class ProcessingIntent:
    """"""

    def __init__(self, data: Dataset):
        """
        """
        self.data = data

    @property
    def visibilities(self) -> NDArray:
        """
        """
        try:
            return self.data["VISIBILITY"].values
        except:
            raise RuntimeError(
                "could not load visibilities from this ProcessingIntent"
            )
        
    @property
    def uvw(self) -> NDArray:
        try:
            return self.data["UVW"].values
        except:
            raise RuntimeError(
                "could not load uvw from this ProcessingIntent"
            )

    @classmethod
    def manual_compute(cls, data: Dataset):
        """
        """
        return cls(data.compute())