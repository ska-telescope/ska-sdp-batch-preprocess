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
        except KeyError as e:
            raise e(
                "expected a 'VISIBILITY' column in this ProcessingIntent"
            )
        
    @property
    def uvw(self) -> NDArray:
        try:
            return self.data["UVW"].values
        except KeyError as e:
            raise e(
                "expected a 'UVW' column in this ProcessingIntent"
            )

    @classmethod
    def manual_compute(cls, data: Dataset):
        """
        """
        return cls(data.compute())