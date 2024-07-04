# see license in parent directory

from xarray.core.dataset import Dataset


class ProcessingIntent:
    """"""

    def __init__(self, data: Dataset):
        """
        """
        self.data = data

    @classmethod
    def manual_compute(cls, data: Dataset):
        """
        """
        return cls(data.compute())