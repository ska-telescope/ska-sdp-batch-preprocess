# see license in parent directory

from xarray.core.dataset import Dataset


class ProcessingIntent:
    """"""

    def __init__(self, data: Dataset):
        """
        """
        self.data = data

    @property
    def computed_data(self) -> Dataset:
        """
        """
        return self.data.compute()