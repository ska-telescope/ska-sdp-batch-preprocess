# see license in parent directory

from pathlib import Path
from typing import Optional

from xradio.vis import(
    read_processing_set
)
from xradio.vis._processing_set import (
    processing_set
)


class ProcessingSet:
    """"""

    def __init__(self, data: processing_set):
        """
        """
        self.data = data

    @classmethod
    def load_lazy(
            cls, dir: Path, *, 
            args: Optional[dict]=None
    ):
        """
        """
        if args is None:
            return cls(
                read_processing_set(f"{dir}")
            )
        return cls(
            read_processing_set(f"{dir}", **args)
        )