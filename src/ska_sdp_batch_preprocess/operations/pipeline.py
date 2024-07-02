# see license in parent directory

from pathlib import Path

from operations.ms import MeasurementSet


def run(
        msin: Path, config: dict
) -> None:
    """
    """
    MSv2 = MeasurementSet(msin)
    MSv2.to_processing_set()