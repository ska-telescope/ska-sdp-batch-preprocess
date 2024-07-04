# see license in parent directory

from pathlib import Path

from operations.ms import MeasurementSet


def run(
        msin: Path, config: dict
) -> None:
    """
    """
    if config is not None:
        for func, args in config.items():
            if func.lower() == "convert_msv2_to_ps":
                MSv2 = MeasurementSet.v2(msin)
                MSv2.to_msv4(args)