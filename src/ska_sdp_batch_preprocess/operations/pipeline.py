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
            if func.lower() == "convert_msv2_to_msv4":
                MSv2 = MeasurementSet.ver_2(msin)
                MSv2.to_msv4(args)
            elif func.lower() == "load_msv4":
                MSv4 = MeasurementSet.ver_4(msin)
            elif func == "convert_msv2_to_msv4_then_load":
                MSv2 = MeasurementSet.ver_2(msin)
                MSv2.to_msv4(args)
                MSv4 = MeasurementSet.ver_4(
                    msin.with_suffix(".ms4")
                )