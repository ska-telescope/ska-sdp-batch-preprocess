# see license in parent directory

from logging import Logger
from pathlib import Path
from typing import Any

import numpy as np
from dask.distributed import Client

from ska_sdp_batch_preprocess.functions.distributed_func import Distribute
from ska_sdp_batch_preprocess.operations.measurement_set import (
    MeasurementSet,
    convert_msv2_to_msv4,
)
from ska_sdp_batch_preprocess.utils import log_handler, tools


def run(
    msin: Path, config: dict[str, Any], *, client: Client, logger: Logger
) -> None:
    """
    Principal function in the pipeline where the various
    functionalities are executed based on the YAML file
    instructions.

    Arguments
    ---------
    msin: pathlib.Path
      directory for the input MS (v2 or v4).

    config: dict[str, typing.Any] | None
      YAML configuration parameters read as Python
      dictionary.

    client: dask.distributed.Client
      DASK client for distribution.

    logger: logging.Logger
      logger object to handle pipeline logs.
    """
    if "processing_chain" in config:
        # Define chunking parameters
        axis = config["processing_chain"].setdefault("axis", "frequency")
        chunksize = config["processing_chain"].setdefault("chunksize", 10)
        logger.info(
            f"DASK distribution - Axis={axis} and Chunksize={chunksize} \n  |"
        )

        # load MS
        if "load_ms" in config["processing_chain"]:
            logger.info(f"Loading {msin.name} into memory")
            args = config["processing_chain"]["load_ms"]
            try:
                with tools.write_to_devnull():
                    ms = MeasurementSet.ver_2(msin, args, logger=logger)
                logger.info(f"Successfully loaded {msin.name} as MSv2\n  |")
            except:  # pylint: disable=bare-except
                tools.reinstate_default_stdout()
                try:
                    with log_handler.temporary_log_disable():
                        ms = MeasurementSet.ver_4(msin, args, logger=logger)
                    logger.info(
                        f"Successfully loaded {msin.name} as MSv4\n  |"
                    )
                except:  # pylint: disable=bare-except
                    log_handler.enable_logs_manually()
                    logger.critical(
                        f"Could not load {msin.name} as Msv2 or MSv4\n  |"
                    )
                    log_handler.exit_pipeline(logger)

            # Initialize distributor & run processing functions
            output_data_list = []
            for data in ms.dataframe:
                logger.info("Initializing distribution strategy")
                distributor = Distribute(
                    data.data_as_ska_vis, axis, chunksize, client
                )
                logger.info("Initialisation successful\n  |")
                processing_functions(
                    distributor, config["processing_chain"], logger=logger
                )
                logger.info("Running requested processing functions ...\n  |")
                output_data_list.append(distributor.vis)
                logger.info("Successfully ran all processing functions\n  |")

        # export to MSv2
        if "export_to_msv2" in config["processing_chain"]:
            msout = MeasurementSet.from_ska_vis(
                output_data_list, logger=logger
            )
            logger.info("Exporting list of processing intents to MSv2")
            args = config["processing_chain"]["export_to_msv2"]
            msout.export_to_msv2(
                msin.with_name(f"{msin.stem}-output.ms"), args
            )
            logger.info(f"{msin.stem}-output.ms generated successfully\n  |")

    # convert MSv2 to MSv4
    if "convert_msv2_to_msv4" in config:
        args = config["convert_msv2_to_msv4"]
        logger.info(f"Converting {msin.name} to MSv4")
        convert_msv2_to_msv4(msin, args, logger=logger)
        logger.info("Conversion successful\n  |")


def processing_functions(
    distributor: Distribute, config: dict[str, Any], *, logger: Logger
) -> None:
    """
    Chain of distributed processing functions, configured from YAML config.

    param: distributor: Initialised Distribute class that can chunk
                    visibilites and distribute over a DASK cluster
    param: config - Dictionary of configuration parameters read from YAML
    param: logger - logger class to store and print logs
    """
    for func, args in config.items():
        if args is None:
            args = {}
        if func == "apply_rfi_masks":
            logger.info("Applying RFI masks ...")
            masks = np.array(
                args.setdefault("rfi_frequency_masks", [1.3440e08, 1.3444e08]),
                dtype=np.float64,
            )
            distributor.rfi_masking(masks)
            logger.info("Apply RFI masks successful\n  |")

        elif func == "averaging_frequency":
            logger.info("Averaging in frequency ...")
            freqstep = args.setdefault("freqstep", 4)
            f_threshold = args.setdefault("flag_threshold", 0.5)
            distributor.avg_freq(freqstep, f_threshold)
            logger.info("Frequency averaging successful\n  |")

        elif func == "averaging_time":
            logger.info("Averaging in time ...")
            timestep = args.setdefault("timestep", 4)
            t_threshold = args.setdefault("flag_threshold", 0.5)
            distributor.avg_time(timestep, t_threshold)
            logger.info("Time averaging successful\n  |")

        elif func == "rfi_flagger":
            logger.info("Flagging ...")
            alpha = args.setdefault("alpha", 0.5)
            threshold_magnitude = args.setdefault("magnitude", 3.5)
            threshold_variation = args.setdefault("variation", 3.5)
            threshold_broadband = args.setdefault("broadband", 3.5)
            sampling = args.setdefault("sampling", 8)
            window = args.setdefault("window", 0)
            window_median_history = args.setdefault("median_history", 10)
            distributor.flagger(
                alpha=alpha,
                threshold_magnitude=threshold_magnitude,
                threshold_variation=threshold_variation,
                threshold_broadband=threshold_broadband,
                sampling=sampling,
                window=window,
                window_median_history=window_median_history,
            )
            logger.info("RFI Flagging successful \n |")
