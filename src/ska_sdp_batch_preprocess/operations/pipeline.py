# see license in parent directory

from logging import Logger
from pathlib import Path
from typing import Optional

import numpy as np
from dask.distributed import Client

from ska_sdp_batch_preprocess.functions.distributed_func import (
    distribute_averaging_time, distribute_rfi_flagger, 
    distribute_rfi_masking, distribute_averaging_freq,
)
from ska_sdp_batch_preprocess.operations.measurement_set import (
    convert_msv2_to_msv4, MeasurementSet
)
from ska_sdp_batch_preprocess.operations.processing_intent import (
    ProcessingIntent
)
from ska_sdp_batch_preprocess.utils import log_handler, tools


def run(
        msin: Path, config: Optional[dict],client: Client, *, logger: Logger
) -> None:
    """
    Principal function in the pipeline where the various
    functionalities are executed based on the YAML file
    instructions.

    Arguments
    ---------
    msin: pathlib.Path
      directory for the input MS (v2 or v4).

    config: dict
      YAML configuration parameters read as Python 
      dictionary.

    logger: logging.Logger
      logger object to handle pipeline logs.
    """
    if config is not None:
        
        if "processing_chain" in config:
            # load MS
            if "load_ms" in config["processing_chain"]:
                logger.info(f"Loading {msin.name} into memory")
                args = config["processing_chain"]["load_ms"]
                try:
                    with tools.write_to_devnull():
                        ms = MeasurementSet.ver_2(msin, args, logger=logger)
                    logger.info(f"Successfully loaded {msin.name} as MSv2\n  |")
                except:
                    tools.reinstate_default_stdout()
                    try:
                        with log_handler.temporary_log_disable():
                            ms = MeasurementSet.ver_4(msin, args, logger=logger)
                        logger.info(f"Successfully loaded {msin.name} as MSv4\n  |")
                    except:
                        log_handler.enable_logs_manually()
                        logger.critical(f"Could not load {msin.name} as either Msv2 or MSv4\n  |")
                        log_handler.exit_pipeline(logger)

                # Run processing functions
                logger.info("Running requested processing functions ...")
                processing_functions(ms.dataframe, config["processing_chain"], client, logger=logger)
                logger.info(f"Successfully ran all processing functions\n  |")

            # export to MSv2
            if "export_to_msv2" in config["processing_chain"]:
                logger.info("Exporting list of processing intents to MSv2")
                args = config["processing_chain"]["export_to_msv2"]
                ms.export_to_msv2(msin.with_name(f"{msin.stem}-output.ms"), args)
                logger.info(f"{msin.stem}-output.ms generated successfully\n  |")

        # convert MSv2 to MSv4
        if 'convert_msv2_to_msv4' in config:
            args = config['convert_msv2_to_msv4']
            logger.info(f"Converting {msin.name} to MSv4")
            convert_msv2_to_msv4(msin, args, logger=logger)
            logger.info("Conversion successful\n  |")

def processing_functions(
     ms: list[ProcessingIntent], config: dict, client: Client, *, logger: Logger
) -> None:
        """
        Chain of distributed processing functions that can be configured from the YAML config file.

        param: ms - Measurement Set data represented by the Measurement Set class 
        param: config - Dictionary of configuration parameters read from a YAML file
        param: client - Dask Client provided by a scheduler or a local cluster
        param: logger - logger class to store and print logs
        """
        
        for func, args in config.items():
            if func == "apply_rfi_masks":
                logger.info("Applying rfi masks ...")
                masks = np.array(args.get('rfi_frequency_masks'), dtype=np.float64)
                f_chunk = args.get('f_chunk', 4)
                for data in ms:
                    data = distribute_rfi_masking(data.data_as_ska_vis, masks, f_chunk, client)
                logger.info("Apply rfi masks successful\n  |")

            elif func == "averaging_frequency":
                logger.info("Averaging in frequency ...")
                freqstep = args.get('freqstep', 4)
                f_threshold = args.get('flag_threshold', 0.5)
                f_chunk = args.get('f_chunk', 10)
                for data in ms:
                    data = distribute_averaging_freq(data.data_as_ska_vis, freqstep, f_chunk, client, f_threshold)
                logger.info("Frequency averaging successful\n  |")

            elif func == "averaging_time":
                logger.info("Averaging in time ...")
                timestep = args.get('timestep', 4)
                t_threshold = args.get('flag_threshold', 0.5)
                t_chunk = args.get('t_chunk', 10)
                for data in ms:
                    data = distribute_averaging_time(data.data_as_ska_vis, timestep, t_chunk, client, t_threshold)
                logger.info("Time averaging successful\n  |")

            elif func == "rfi_flagger":
                logger.info("Flagging ...")
                t_chunk=args.get('t_chunk', 4)
                alpha=args.get('alpha', 0.5)
                threshold_magnitude=args.get('magnitude', 3.5)
                threshold_variation=args.get('variation', 3.5)
                threshold_broadband=args.get('broadband', 3.5)
                sampling=args.get('sampling', 8)
                window=args.get('window', 0)
                window_median_history= args.get('median_history', 10)
                for data in ms:
                    distribute_rfi_flagger(data.data_as_ska_vis, t_chunk, client, alpha=alpha, threshold_magnitude=threshold_magnitude, 
                                           threshold_variation=threshold_variation, threshold_broadband=threshold_broadband, 
                                           sampling=sampling, window=window, window_median_history=window_median_history)
                logger.info("RFI Flagging successful \n |")