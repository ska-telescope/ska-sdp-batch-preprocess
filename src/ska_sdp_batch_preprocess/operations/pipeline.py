# see license in parent directory

from logging import Logger
from pathlib import Path

from ska_sdp_batch_preprocess.operations.measurement_set import (
    convert_msv2_to_msv4, MeasurementSet
)
from ska_sdp_batch_preprocess.utils import (
    log_handler, tools
)
from ska_sdp_func_python.preprocessing import (
    apply_rfi_masks, averaging_frequency, 
    averaging_time
)


def run(
        msin: Path, config: dict, client: Client, *, logger: Logger
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
        if 'convert_msv2_to_msv4' in config:
            args = config['convert_msv2_to_msv4']
            logger.info(f"Converting {msin.name} to MSv4")
            convert_msv2_to_msv4(msin, args, logger=logger)
            logger.info("Conversion successful\n  |")

            elif func.lower() == "load_ms":
                logger.info(f"Loading {msin.name} into memory")
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

            elif func.lower() == "export_to_msv2":
                logger.info("Exporting list of processing intents to MSv2")
                ms.export_to_msv2(msin.with_name(f"{msin.stem}-output.ms"))
                logger.info(f"{msin.stem}-output.ms generated successfully")
                


def processing_functions(
     ms: xr.Dataset, config: dict, client: Client, *, logger: Logger
) -> None:
        """
        Chain of distributed processing functions that can be configured from the YAML config file.

        param: ms - Measurement Set data represented by the Measurement Set class 
        param: config - Dictionary of configuration parameters read from a YAML file
        param: client - Dask Client provided by a scheduler or a local cluster
        param: logger - logger class to store and print logs
        """
        
        if config is not None:
            for func in config.keys():
                if func == "apply_rfi_masks":
                    logger.info("Applying rfi masks ...")
                    masks = config['apply_rfi_masks']['rfi_frequency_masks']
                    f_chunk = config['apply_rfi_masks']['f_chunk']
                    ms = distribute_rfi_masking(ms, masks, f_chunk, client)
    
                elif func == "averaging_frequency":
                    logger.info("Averaging in frequency ...")
                    freqstep = config['averaging_frequency']['freqstep']
                    f_threshold = config['averaging_frequency']['flag_threshold']
                    f_chunk = config['averaging_frequency']['f_chunk']
                    ms = distribute_averaging_freq(ms, freqstep, f_chunk, client, f_threshold)

                elif func == "averaging_time":
                    logger.info("Averaging in time ...")
                    timestep = config['averaging_time']['timestep']
                    t_threshold = config['averaging_time']['flag_threshold']
                    t_chunk = config['averaging_time']['t_chunk']
                    ms = distribute_averaging_time(ms, timestep, t_chunk, client, t_threshold)

                elif func == "rfi_flagger":
                    logger.info("Flagging ...")
                    pass


