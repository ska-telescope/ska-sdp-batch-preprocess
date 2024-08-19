# see license in parent directory

from logging import Logger
from pathlib import Path
from operations.processing_intent import ProcessingIntent
from ska_sdp_batch_preprocess.operations.measurement_set import (
    convert_msv2_to_msv4, MeasurementSet
)
from ska_sdp_batch_preprocess.utils import (
    log_handler, tools
)
from functions.distributed_func import (
    distribute_averaging_time, distribute_rfi_flagger, 
    distribute_rfi_masking, distribute_averaging_freq,
)
from dask.distributed import (
    Client, performance_report, 
    get_task_stream
)
import numpy as np



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
        
        if 'load_ms' in config:
            args = config['load_ms']
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
            
            logger.info("Processing measurement set intents ...")
            try: 
                with tools.write_to_devnull():
                    processing_functions(ms.dataframe, config, client, logger=logger)
            except:
                tools.reinstate_default_stdout()
        
        if 'export_to_msv2' in config:
            logger.info("Exporting list of processing intents to MSv2")
            ms.export_to_msv2(msin.with_name(f"{msin.stem}-output.ms"))
            logger.info(f"{msin.stem}-output.ms generated successfully")

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
        
        for func in config.keys():
            if func == "apply_rfi_masks":
                logger.info("Applying rfi masks ...")
                masks = np.array(config['apply_rfi_masks']['rfi_frequency_masks'], dtype=np.float64)
                print(f" \n Masks shape = {masks.shape} \n")
                f_chunk = config['apply_rfi_masks']['f_chunk']
                for data in ms:
                    data = distribute_rfi_masking(data._input_data, masks, f_chunk, client)

            elif func == "averaging_frequency":
                logger.info("Averaging in frequency ...")
                freqstep = config['averaging_frequency']['freqstep']
                f_threshold = config['averaging_frequency']['flag_threshold']
                f_chunk = config['averaging_frequency']['f_chunk']
                for data in ms:
                    data = distribute_averaging_freq(data._input_data, freqstep, f_chunk, client, f_threshold)

            elif func == "averaging_time":
                logger.info("Averaging in time ...")
                timestep = config['averaging_time']['timestep']
                t_threshold = config['averaging_time']['flag_threshold']
                t_chunk = config['averaging_time']['t_chunk']
                for data in ms:
                    data = distribute_averaging_time(data._input_data, timestep, t_chunk, client, t_threshold)

            elif func == "rfi_flagger":
                logger.info("Flagging ...")
                pass


