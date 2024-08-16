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
        msin: Path, config: dict, *, logger: Logger
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
    ###### This needs changing ######
    #data = create_visibility_from_ms(str(msin))[0]
    if config is not None:
        ###### This needs changing ######
        #for process,val in config['processing_functions'].items():
        #    if val is None:
        #        data = eval(process)(data)
        #    else:
        #        for params in config['processing_functions'][process]:
        #            
        #            if isinstance(config['processing_functions'][process][params], list):
        #                
        #                x = numpy.array(config['processing_functions'][process][params]).astype(numpy.float32)
        #                config['processing_functions'][process][params] = x
        #        
        #        arguments = config['processing_functions'][process]
        #        
        #        data = eval(process) (data, **arguments)
        for func, args in config.items():
            if func.lower() == "convert_msv2_to_msv4":
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

            ###### Add processing functions here please ###### 
            elif func.lower() == "apply_rfi_masks":
                pass

            elif func.lower() == "averaging_frequency":
                pass

            elif func.lower() == "averaging_time":
                pass

            elif func.lower() == "ao_flagger":
                pass