# see license in parent directory

import argparse
import yaml
from logging import Logger
from pathlib import Path

from operations import pipeline
from utils import log_handler
from dask.distributed import Client, LocalCluster

def main() -> None:
    """
    Pipeline entry point.
    """
    args = parse_args()
    logger = log_handler.generate(
        "Batch Preprocess", cmd_logs=args.cmd_logs
    )

    logger.info(
        f"Loading {Path(args.config).name} into memory"
    )
    yaml_dict = read_yaml(Path(args.config), logger=logger)
    logger.info(
        f"Load successful\n  |" 
    )

    if(args.scheduler):
        logger.info(f"Utilizing the cluster provided: {args.scheduler}")
        client = Client(args.scheduler)
    else:
        logger.info("Utilizing the local cluster")
        cluster = LocalCluster
        client = Client(cluster)

    if(args.time_chunksize):
        logger.info(f"Setting time chunk size to {args.time_chunksize}")
        t_chunksize = args.time_chunksize
    else:
        t_chunksize = 4

    if(args.frequency_chunksize):
        logger.info(f"Setting frequency chunk size to {args.frequency_chunksize}")
        f_chunksize = args.frequency_chunksize
    else:
        f_chunksize = 6   

    logger.info("Pipeline running\n  |")
    pipeline.run(
        Path(args.msin), yaml_dict, client, t_chunksize, f_chunksize,
        logger=logger
    )
    log_handler.exit_pipeline(logger, success=True)

def parse_args() -> argparse.Namespace:
    """
    Parses command line arguments.

    cmd Arguments
    -------------
    --config (optional): str 
      directory for the YAML configuration file,
      (default /ska-sdp-batch-preprocess/config/config_default.yml).

    --cmd_logs (optional): None
      raising this flag will prompt the pipeline to output
      logs on the command line in addition to logging into a
      logfile.
    
    msin: str
      directory for the input measurement set (v2 or v4).

    Returns
    -------
    argparse.Namespace class instance enclosing the parsed
    cmd arguments.
    """
    parser = argparse.ArgumentParser(
        description="Batch preprocessing pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-c"
        "--config",
        type=str,
        default=f"{Path.cwd().joinpath('config', 'config_default.yml')}",
        help="Input YAML configuration file"
    )
    parser.add_argument(
        "--cmd_logs",
        action="store_true",
        help="Generates detailed logs on the command line"
    )
    parser.add_argument(
        "-ms"
        "--msin",
        type=str,
        help="Measurement set (v2 or v4) directory"
    )

    dask_args = parser.add_argument_group("dask")
    dask_args.add_argument(
        "-s",
        "--scheduler",
        type=str, 
        help="Address of a dask scheduler to use for distribution"
    )
    dask_args.add_argument(
        "-t",
        "--time_chunksize",
        type=int, 
        help="Set chunksize for the time distributed functions"
    )
    dask_args.add_argument(
        "-f", 
        "--frequency_chunksize",
        type=int, 
        help="Set chunksize for the frequency distributed functions"
    )
    return parser.parse_args()

def read_yaml(dir: Path, *, logger: Logger) -> dict:
    """
    Reads YAML configuration file as a dictionary.
    No custom format checks as of yet.

    Arguments
    ---------
    dir: pathlib.Path
      directory for the YAML configuration file.

    Returns
    -------
    Python dictionary enclosing the YAML configurations.
    """
    try:
        with open(f"{dir}", 'r') as file:
            return yaml.safe_load(file)
    except:
        logger.critical(f"Loading {dir.name} failed")
        log_handler.exit_pipeline(logger)


if __name__ == "__main__":
    main()