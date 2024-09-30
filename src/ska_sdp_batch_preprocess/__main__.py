# see license in parent directory

import argparse
import yaml
from logging import Logger
from pathlib import Path
from typing import Any

from dask.distributed import Client, LocalCluster

from ska_sdp_batch_preprocess.config.validate_config import (
    validate_config
)
from ska_sdp_batch_preprocess.operations import pipeline
from ska_sdp_batch_preprocess.utils import log_handler


def main() -> None:
    """
    Pipeline entry point.
    """
    args = parse_args()
    logger = log_handler.generate("Batch Preprocess", cmd_logs=args.cmd_logs)

    yaml_dict = read_yaml(Path(args.config), logger=logger)

    if args.scheduler:
        logger.info(f"DASK distribution - utilizing the cluster provided: {args.scheduler}\n  |")
        client = Client(args.scheduler, timeout=3500)

    else:
        logger.info("DASK distribution - utilizing the local cluster\n  |")
        client = Client(LocalCluster())

    logger.info("Pipeline running\n  |")
    pipeline.run(
        Path(args.msin), yaml_dict, client=client, logger=logger
    )
    log_handler.exit_pipeline(logger, success=True)

def parse_args() -> argparse.Namespace:
    """
    Parses command line arguments.

    cmd Arguments
    -------------
    msin: str
      directory for the input measurement set (v2 or v4).
    
    --config (optional): str 
      directory for the YAML configuration file,
      (default /ska-sdp-batch-preprocess/config/config_default.yml).

    --cmd_logs (optional): None
      raising this flag will prompt the pipeline to output
      logs on the command line in addition to logging into a
      logfile.

    --scheduler (optional): str
      address of a DASK scheduler to use for distribution
      (if not called, the local cluster will be utilised).

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
        "msin",
        type=str,
        help="Measurement set (v2 or v4) directory"
    )
    parser.add_argument(
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
        "--scheduler",
        type=str, 
        help="Address of a DASK scheduler to use for distribution"
    )

    return parser.parse_args()

def read_yaml(dir: Path, *, logger: Logger) -> dict[str, Any]:
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
    logger.info(f"Loading {dir.name} into memory")
    try:
        with open(f"{dir}", 'r') as file:
            config = yaml.safe_load(file)
    except:
        logger.critical(f"Loading {dir.name} failed")
        log_handler.exit_pipeline(logger)

    logger.info("Validating loaded YAML object")
    try:
        validate_config(config)
    except:
        logger.critical("Invalid YAML format\n  |")
        log_handler.exit_pipeline(logger)

    logger.info("Success\n  |")
    return config


if __name__ == "__main__":
    main()