# see license in parent directory

import argparse
import yaml
from logging import Logger
from pathlib import Path

from ska_sdp_batch_preprocess.operations import pipeline
from ska_sdp_batch_preprocess.utils import log_handler


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

    logger.info("Entering pipeline\n  |")
    pipeline.run(
        Path(args.msin), yaml_dict,
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
        "--config",
        type=str,
        default=f"{Path.cwd().joinpath('config', 'config_default.yml')}",
        help="input YAML configuration file"
    )
    parser.add_argument(
        "--cmd_logs",
        action="store_true",
        help="generate logs on the command line"
    )
    parser.add_argument(
        "msin",
        type=str,
        help="measurement set (v2 or v4) directory"
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