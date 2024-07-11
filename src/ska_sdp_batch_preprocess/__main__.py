# see license in parent directory

import argparse
import yaml
from pathlib import Path

from operations import pipeline


def main() -> None:
    """
    Pipeline entry point.
    """
    args = parse_args()
    pipeline.run(
        Path(args.msin), read_yaml(Path(args.config))
    )

def parse_args() -> argparse.Namespace:
    """
    Parses command line arguments.

    cmd Arguments
    -------------
    --config (optional): str 
      directory for the YAML configuration file,
      (default /ska-sdp-batch-preprocess/data/config.yml).
    
    msin: str
      directory for the input measurement set (v2 or v4).
    """
    parser = argparse.ArgumentParser(
        description="Batch preprocessing pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--config",
        type=str,
        default=f"{Path.cwd().joinpath('data', 'config.yml')}",
        help="input YAML configuration file"
    )
    parser.add_argument(
        "msin",
        type=str,
        help="measurement set (v2 or v4) directory"
    )
    return parser.parse_args()

def read_yaml(dir: Path) -> dict:
    """
    Reads YAML configuration file as a dictionary.
    Raises errors where the load fails or the file does not exist.
    """
    with open(f"{dir}", 'r') as file:
        return yaml.safe_load(file)


if __name__ == "__main__":
    main()