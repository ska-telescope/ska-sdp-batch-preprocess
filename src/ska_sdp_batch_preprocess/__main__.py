# see license in parent directory

import argparse
import yaml
from pathlib import Path

from operations import pipeline


def main() -> None:
    """
    """
    args = parse_args()
    pipeline.run(
        Path(args.msin), read_yaml(Path(args.config))
    )

def parse_args() -> argparse.Namespace:
    """
    """
    parser = argparse.ArgumentParser(
        description="Batch preprocessing pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--config",
        type=str,
        default=f"{Path.cwd().joinpath(
            "data", "config.yml"
        )}",
        help="input YAML configuration file"
    )
    parser.add_argument(
        "msin",
        type=str,
        help="measurement set (v2) directory"
    )
    return parser.parse_args()

def read_yaml(dir: Path) -> dict:
    """
    """
    try:
        with open(f"{dir}", 'r') as file:
            try:
                return yaml.safe_load(file)
            except yaml.YAMLError as e:
                raise e("YAML file could not be loaded")
    except FileNotFoundError as e:
        raise e("YAML file not found")


if __name__ == "__main__":
    main()