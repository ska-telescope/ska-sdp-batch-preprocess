import argparse
import yaml
from pathlib import Path


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
        required=True,
        help="input YAML configuration file"
    )
    parser.add_argument(
        "ms",
        type=str,
        help="measurement set (V2) directory"
    )
    return parser.parse_args()

def main() -> None:
    """
    """
    args = parse_args()
    config = read_yaml(Path(args.config))

def read_yaml(dir: Path) -> dict:
    """
    """
    try:
        with open(F"{dir}", 'r') as file:
            try:
                return yaml.safe_load(file)
            except yaml.YAMLError as e:
                raise e("YAML file could not be loaded")
    except FileNotFoundError as e:
        raise e("YAML file not found")


if __name__ == "__main__":
    main()