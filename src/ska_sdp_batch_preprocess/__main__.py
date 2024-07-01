import argparse

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
    return parser.parse_args()

def main() -> None:
    """
    """
    args = parse_args()


if __name__ == "__main__":
    raise NotImplementedError(
        "pipeline not implemented"
    )