import argparse
import sys
from pathlib import Path

from ska_sdp_batch_preprocess import __version__


def make_parser() -> argparse.ArgumentParser:
    """
    Self-explanatory.
    """
    parser = argparse.ArgumentParser(
        description="SKA Batch pre-processing pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory where the pre-processed data will be written",
    )
    parser.add_argument(
        "input_ms",
        type=Path,
        nargs="+",
        help="Input measurement set(s)",
    )
    return parser


def run_program(cli_args: list[str]):
    """
    Runs the batch preprocessing pipeline.
    """
    parser = make_parser()
    args = parser.parse_args(cli_args)
    print(args)


def main():
    """
    Batch preprocessing pipeline app entrypoint.
    """
    run_program(sys.argv[1:])
