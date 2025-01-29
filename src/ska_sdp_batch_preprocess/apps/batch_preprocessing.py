import sys
from argparse import (
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    ArgumentTypeError,
)
from pathlib import Path

from ska_sdp_batch_preprocess import __version__
from ska_sdp_batch_preprocess.application import Application
from ska_sdp_batch_preprocess.logging_setup import configure_logger


def make_parser() -> ArgumentParser:
    """
    Self-explanatory.
    """
    parser = ArgumentParser(
        description="SKA Batch pre-processing pipeline",
        formatter_class=ArgumentDefaultsHelpFormatter,
        add_help=False,
    )

    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "-c",
        "--config",
        type=Path,
        required=True,
        help="YAML configuration file",
    )
    required.add_argument(
        "-o",
        "--output-dir",
        type=existing_directory,
        required=True,
        help=("Output directory where the pre-processed data will be written"),
    )
    required.add_argument(
        "input_ms",
        type=existing_directory,
        nargs="+",
        help="Input measurement set(s)",
    )

    optional = parser.add_argument_group("optional arguments")
    optional.add_argument(
        "-s",
        "--solutions-dir",
        type=existing_directory,
        help=(
            "Directory containing solution tables (in H5Parm format). "
            "Any solution table paths in the config file ApplyCal "
            "steps that are not absolute will be preprended with this "
            "directory."
        ),
    )
    optional.add_argument(
        "--dask-scheduler",
        help=(
            "Network address of the dask scheduler to use for distribution; "
            "format is HOST:PORT"
        ),
    )
    optional.add_argument(
        "-h", "--help", action="help", help="show this help message and exit"
    )
    optional.add_argument("--version", action="version", version=__version__)
    return parser


def existing_directory(dirname: str) -> Path:
    """
    Validate CLI argument that must be an existing directory.
    """
    path = Path(dirname)
    if not path.is_dir():
        raise ArgumentTypeError(f"{dirname!r} must be an existing directory")
    return path


def run_program(cli_args: list[str]):
    """
    Runs the batch preprocessing pipeline.
    """
    configure_logger()
    args = make_parser().parse_args(cli_args)
    app = Application(
        args.config,
        args.output_dir,
        solutions_dir=args.solutions_dir,
    )
    if args.dask_scheduler:
        app.process_distributed(args.input_ms, args.dask_scheduler)
    else:
        app.process_sequentially(args.input_ms)


def main():
    """
    Batch preprocessing pipeline app entrypoint.
    """
    run_program(sys.argv[1:])
