import sys
import warnings
from argparse import (
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    ArgumentTypeError,
)
from pathlib import Path

from ska_sdp_batch_preprocess import __version__
from ska_sdp_batch_preprocess.logging_setup import configure_logger
from ska_sdp_batch_preprocess.pipeline import Pipeline


def make_parser() -> ArgumentParser:
    """
    Self-explanatory.
    """
    parser = ArgumentParser(
        description="SKA Batch pre-processing pipeline",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        required=True,
        help="YAML configuration file",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=existing_directory,
        required=True,
        help=("Output directory where the pre-processed data will be written"),
    )
    parser.add_argument(
        "--dask-scheduler",
        help=(
            "Network address of the dask scheduler to use for distribution; "
            "format is HOST:PORT"
        ),
    )
    parser.add_argument(
        "input_ms",
        type=existing_directory,
        nargs="+",
        help="Input measurement set(s)",
    )
    return parser


def existing_directory(dirname: str) -> Path:
    """
    Validate CLI argument that must be an existing directory.
    """
    path = Path(dirname)
    if not (path.exists() and path.is_dir()):
        raise ArgumentTypeError(f"{dirname!r} must be an existing directory")
    return path


def run_program(cli_args: list[str]):
    """
    Runs the batch preprocessing pipeline.
    """
    configure_logger()
    parser = make_parser()
    args = parser.parse_args(cli_args)

    if args.dask_scheduler is not None:
        warnings.warn(
            "Dask distribution is not implemented yet, "
            "ignoring --dask-scheduler argument"
        )

    input_ms_list: list[Path] = args.input_ms
    output_dir: Path = args.output_dir

    pipeline = Pipeline.from_yaml(args.config)

    for input_ms in input_ms_list:
        pipeline.run(input_ms, output_dir / input_ms.name)


def main():
    """
    Batch preprocessing pipeline app entrypoint.
    """
    run_program(sys.argv[1:])
