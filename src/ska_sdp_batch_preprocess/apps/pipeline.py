import itertools
import sys
from argparse import (
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    ArgumentTypeError,
)
from collections import defaultdict
from pathlib import Path
from typing import Iterable

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


def assert_no_duplicate_input_names(paths: Iterable[Path]):
    """
    Given input paths, raise ValueError if any two paths have the same name,
    i.e. the same last component.

    We have to run this check on input MS paths, because two input MSes with
    different paths but identical names would correspond to the same output
    path.
    """
    name_to_full_path_mapping: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        name_to_full_path_mapping[path.name].append(str(path.resolve()))

    duplicate_paths = list(
        itertools.chain.from_iterable(
            path_list
            for path_list in name_to_full_path_mapping.values()
            if len(path_list) > 1
        )
    )

    if duplicate_paths:
        lines = [
            "There are duplicate input MS names. Offending paths: "
        ] + duplicate_paths
        raise ValueError("\n".join(lines))


def run_program(cli_args: list[str]):
    """
    Runs the batch preprocessing pipeline.
    """
    configure_logger()
    args = make_parser().parse_args(cli_args)
    assert_no_duplicate_input_names(args.input_ms)
    app = Application(
        args.config,
        args.output_dir,
        solutions_dir=args.solutions_dir,
        dask_scheduler=args.dask_scheduler,
    )
    app.process(args.input_ms)


def main():
    """
    Batch preprocessing pipeline app entrypoint.
    """
    run_program(sys.argv[1:])
