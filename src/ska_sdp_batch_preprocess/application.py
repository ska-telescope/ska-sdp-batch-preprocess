import itertools
import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional

import dask
import dask.distributed

from ska_sdp_batch_preprocess.logging_setup import LOGGER
from ska_sdp_batch_preprocess.pipeline import Pipeline


# pylint:disable=too-few-public-methods
class Application:
    """
    Main application class. Applies the same pipeline (i.e. sequence of steps)
    to all inputs.
    """

    def __init__(
        self,
        config_file: Path,
        output_dir: Path,
        *,
        extra_inputs_dir: Optional[Path] = None,
    ):
        """
        Create a new Application.
        """
        # All paths must be made absolute before being sent to dask workers,
        # because they may operate from a different working directory.
        self._output_dir = output_dir.resolve()
        extra_inputs_dir = (
            extra_inputs_dir.resolve() if extra_inputs_dir else None
        )
        self._pipeline = Pipeline.create(config_file, extra_inputs_dir)

    def process(
        self,
        measurement_sets: Iterable[Path],
        dask_scheduler: Optional[str] = None,
    ):
        """
        Process a list of measurement sets, sequentially or using a dask
        cluster if the network address of its scheduler is given via
        the `dask_scheduler` argument.
        """
        assert_no_duplicate_input_names(measurement_sets)
        measurement_sets = map(Path.resolve, measurement_sets)

        if dask_scheduler:
            self._process_distributed(measurement_sets, dask_scheduler)
        else:
            self._process_sequentially(measurement_sets)

    def _process_sequentially(self, absolute_ms_paths: Iterable[Path]):
        for mset in absolute_ms_paths:
            self._pipeline.run(mset, self._output_dir / mset.name)

    def _process_distributed(
        self, absolute_ms_paths: Iterable[Path], dask_scheduler: str
    ):
        client = dask.distributed.Client(dask_scheduler, timeout=5.0)
        client.forward_logging(LOGGER.name, level=logging.DEBUG)

        with dask.annotate(resources={"process": 1}):
            tasks = [
                dask.delayed(process_ms_on_dask_worker)(
                    self._pipeline, self._output_dir, mset
                )
                for mset in absolute_ms_paths
            ]

        futures = client.compute(tasks)
        dask.distributed.wait(futures)
        client.close()


def process_ms_on_dask_worker(
    pipeline: Pipeline, output_dir: Path, mset: Path
):
    """
    Self-explanatory. All Path arguments must be absolute.
    """
    # Ensure all logs created on the worker are forwarded to the client
    LOGGER.setLevel(logging.DEBUG)
    worker = dask.distributed.get_worker()
    LOGGER.info(f"Processing {mset!s} on worker {worker.address!s}")
    numthreads = worker.state.nthreads
    pipeline.run(mset, output_dir / mset.name, numthreads=numthreads)


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
