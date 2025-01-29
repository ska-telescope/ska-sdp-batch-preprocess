import itertools
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional

import dask
import dask.distributed

from ska_sdp_batch_preprocess.logging_setup import LOGGER
from ska_sdp_batch_preprocess.pipeline import Pipeline

SUBPROCESS_RESOURCE = "subprocess"
"""
Name of the dask resource used to limit the number of DP3 subprocesses
run simultaneously by dask workers.
"""


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
        solutions_dir: Optional[Path] = None,
    ):
        """
        Create a new Application.
        """
        # All paths must be made absolute before being sent to dask workers,
        # because they may operate from a different working directory.
        self._output_dir = output_dir.resolve()
        solutions_dir = solutions_dir.resolve() if solutions_dir else None
        self._pipeline = Pipeline.create(config_file, solutions_dir)

    def process(
        self, input_mses: Iterable[Path], dask_scheduler: Optional[str] = None
    ):
        """
        Process a list of measurement sets, sequentially or using a dask
        cluster if the network address of its scheduler is given via
        the `dask_scheduler` argument.
        """
        assert_no_duplicate_input_names(input_mses)
        input_mses = map(Path.resolve, input_mses)

        if dask_scheduler:
            self._process_distributed(input_mses, dask_scheduler)
        else:
            self._process_sequentially(input_mses)

    def _process_sequentially(self, input_mses: Iterable[Path]):
        for input_ms in input_mses:
            self._pipeline.run(input_ms, self._output_dir / input_ms.name)

    def _process_distributed(
        self, input_mses: Iterable[Path], dask_scheduler: str
    ):
        client = dask.distributed.Client(dask_scheduler, timeout=5.0)
        _assert_at_least_one_worker_with_subprocess_resource(client)
        client.forward_logging(LOGGER.name, level=logging.DEBUG)

        with dask.annotate(resources={SUBPROCESS_RESOURCE: 1}):
            delayed_list = [
                dask.delayed(self._process_ms_on_dask_worker)(input_ms)
                for input_ms in input_mses
            ]
        futures = client.compute(delayed_list)
        dask.distributed.wait(futures)

    def _process_ms_on_dask_worker(self, input_ms: Path):
        LOGGER.setLevel(logging.DEBUG)
        worker = dask.distributed.get_worker()
        self._pipeline.run(
            input_ms,
            self._output_dir / input_ms.name,
            numthreads=worker.state.nthreads,
        )


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


def _assert_at_least_one_worker_with_subprocess_resource(
    client: dask.distributed.Client,
):
    def _usable_worker(worker: dict[str, Any]) -> bool:
        resources: dict[str, float] = worker["resources"]
        return resources.get(SUBPROCESS_RESOURCE, 0) >= 1

    workers_dict: dict[str, dict] = client.scheduler_info()["workers"]
    if not any(map(_usable_worker, workers_dict.values())):
        raise RuntimeError(
            f"Found no workers with resource {SUBPROCESS_RESOURCE!r} >= 1"
        )
