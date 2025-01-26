import logging
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

    def process_sequentially(self, input_mses: Iterable[Path]):
        """
        Process MeasurementSets sequentially.
        """
        for input_ms in map(Path.resolve, input_mses):
            self._pipeline.run(input_ms, self._output_dir / input_ms.name)

    def process_distributed(
        self, input_mses: Iterable[Path], dask_scheduler: str
    ):
        """
        Process MeasurementSets in parallel on a dask cluster, given its
        scheduler network address.
        """
        client = dask.distributed.Client(dask_scheduler, timeout=5.0)
        _assert_at_least_one_worker_with_subprocess_resource(client)
        client.forward_logging(LOGGER.name, level=logging.DEBUG)

        with dask.annotate(resources={SUBPROCESS_RESOURCE: 1}):
            delayed_list = [
                dask.delayed(self._process_ms_on_dask_worker)(input_ms)
                for input_ms in map(Path.resolve, input_mses)
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
