import logging
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
        solutions_dir: Optional[Path] = None,
        dask_scheduler: Optional[str] = None,
    ):
        """
        Create a new Application.
        """
        # All paths must be made absolute before being sent to dask workers,
        # because they may operate from a different working directory.
        self._output_dir = output_dir.resolve()
        self._dask_scheduler = dask_scheduler
        solutions_dir = solutions_dir.resolve() if solutions_dir else None
        self._pipeline = Pipeline.create(config_file, solutions_dir)

    def process(self, input_mses: Iterable[Path]):
        """
        Process the given MSv2 paths.
        """
        input_mses = map(Path.resolve, input_mses)
        if self._dask_scheduler:
            self._process_distributed(input_mses)
        else:
            self._process_sequentially(input_mses)

    def _process_sequentially(self, input_mses: Iterable[Path]):
        for input_ms in input_mses:
            self._pipeline.run(input_ms, self._output_dir / input_ms.name)

    def _process_distributed(self, input_mses: Iterable[Path]):
        client = dask.distributed.Client(self._dask_scheduler, timeout=5.0)
        client.forward_logging(LOGGER.name, level=logging.DEBUG)
        futures = client.compute(
            [
                dask.delayed(self._process_ms_on_dask_worker)(input_ms)
                for input_ms in input_mses
            ]
        )
        dask.distributed.wait(futures)

    def _process_ms_on_dask_worker(self, input_ms: Path):
        LOGGER.setLevel(logging.DEBUG)
        worker = dask.distributed.get_worker()
        self._pipeline.run(
            input_ms,
            self._output_dir / input_ms.name,
            numthreads=worker.state.nthreads,
        )
