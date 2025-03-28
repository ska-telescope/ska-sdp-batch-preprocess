import os
import shlex
import subprocess
from typing import Iterable, Optional

from ..config import Step, parse_config_file
from ..logging_setup import LOGGER
from .dp3_params import DP3Params
from .step_preparation import prepare_steps


class Pipeline:
    """
    Sequence of operations to be run on Measurement Sets.
    """

    def __init__(
        self,
        steps: Iterable[Step],
        extra_inputs_dir: Optional[str | os.PathLike] = None,
    ):
        """
        Initialise Pipeline given a sequence of Steps. `extra_inputs_dir` is an
        optional directory path where additional input files mentioned in the
        config are expected to be stored. Any path to e.g. a solution table in
        the config that is not absolute will be prepended with
        `extra_inputs_dir`.
        """
        self._prepared_steps = tuple(prepare_steps(steps, extra_inputs_dir))

    def run(
        self,
        msin: str | os.PathLike,
        msout: str | os.PathLike,
        *,
        numthreads: Optional[int] = None,
    ):
        """
        Run the pipeline on given input Measurement Set path `msin`, write
        the pre-processed output at path `msout`. If not specified,
        `numthreads` defaults to the total number of threads allocated to the
        current process.
        """
        LOGGER.info(f"Processing: {msin!s}")
        params = DP3Params.create(
            self._prepared_steps, msin, msout, numthreads=numthreads
        )
        command_line = params.to_command_line()
        LOGGER.info(shlex.join(command_line))

        subprocess.check_call(
            command_line, env=os.environ | {"OPENBLAS_NUM_THREADS": "1"}
        )
        LOGGER.info(f"Finished: {msin!s}")

    @classmethod
    def create(
        cls,
        config_path: str | os.PathLike,
        extra_inputs_dir: Optional[str | os.PathLike] = None,
    ) -> "Pipeline":
        """
        Create a Pipeline, given a YAML config file path and an optional
        directory where the solution tables are stored.
        """
        return cls(parse_config_file(config_path), extra_inputs_dir)
