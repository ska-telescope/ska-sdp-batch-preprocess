import os
import shlex
import subprocess
from typing import Iterable, Optional

from ska_sdp_batch_preprocess.config import Step, parse_config_file

from .dp3_params import DP3Params
from .logging_setup import LOGGER


class Pipeline:
    """
    Sequence of operations to be run on Measurement Sets.
    """

    def __init__(self, steps: Iterable[Step]):
        """
        Initialise Pipeline given a sequence of Steps.
        """
        self._steps = tuple(steps)

    def run(self, msin: str | os.PathLike, msout: str | os.PathLike):
        """
        Run the pipeline on given input Measurement Set path `msin`, write
        the pre-processed output at path `msout`.
        """
        LOGGER.info(f"Processing: {msin!s}")

        command_line = DP3Params.create(
            self._steps, msin, msout
        ).to_command_line()
        LOGGER.info(shlex.join(command_line))

        subprocess.check_call(
            command_line, env=os.environ | {"OPENBLAS_NUM_THREADS": "1"}
        )
        LOGGER.info(f"Finished: {msin!s}")

    @classmethod
    def create(
        cls,
        config_path: str | os.PathLike,
        solutions_dir: Optional[str | os.PathLike] = None,
    ) -> "Pipeline":
        """
        Create a Pipeline, given a YAML config file path and an optional
        directory where the solution tables are stored.
        """
        return cls(parse_config_file(config_path, solutions_dir))
