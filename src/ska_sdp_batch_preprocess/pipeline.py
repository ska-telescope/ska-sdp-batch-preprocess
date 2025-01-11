import os
import shlex
import subprocess

from ska_sdp_batch_preprocess.config import (
    NamedStep,
    parse_and_validate_config_file,
)

from .dp3_config import DP3Config
from .logging_setup import LOGGER


class Pipeline:
    """
    Sequence of operations to be run on Measurement Sets.
    """

    def __init__(self, named_steps: list[NamedStep]):
        """
        Initialise Pipeline given a list of NamedSteps.
        """
        self.named_steps = named_steps

    def run(self, msin: str | os.PathLike, msout: str | os.PathLike):
        """
        Run the pipeline on given input Measurement Set path `msin`, write
        the pre-processed output at path `msout`.
        """
        LOGGER.info(f"Processing: {msin!s}")

        command_line = DP3Config.create(
            self.named_steps, msin, msout
        ).to_command_line()
        LOGGER.info(shlex.join(command_line))

        subprocess.check_call(
            command_line, env=os.environ | {"OPENBLAS_NUM_THREADS": "1"}
        )
        LOGGER.info(f"Finished: {msin!s}")

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> "Pipeline":
        """
        Creates a Pipeline from a YAML config file.
        """
        return cls(parse_and_validate_config_file(path))
