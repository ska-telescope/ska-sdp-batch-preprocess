import os
import subprocess

from .config import DP3Config, PipelineConfig


class Pipeline:
    """
    Sequence of operations to be run on Measurement Sets.
    """

    def __init__(self, config: PipelineConfig):
        """
        Initialise Pipeline given a config object.
        """
        self.config = config

    def run(self, msin: str | os.PathLike, msout: str | os.PathLike):
        """
        Run the pipeline on given input Measurement Set path `msin`, write
        the pre-processed output at path `msout`.
        """
        command_line = DP3Config.create(
            self.config, msin, msout
        ).to_command_line()
        subprocess.check_call(command_line)

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> "Pipeline":
        """
        Creates a Pipeline from a YAML config file.
        """
        return cls(PipelineConfig.from_yaml(path))
