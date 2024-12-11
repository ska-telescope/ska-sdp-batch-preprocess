import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator

import yaml


class ConfigBase(Mapping):
    """
    Base class for configuration objects; just an immutable wrapper for a dict.
    """

    def __init__(self, params: dict[str, Any]):
        self._params = params

    def __iter__(self):
        return iter(self._params)

    def __len__(self):
        return len(self._params)

    def __getitem__(self, key):
        return self._params[key]


class PipelineConfig(ConfigBase):
    """
    Configuration for the pipeline, as a dict-like object.
    """

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> "PipelineConfig":
        """
        Creates a Config object from a YAML file.
        """
        with open(path, "r", encoding="utf-8") as file:
            return cls(yaml.safe_load(file))

    def steps(self) -> Iterator[tuple[str, dict]]:
        """
        Iterator through pipeline steps.
        Yields tuples (step_name, step_params).
        """
        steps: list[dict] = self["DP3"]["steps"]
        for step in steps:
            # "step" is a dictionary with one key: the step name
            # The associated value is a dict of parameters for the step
            name = list(step.keys())[0]
            params = step[name]
            if params is None:
                params = {}
            yield name, params


class DP3Config(ConfigBase):
    """
    Configuration for DP3, as a dict-like object. Parameters are stored in
    their natural Python type. Paths must be stored as `Path` objects,
    so that they can be distinguished from plain strings and made absolute.
    """

    @classmethod
    def create(
        cls,
        pipeline_config: PipelineConfig,
        msin: str | os.PathLike,
        msout: str | os.PathLike,
    ) -> "DP3Config":
        """
        Translate pipeline config into parameters for a single DP3 execution.
        """
        conf = {
            "steps": [name.lower() for name, _ in pipeline_config.steps()],
            "msin.name": Path(msin),
            "msout.name": Path(msout),
        }

        for name, params in pipeline_config.steps():
            for key, val in params.items():
                conf[f"{name.lower()}.{key}"] = val

        return cls(conf)

    def to_command_line(self) -> list[str]:
        """
        Convert to a DP3 command line ready to be executed.
        """
        args = ["DP3"]
        for key, val in self.items():
            args.append(f"{key}={_dp3_format_value(val)}")
        return args


def _dp3_format_value(value: Any) -> str:
    """
    Convert a DP3 parameter value to a string that can be passed to DP3.
    - Lists/sequences need to be formatted in a particular way
    - We make all Paths absolute so that we can safely call DP3 as a subprocess
      from any working directory.
    """
    if isinstance(value, (list, tuple)):
        result = ",".join(map(_dp3_format_value, value))
        return f"[{result}]"

    if isinstance(value, Path):
        return str(value.resolve())

    return str(value)
