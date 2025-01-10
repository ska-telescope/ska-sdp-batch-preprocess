import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator

import yaml

from .validation import NamedStep, parse_and_validate_config


class PipelineConfig:
    """
    Configuration for the pipeline, as a dict-like object.
    """

    def __init__(self, conf: dict[str, Any]):
        """
        Initialise from a config dictionary.
        """
        self._steps = parse_and_validate_config(conf)

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> "PipelineConfig":
        """
        Creates a Config object from a YAML file.
        """
        with open(path, "r", encoding="utf-8") as file:
            return cls(yaml.safe_load(file))

    @property
    def steps(self) -> list[NamedStep]:
        """
        List of named pipeline steps.
        """
        return self._steps


class DP3Config(Mapping[str, Any]):
    """
    Configuration for DP3, as a dict-like object. Parameters are stored in
    their natural Python type. Paths must be stored as `Path` objects,
    so that they can be distinguished from plain strings and made absolute.
    """

    def __init__(self, params: dict[str, Any]):
        self._params = params

    def __iter__(self) -> Iterator[str]:
        return iter(self._params)

    def __len__(self) -> int:
        return len(self._params)

    def __getitem__(self, key: str):
        return self._params[key]

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
        step_names: list[str] = []
        conf = {
            "checkparset": 1,
            "steps": step_names,
            "msin.name": Path(msin),
            "msout.name": Path(msout),
        }

        for step in pipeline_config.steps:
            if step.type not in {"msin", "msout"}:
                step_names.append(step.name)
                conf[f"{step.name}.type"] = step.type

            for key, val in step.params.items():
                conf[f"{step.name}.{key}"] = val

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
    if isinstance(value, (list, tuple)):
        result = ",".join(map(_dp3_format_value, value))
        return f"[{result}]"

    if isinstance(value, bool):
        return "true" if value else "false"

    # Make paths absolute so we can safely call DP3 from any working directory
    if isinstance(value, Path):
        return str(value.resolve())

    return str(value)
