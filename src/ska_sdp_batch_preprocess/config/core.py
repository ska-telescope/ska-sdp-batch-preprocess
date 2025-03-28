import functools
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml
from jsonschema import Draft202012Validator, ValidationError


def _schemas_dir() -> Path:
    return Path(__file__).parent / "schemas"


@functools.cache
def _step_validators() -> dict[str, Draft202012Validator]:
    paths = _schemas_dir().glob("step_*.yaml")

    def _load_schema(path: Path) -> tuple[str, Draft202012Validator]:
        __, name = path.stem.split("_", maxsplit=1)
        validator = Draft202012Validator(yaml.safe_load(path.read_text()))
        return name, validator

    return dict(map(_load_schema, paths))


def _validate_step_params(stype: str, params: dict[str, Any]):
    validators = _step_validators()
    if stype not in validators:
        raise ValidationError(
            f"Invalid step name: {stype!r}. Valid choices "
            f"(case-insensitive): {sorted(validators.keys())}"
        )
    validators[stype].validate(instance=params)


@dataclass
class Step:
    """
    Step with parameters as read from the config file.
    """

    type: str
    """
    Step type as a lowercase string, e.g. 'preflagger'.
    """

    params: dict[str, Any]
    """
    Dictionary of parameters with values in their natural type.
    """

    def __post_init__(self):
        self.type = self.type.lower()
        _validate_step_params(self.type, self.params)

    @classmethod
    def from_step_dict(cls, step_dict: dict[str, Any]) -> "Step":
        """
        Create a Step from a dictionary of the form {step_type: {step_params}},
        as loaded from a config file.
        """
        if not len(step_dict.keys()) == 1:
            msg = (
                "Step must be given as a dictionary with one key: the step "
                f"type. This is invalid: {step_dict!r}"
            )
            raise ValidationError(msg)

        [(stype, params)] = step_dict.items()
        params = {} if params is None else params
        return cls(stype, params)


def validate_top_level_structure(conf: dict[str, Any]):
    """
    Validate config file except the name and parameters of each step.
    """
    path = _schemas_dir() / "config.yaml"
    schema = yaml.safe_load(path.read_text())
    validator = Draft202012Validator(schema)
    validator.validate(instance=conf)


def _assert_no_more_than_one_step_with_type(steps: Iterable[Step], stype: str):
    if len([s for s in steps if s.type == stype]) > 1:
        msg = f"Cannot specify more than 1 step with type {stype!r}"
        raise ValidationError(msg)


def parse_config(conf: dict) -> list[Step]:
    """
    Parse config dictionary into a list of Steps.
    Raise jsonschema.ValidationError if the config is invalid.
    """
    validate_top_level_structure(conf)
    steps = list(map(Step.from_step_dict, conf["steps"]))
    _assert_no_more_than_one_step_with_type(steps, "msin")
    _assert_no_more_than_one_step_with_type(steps, "msout")
    _assert_no_more_than_one_step_with_type(steps, "demixer")
    return steps


def parse_config_file(path: str | os.PathLike) -> list[Step]:
    """
    Parse config file into a list of Steps.
    Raise jsonschema.ValidationError if the config is invalid.
    """
    with open(path, "r", encoding="utf-8") as file:
        return parse_config(yaml.safe_load(file))
