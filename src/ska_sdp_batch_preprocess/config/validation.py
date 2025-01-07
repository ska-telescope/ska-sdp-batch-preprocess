import functools
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


@dataclass
class Step:
    """
    Simple wrapper for a step's name and parameters.
    """

    name: str
    """
    Lowercase name.
    """

    params: dict[str, Any]
    """
    Dictionary of parameters with values in their natural type.
    """

    def __post_init__(self):
        validators = _step_validators()
        if self.name.lower() not in validators:
            raise ValidationError(
                f"Invalid step name: {self.name!r}. Valid choices "
                f"(case-insensitive): {sorted(validators.keys())}"
            )
        self.name = self.name.lower()
        validators[self.name].validate(instance=self.params)

    @classmethod
    def from_step_dict(cls, step_dict: dict[str, Any]) -> "Step":
        """
        Create from dictionary of the form {step_name: {step_params}}, as
        loaded from the config file.
        """
        if not len(step_dict.keys()) == 1:
            msg = (
                "Step must be given as a dictionary with one key: the step "
                f"name. This is invalid: {step_dict!r}"
            )
            raise ValidationError(msg)

        name, params = next(iter(step_dict.items()))
        params = {} if params is None else params
        return cls(name, params)


def _assert_no_more_than_one_step_with_name(steps: Iterable[Step], name: str):
    if len([s for s in steps if s.name == name]) > 1:
        msg = f"Cannot specify more than 1 step with name {name!r}"
        raise ValidationError(msg)


def validate_top_level_structure(conf: dict[str, Any]):
    """
    Validate config file except the name and parameters of each step.
    """
    path = _schemas_dir() / "config.yaml"
    schema = yaml.safe_load(path.read_text())
    validator = Draft202012Validator(schema)
    validator.validate(instance=conf)


def parse_and_validate_config(conf: dict) -> list[Step]:
    """
    Parse config dictionary into a list of Steps. Raise
    jsonschema.ValidationError if the config is invalid.
    """
    validate_top_level_structure(conf)
    steps = list(map(Step.from_step_dict, conf["steps"]))
    _assert_no_more_than_one_step_with_name(steps, "msin")
    _assert_no_more_than_one_step_with_name(steps, "msout")
    return steps
