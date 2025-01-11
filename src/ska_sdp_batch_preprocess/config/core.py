import functools
from collections import defaultdict
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
    Simple wrapper for a step's type and parameters.
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
        validators = _step_validators()
        if self.type.lower() not in validators:
            raise ValidationError(
                f"Invalid step name: {self.type!r}. Valid choices "
                f"(case-insensitive): {sorted(validators.keys())}"
            )
        self.type = self.type.lower()
        validators[self.type].validate(instance=self.params)

    @classmethod
    def from_step_dict(cls, step_dict: dict[str, Any]) -> "Step":
        """
        Create from dictionary of the form {step_type: {step_params}}, as
        loaded from the config file.
        """
        if not len(step_dict.keys()) == 1:
            msg = (
                "Step must be given as a dictionary with one key: the step "
                f"type. This is invalid: {step_dict!r}"
            )
            raise ValidationError(msg)

        stype, params = next(iter(step_dict.items()))
        params = {} if params is None else params
        return cls(stype, params)


def _assert_no_more_than_one_step_with_type(steps: Iterable[Step], stype: str):
    if len([s for s in steps if s.type == stype]) > 1:
        msg = f"Cannot specify more than 1 step with type {stype!r}"
        raise ValidationError(msg)


@dataclass
class NamedStep:
    """
    Step that has been given a unique name.
    """

    type: str
    """
    Step type as a lowercase string, e.g. 'preflagger'.
    """

    name: str
    """
    Unique name for the step, e.g. 'preflagger_01'.
    NOTE: 'msin' and 'msout' steps will be named 'msin' and 'msout' without
    numerical suffix.
    """

    params: dict[str, Any]
    """
    Dictionary of parameters with values in their natural type.
    """


def validate_top_level_structure(conf: dict[str, Any]):
    """
    Validate config file except the name and parameters of each step.
    """
    path = _schemas_dir() / "config.yaml"
    schema = yaml.safe_load(path.read_text())
    validator = Draft202012Validator(schema)
    validator.validate(instance=conf)


def make_uniquely_named_steps(steps: Iterable[Step]) -> list[NamedStep]:
    """
    Self-explanatory.
    """
    counter = defaultdict(int)

    def _make_unique_name(step: Step) -> str:
        if step.type in {"msin", "msout"}:
            return step.type
        counter[step.type] += 1
        return f"{step.type}_{counter[step.type]:02d}"

    return [
        NamedStep(step.type, _make_unique_name(step), step.params)
        for step in steps
    ]


def parse_and_validate_config(conf: dict) -> list[NamedStep]:
    """
    Parse config dictionary into a list of NamedSteps. Raise
    jsonschema.ValidationError if the config is invalid.
    """
    validate_top_level_structure(conf)
    steps = list(map(Step.from_step_dict, conf["steps"]))
    _assert_no_more_than_one_step_with_type(steps, "msin")
    _assert_no_more_than_one_step_with_type(steps, "msout")
    return make_uniquely_named_steps(steps)
