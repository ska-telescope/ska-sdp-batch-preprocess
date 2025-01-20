import functools
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml
from jsonschema import Draft202012Validator, ValidationError

from ska_sdp_batch_preprocess.h5parm import H5Parm


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
class StepDefinition:
    """
    Parameters of one step as specified in the config file. They still
    need further conversion to DP3 parameters.
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
    def from_step_dict(cls, step_dict: dict[str, Any]) -> "StepDefinition":
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


def _assert_no_more_than_one_step_definition_with_type(
    steps: Iterable[StepDefinition], stype: str
):
    if len([s for s in steps if s.type == stype]) > 1:
        msg = f"Cannot specify more than 1 step with type {stype!r}"
        raise ValidationError(msg)


@dataclass
class Step:
    """
    Step with a unique name and parameters mapping 1:1 to what DP3 expects.
    It can be directly converted to DP3 command-line parameters.
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
    Dictionary of legal DP3 parameters with values in their natural type.
    """


def validate_top_level_structure(conf: dict[str, Any]):
    """
    Validate config file except the name and parameters of each step.
    """
    path = _schemas_dir() / "config.yaml"
    schema = yaml.safe_load(path.read_text())
    validator = Draft202012Validator(schema)
    validator.validate(instance=conf)


def prepare_applycal_step(step: StepDefinition) -> StepDefinition:
    """
    Prepare applycal step parameters based on the contents of the associated
    H5Parm.
    """
    fname = step.params["parmdb"]
    h5parm = H5Parm.from_file(fname)

    if h5parm.is_fulljones:
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.solution_type)
        params = step.params | {
            "parmdb": fname,
            "correction": "fulljones",
            "soltab": [amp.name, phase.name],
        }
        return StepDefinition(type="applycal", params=params)

    if len(h5parm.soltabs) == 1:
        params = step.params | {
            "parmdb": fname,
            "correction": h5parm.soltabs[0].name,
        }
        return StepDefinition(type="applycal", params=params)

    if len(h5parm.soltabs) == 2:
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.solution_type)
        params = step.params | {
            "parmdb": fname,
            "steps": ["amp", "phase"],
            "amp.correction": amp.name,
            "phase.correction": phase.name,
        }
        return StepDefinition(type="applycal", params=params)

    raise ValidationError(
        f"Failed to prepare applycal step: H5Parm {fname!r} "
        "has unexpected schema"
    )


def make_uniquely_named_steps(steps: Iterable[StepDefinition]) -> list[Step]:
    """
    Self-explanatory.
    """
    counter = defaultdict(int)

    def _make_unique_name(step: StepDefinition) -> str:
        if step.type in {"msin", "msout"}:
            return step.type
        counter[step.type] += 1
        return f"{step.type}_{counter[step.type]:02d}"

    return [
        Step(step.type, _make_unique_name(step), step.params) for step in steps
    ]


def parse_config(conf: dict) -> list[Step]:
    """
    Parse config dictionary into a list of Steps. Raise
    jsonschema.ValidationError if the config is invalid.
    """
    validate_top_level_structure(conf)
    steps = list(map(StepDefinition.from_step_dict, conf["steps"]))
    _assert_no_more_than_one_step_definition_with_type(steps, "msin")
    _assert_no_more_than_one_step_definition_with_type(steps, "msout")

    prepared_steps = []
    for step in steps:
        if step.type == "applycal":
            prepared_steps.append(prepare_applycal_step(step))
        else:
            prepared_steps.append(step)

    return make_uniquely_named_steps(prepared_steps)


def parse_config_file(path: str | os.PathLike) -> list[Step]:
    """
    Same as parse_config, but takes a file path as input.
    """
    with open(path, "r", encoding="utf-8") as file:
        return parse_config(yaml.safe_load(file))
