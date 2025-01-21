import functools
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

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
    Step with parameters mapping 1:1 to what DP3 expects.
    It can be directly converted to DP3 command-line parameters.
    """

    type: str
    """
    Step type as a lowercase string, e.g. 'preflagger'.
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


def prepare_applycal_step(
    step: StepDefinition, solutions_dir: Optional[Path] = None
) -> Step:
    """
    Prepare applycal step parameters based on the contents of the associated
    H5Parm.
    """
    parmdb = Path(step.params["parmdb"])
    if not parmdb.is_absolute() and solutions_dir is not None:
        parmdb = solutions_dir / parmdb

    h5parm = H5Parm.from_file(parmdb)

    if h5parm.is_fulljones:
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.solution_type)
        params = step.params | {
            "parmdb": parmdb,
            "correction": "fulljones",
            "soltab": [amp.name, phase.name],
        }
        return Step(type="applycal", params=params)

    if len(h5parm.soltabs) == 1:
        params = step.params | {
            "parmdb": parmdb,
            "correction": h5parm.soltabs[0].name,
        }
        return Step(type="applycal", params=params)

    if len(h5parm.soltabs) == 2:
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.solution_type)
        params = step.params | {
            "parmdb": parmdb,
            "steps": ["amp", "phase"],
            "amp.correction": amp.name,
            "phase.correction": phase.name,
        }
        return Step(type="applycal", params=params)

    raise ValidationError(
        f"Failed to prepare applycal step: H5Parm {str(parmdb)!r} "
        "has unexpected schema"
    )


def prepare_steps(
    step_definitions: Iterable[StepDefinition],
    solutions_dir: Optional[str | os.PathLike] = None,
) -> list[Step]:
    """
    Create Steps with parameters ready to be passed to DP3.
    """
    solutions_dir = Path(solutions_dir) if solutions_dir is not None else None
    steps = []
    for step_def in step_definitions:
        if step_def.type == "applycal":
            steps.append(prepare_applycal_step(step_def, solutions_dir))
        else:
            steps.append(Step(type=step_def.type, params=step_def.params))
    return steps


def parse_config(
    conf: dict, solutions_dir: Optional[str | os.PathLike] = None
) -> list[Step]:
    """
    Parse config dictionary into a list of Steps. `solutions_dir` is an
    optional directory path where the solution tables for ApplyCal steps are
    expected to be stored; any solution table path in the config that is not
    absolute will be prepended with `solutions_dir`.

    Raise jsonschema.ValidationError if the config is invalid.
    """
    validate_top_level_structure(conf)
    step_defs = list(map(StepDefinition.from_step_dict, conf["steps"]))
    _assert_no_more_than_one_step_definition_with_type(step_defs, "msin")
    _assert_no_more_than_one_step_definition_with_type(step_defs, "msout")
    return prepare_steps(step_defs, solutions_dir)


def parse_config_file(
    path: str | os.PathLike, solutions_dir: Optional[str | os.PathLike] = None
) -> list[Step]:
    """
    Same as parse_config, except that the first argument is a config file path
    instead of a config dict.
    """
    with open(path, "r", encoding="utf-8") as file:
        return parse_config(yaml.safe_load(file), solutions_dir)
