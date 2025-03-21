import functools
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml
from jsonschema import Draft202012Validator, ValidationError

from ska_sdp_batch_preprocess.h5parm import H5Parm, InvalidH5Parm


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

    def __post_init__(self):
        self.type = self.type.lower()


def validate_top_level_structure(conf: dict[str, Any]):
    """
    Validate config file except the name and parameters of each step.
    """
    path = _schemas_dir() / "config.yaml"
    schema = yaml.safe_load(path.read_text())
    validator = Draft202012Validator(schema)
    validator.validate(instance=conf)


def parse_step_dictionary(step_dict: dict[str, Any]) -> Step:
    """
    First pass of parsing. Create a Step from a dictionary of the form
    {step_type: {step_params}}, as loaded from the config file.
    """
    if not len(step_dict.keys()) == 1:
        msg = (
            "Step must be given as a dictionary with one key: the step "
            f"type. This is invalid: {step_dict!r}"
        )
        raise ValidationError(msg)

    [(stype, params)] = step_dict.items()
    params = {} if params is None else params

    validators = _step_validators()
    if stype.lower() not in validators:
        raise ValidationError(
            f"Invalid step name: {stype!r}. Valid choices "
            f"(case-insensitive): {sorted(validators.keys())}"
        )
    validators[stype.lower()].validate(instance=params)
    return Step(stype, params)


def _assert_no_more_than_one_step_with_type(steps: Iterable[Step], stype: str):
    if len([s for s in steps if s.type == stype]) > 1:
        msg = f"Cannot specify more than 1 step with type {stype!r}"
        raise ValidationError(msg)


def prepend_extra_inputs_dir_to_parameters_that_require_it(
    steps: Iterable[Step], extra_inputs_dir: Path
) -> list[Step]:
    """
    Self-explanatory. Any parameter that represents an input path needs to be
    prepended with `extra_inputs_dir`, unless it is already absolute.
    """
    target_parameters = {
        "applycal": ["parmdb"],
        "demixer": ["skymodel"],
    }

    def _prepended_path(path: str | os.PathLike) -> Path:
        path = Path(path)
        return path if path.is_absolute() else extra_inputs_dir / path

    def _updated_step(step: Step) -> Step:
        if step.type not in target_parameters:
            return step

        param_names = target_parameters[step.type]
        updated_params = {
            key: _prepended_path(value)
            for key, value in step.params.items()
            if key in param_names
        }
        return Step(type=step.type, params=step.params | updated_params)

    return list(map(_updated_step, steps))


def is_fulljones(parm: H5Parm) -> bool:
    """
    Whether given H5Parm represents a full Jones solution.
    """
    soltypes = set(tab.soltype for tab in parm.soltabs)
    pols = [tuple(tab.axes.get("pol", [])) for tab in parm.soltabs]
    linear = ("XX", "XY", "YX", "YY")
    return soltypes == {"amplitude", "phase"} and pols == [linear, linear]


def prepare_applycal_step(step: Step) -> Step:
    """
    Prepare applycal step parameters based on the contents of the associated
    H5Parm.
    """
    parmdb = Path(step.params["parmdb"])

    try:
        h5parm = H5Parm.load(parmdb)
    except InvalidH5Parm as err:
        # Catch and re-raise to show the H5Parm file path in the error message
        updated_msg = f"{parmdb!r} is invalid, reason: {str(err)}"
        raise InvalidH5Parm(updated_msg) from err

    if is_fulljones(h5parm):
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.soltype)
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
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.soltype)
        params = step.params | {
            "parmdb": parmdb,
            "steps": ["amp", "phase"],
            "amp.correction": amp.name,
            "phase.correction": phase.name,
        }
        return Step(type="applycal", params=params)

    raise InvalidH5Parm(
        f"Failed to prepare applycal step: H5Parm {str(parmdb)!r} "
        "has unexpected schema"
    )


def prepare_applycal_steps(steps: Iterable[Step]) -> list[Step]:
    """
    Apply `prepare_applycal_step` to applycal steps, leave the others
    unchanged.
    """

    def _prepare(step: Step) -> Step:
        return prepare_applycal_step(step) if step.type == "applycal" else step

    return list(map(_prepare, steps))


def parse_config(
    conf: dict, extra_inputs_dir: Optional[str | os.PathLike] = None
) -> list[Step]:
    """
    Parse config dictionary into a list of Steps. `extra_inputs_dir` is an
    optional directory path where additional input files mentioned in the
    config are expected to be stored. Any path to e.g. a solution table in the
    config that is not absolute will be prepended with `extra_inputs_dir`.

    Raise jsonschema.ValidationError if the config is invalid.
    """
    validate_top_level_structure(conf)
    steps = list(map(parse_step_dictionary, conf["steps"]))
    _assert_no_more_than_one_step_with_type(steps, "msin")
    _assert_no_more_than_one_step_with_type(steps, "msout")
    _assert_no_more_than_one_step_with_type(steps, "demixer")
    if extra_inputs_dir is not None:
        steps = prepend_extra_inputs_dir_to_parameters_that_require_it(
            steps, extra_inputs_dir
        )
    return prepare_applycal_steps(steps)


def parse_config_file(
    path: str | os.PathLike,
    extra_inputs_dir: Optional[str | os.PathLike] = None,
) -> list[Step]:
    """
    Same as parse_config, except that the first argument is a config file path
    instead of a config dict.
    """
    with open(path, "r", encoding="utf-8") as file:
        return parse_config(yaml.safe_load(file), extra_inputs_dir)
