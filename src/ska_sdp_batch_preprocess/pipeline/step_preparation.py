import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from ska_sdp_batch_preprocess.config import Step
from ska_sdp_batch_preprocess.h5parm import H5Parm, InvalidH5Parm


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

    def _prepended_path(path_str: str) -> str:
        path = Path(path_str)
        result = path if path.is_absolute() else extra_inputs_dir / path
        return str(result)

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
    parmdb = step.params["parmdb"]

    try:
        h5parm = H5Parm.load(parmdb)
    except InvalidH5Parm as err:
        # Catch and re-raise to show the H5Parm file path in the error message
        updated_msg = f"{parmdb!r} is invalid, reason: {str(err)}"
        raise InvalidH5Parm(updated_msg) from err

    if is_fulljones(h5parm):
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.soltype)
        update = {
            "parmdb": parmdb,
            "correction": "fulljones",
            "soltab": [amp.name, phase.name],
        }

    elif len(h5parm.soltabs) == 1:
        update = {
            "parmdb": parmdb,
            "correction": h5parm.soltabs[0].name,
        }

    elif len(h5parm.soltabs) == 2:
        amp, phase = sorted(h5parm.soltabs, key=lambda s: s.soltype)
        update = {
            "parmdb": parmdb,
            "steps": ["amp", "phase"],
            "amp.correction": amp.name,
            "phase.correction": phase.name,
        }

    else:
        raise InvalidH5Parm(
            f"Failed to prepare applycal step: H5Parm {parmdb!r} "
            "has unexpected schema"
        )

    return Step(type="applycal", params=step.params | update)


def prepare_applycal_steps(steps: Iterable[Step]) -> list[Step]:
    """
    Apply `prepare_applycal_step` to applycal steps, leave the others
    unchanged.
    """

    def _prepare(step: Step) -> Step:
        return prepare_applycal_step(step) if step.type == "applycal" else step

    return list(map(_prepare, steps))


@dataclass
class PreparedStep:
    """
    Same as Step class, but carries an extra 'name' attribute which is a
    unique name for the DP3 step.

    NOTE: PreparedStep objects are meant to be created only via initialising a
    Pipeline object, and are NOT validated.
    """

    type: str
    """
    Step type as a lowercase string, e.g. 'preflagger'.
    """

    name: str
    """
    Name for the step, presumed to be unique, e.g. 'preflagger_01'.
    NOTE: `name` is ignored for steps `msin` and `msout`.
    """

    params: dict[str, Any]
    """
    Dictionary of parameters with values in their natural type.
    """


def uniquely_named_steps(steps: Iterable[Step]) -> list[PreparedStep]:
    """
    Final step of parsing where Steps are given a unique name and converted
    to PreparedSteps.
    """
    counter = defaultdict(int)

    def make_name(step: Step) -> str:
        if step.type in {"msin", "msout"}:
            return step.type
        counter[step.type] += 1
        index = counter[step.type]
        return f"{step.type}_{index:02d}"

    return [
        PreparedStep(step.type, make_name(step), step.params) for step in steps
    ]


def prepare_steps(
    steps: Iterable[Step], extra_inputs_dir: Optional[str | os.PathLike] = None
) -> list[PreparedStep]:
    """
    Prepare the Steps read from a configuration dictionary so that their
    parameters map 1-1 to what DP3 expects. Also takes care of preprending
    `extra_inputs_dir` where necessary and giving each step a unique name.

    Returns a list of PreparedSteps.
    """
    if extra_inputs_dir is not None:
        steps = prepend_extra_inputs_dir_to_parameters_that_require_it(
            steps, extra_inputs_dir
        )
    steps = prepare_applycal_steps(steps)
    return uniquely_named_steps(steps)
