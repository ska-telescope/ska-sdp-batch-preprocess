import os
from pathlib import Path
from typing import Any, Iterable, Optional

from .step_preparation import PreparedStep


def make_instrumentmodel_unique_in_demixer_steps(
    steps: Iterable[PreparedStep], msout: str | os.PathLike
) -> list[PreparedStep]:
    """
    As the name says. The Demixer step always writes the bright source gains
    to disk, under a path specified in the "instrumentmodel" parameter which
    defaults to "instrument". This path must be unique among all DP3 instances
    running, but this is incompatible with the single config, multiple data
    model.

    Solution: treat "instrumentmodel" as a path prefix, and write the gains to
    `<instrumentmodel>_<MSOUT_STEM>`. instrumentmodel can be:
    - an absolute path, e.g. "/abspath/to/gains"
    - a relative path, e.g. "path/to/gains" or just "gains"

    If "instrumentmodel" is a relative path, it will be prepended by the parent
    directory of `msout`.

    If "instrumentmodel" is not provided in the demixer step config, it is set
    to a default value of "demixer_gains".
    """
    msout = Path(msout).resolve()

    def adjust(step: PreparedStep) -> PreparedStep:
        if not step.type == "demixer":
            return step

        prefix = Path(step.params.get("instrumentmodel", "demixer_gains"))
        if not prefix.is_absolute():
            prefix = msout.parent / prefix

        # DP3 wants the parent directory tree for the gain table to exist
        prefix.parent.mkdir(parents=True, exist_ok=True)

        instrumentmodel = prefix.with_name(f"{prefix.name}_{msout.stem}")
        return PreparedStep(
            step.type,
            step.name,
            params=step.params | {"instrumentmodel": instrumentmodel},
        )

    return list(map(adjust, steps))


def make_dp3_parameters(
    steps: Iterable[PreparedStep],
    msin: str | os.PathLike,
    msout: str | os.PathLike,
    *,
    numthreads: Optional[int] = None,
) -> dict[str, Any]:
    """
    Translate pipeline steps into parameters for a single DP3 execution. If not
    specified, `numthreads` defaults to the total number of threads allocated
    to the current process.

    NOTE: This function may create additional directories required to store
    additional outputs specified in the config file, e.g. demixer's
    instrumentmodel table.
    """
    steps = make_instrumentmodel_unique_in_demixer_steps(steps, msout)

    conf = {
        "checkparset": 1,
        "steps": [
            step.name for step in steps if step.type not in {"msin", "msout"}
        ],
        "msin.name": Path(msin),
        "msout.name": Path(msout),
    }

    # If 'numthreads' is not given, DP3 uses the total number of cores on
    # the machine. We want instead the number of cores allocated to the
    # process.
    if numthreads is None:
        numthreads = len(os.sched_getaffinity(0))
    conf["numthreads"] = numthreads

    for step in steps:
        if step.type not in {"msin", "msout"}:
            conf[f"{step.name}.type"] = step.type

        for key, val in step.params.items():
            conf[f"{step.name}.{key}"] = val

    return conf


def format_dp3_parameters(params: dict[str, Any]) -> dict[str, str]:
    """
    Convert DP3 parameter values from their natural type to strings in the
    format that DP3 wants them.
    """
    return {key: _format_dp3_value(value) for key, value in params.items()}


def _format_dp3_value(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        result = ",".join(map(_format_dp3_value, value))
        return f"[{result}]"

    if isinstance(value, bool):
        return "true" if value else "false"

    return str(value)


def make_dp3_command_line(formatted_params: dict[str, str]) -> list[str]:
    """
    Convert already formatted DP3 params dict into a command line ready to be
    executed via `subprocess.check_call()` or equivalent.
    """
    return ["DP3"] + [f"{key}={val}" for key, val in formatted_params.items()]
