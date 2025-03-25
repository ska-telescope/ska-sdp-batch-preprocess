import os
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

from ska_sdp_batch_preprocess.config import Step


# pylint:disable=too-few-public-methods
class UniqueNamer:
    """
    Makes unique names for DP3 steps.
    """

    def __init__(self):
        self._counter: dict[str, int] = defaultdict(int)

    def make_name(self, step: Step) -> str:
        """
        Make name for given Step, such as 'applycal_02'.
        """
        if step.type in {"msin", "msout"}:
            return step.type
        self._counter[step.type] += 1
        index = self._counter[step.type]
        return f"{step.type}_{index:02d}"


def make_instrumentmodel_unique_in_demixer_steps(
    steps: Iterable[Step], msout: str | os.PathLike
) -> list[Step]:
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

    def adjust(step: Step) -> Step:
        if not step.type == "demixer":
            return step

        prefix = Path(step.params.get("instrumentmodel", "demixer_gains"))
        if not prefix.is_absolute():
            prefix = msout.parent / prefix

        # DP3 wants the parent directory tree for the gain table to exist
        prefix.parent.mkdir(parents=True, exist_ok=True)

        instrumentmodel = prefix.with_name(f"{prefix.name}_{msout.stem}")
        return Step(
            step.type,
            params=step.params | {"instrumentmodel": instrumentmodel},
        )

    return list(map(adjust, steps))


class DP3Params(Mapping[str, Any]):
    """
    Parameters for DP3, as a dict-like object. Parameters are stored in
    their natural Python type.
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
        steps: Iterable[Step],
        msin: str | os.PathLike,
        msout: str | os.PathLike,
        numthreads: Optional[int] = None,
    ) -> "DP3Params":
        """
        Create DP3Params, translating pipeline steps into parameters for a
        single DP3 execution. If not specified, `numthreads` defaults to the
        total number of threads allocated to the current process.
        """
        steps = make_instrumentmodel_unique_in_demixer_steps(steps, msout)

        step_names: list[str] = []
        conf = {
            "checkparset": 1,
            "steps": step_names,
            "msin.name": Path(msin),
            "msout.name": Path(msout),
        }

        # If 'numthreads' is not given, DP3 uses the total number of cores on
        # the machine. We want instead the number of cores allocated to the
        # process.
        if numthreads is None:
            numthreads = len(os.sched_getaffinity(0))
        conf["numthreads"] = numthreads

        unique_namer = UniqueNamer()

        for step in steps:
            step_name = unique_namer.make_name(step)

            if step.type not in {"msin", "msout"}:
                step_names.append(step_name)
                conf[f"{step_name}.type"] = step.type

            for key, val in step.params.items():
                conf[f"{step_name}.{key}"] = val

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

    return str(value)
