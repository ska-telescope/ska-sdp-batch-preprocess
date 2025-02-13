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
