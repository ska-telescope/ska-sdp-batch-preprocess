import os
from pathlib import Path

import pytest

from ska_sdp_batch_preprocess.config import Step
from ska_sdp_batch_preprocess.dp3_params import DP3Params


@pytest.fixture(name="steps")
def fixture_steps() -> list[Step]:
    """
    List of pipeline steps for which we want to generate a DP3 call.
    """

    return [
        Step(type="preflagger", params={}),
        Step(type="aoflagger", params={"memorymax": 8.0}),
        Step(type="averager", params={"timestep": 4, "freqstep": 4}),
        Step(type="aoflagger", params={"memorymax": 16.0}),
        Step(type="msout", params={"overwrite": True}),
    ]


def test_generated_dp3_command_is_correct(steps: list[Step]):
    """
    Generate a DP3 command based on the given pipeline steps, check that
    it is as expected.
    """
    msin = Path("/path/to/input.ms")
    msout = Path("/path/to/output.ms")

    command = DP3Params.create(steps, msin, msout).to_command_line()
    expected_numthreads = len(os.sched_getaffinity(0))
    expected_command = [
        "DP3",
        "checkparset=1",
        "steps=[preflagger_01,aoflagger_01,averager_01,aoflagger_02]",
        "msin.name=/path/to/input.ms",
        "msout.name=/path/to/output.ms",
        f"numthreads={expected_numthreads}",
        "preflagger_01.type=preflagger",
        "aoflagger_01.type=aoflagger",
        "aoflagger_01.memorymax=8.0",
        "averager_01.type=averager",
        "averager_01.timestep=4",
        "averager_01.freqstep=4",
        "aoflagger_02.type=aoflagger",
        "aoflagger_02.memorymax=16.0",
        "msout.overwrite=true",
    ]
    assert command == expected_command
