from pathlib import Path

import pytest
import yaml

from ska_sdp_batch_preprocess.config import Step, parse_config
from ska_sdp_batch_preprocess.dp3_params import DP3Params


@pytest.fixture(name="steps")
def fixture_steps() -> list[Step]:
    """
    List of pipeline steps, loaded from the example config file provided in the
    repository.
    """
    path = Path(__file__).parent / ".." / "config" / "config.yaml"
    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
        return parse_config(config)


def test_generated_dp3_command_is_correct(steps: list[Step]):
    """
    Generate a DP3 command based on the pipeline configuration and check that
    it is as expected.
    """
    msin = Path("/path/to/input.ms")
    msout = Path("/path/to/output.ms")

    command = DP3Params.create(steps, msin, msout).to_command_line()
    expected_command = [
        "DP3",
        "checkparset=1",
        "steps=[preflagger_01,aoflagger_01,averager_01]",
        "msin.name=/path/to/input.ms",
        "msout.name=/path/to/output.ms",
        "preflagger_01.type=preflagger",
        "aoflagger_01.type=aoflagger",
        "aoflagger_01.memorymax=8.0",
        "averager_01.type=averager",
        "averager_01.timestep=4",
        "averager_01.freqstep=4",
        "msout.overwrite=true",
    ]
    assert command == expected_command
