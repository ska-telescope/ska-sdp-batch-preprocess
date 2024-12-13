from pathlib import Path

import pytest

from ska_sdp_batch_preprocess.config import DP3Config, PipelineConfig


@pytest.fixture(name="pipeline_config")
def fixture_pipeline_config() -> PipelineConfig:
    """
    Config for the pipeline. We load it from the example config file provided
    in the repository.
    """
    path = Path(__file__).parent / ".." / "config" / "config.yaml"
    return PipelineConfig.from_yaml(path)


def test_generated_dp3_command_is_correct(pipeline_config: PipelineConfig):
    """
    Generate a DP3 command based on the pipeline configuration and check that
    it is as expected.
    """
    msin = Path("/path/to/input.ms")
    msout = Path("/path/to/output.ms")

    command = DP3Config.create(pipeline_config, msin, msout).to_command_line()
    expected_command = [
        "DP3",
        "checkparset=1",
        "steps=[preflagger,aoflagger,averager]",
        "msin.name=/path/to/input.ms",
        "msout.name=/path/to/output.ms",
        "aoflagger.memorymax=8.0",
        "averager.timestep=4",
        "averager.freqstep=4",
        "msout.overwrite=true",
    ]
    assert command == expected_command
