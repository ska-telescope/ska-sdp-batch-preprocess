from pathlib import Path

import pytest


@pytest.fixture(name="yaml_config", scope="session")
def fixture_yaml_config() -> Path:
    """
    YAML config file path.
    """
    return Path(__file__).parent / ".." / ".." / "config" / "config.yaml"
