# see license in parent directory

import json
from pathlib import Path

from jsonschema import Draft202012Validator


def validate_config(config: dict) -> None:
    """
    Validates the input .yml file against 'schemas/config.json'.

    Attributes
    ----------
    config: dict
      the configuration object loaded from the .yml file
    """
    schemas_path = Path(__file__).parent.joinpath("schemas")

    path = schemas_path / "config.json"
    schemas_dict = json.loads(path.read_text())
    
    validator = Draft202012Validator(schemas_dict)
    validator.validate(instance=config)