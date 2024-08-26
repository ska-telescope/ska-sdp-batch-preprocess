# see license in parent directory

import json
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from pathlib import Path
from referencing.jsonschema import DRAFT202012


def validate_config(config: dict) -> None:
    """
    """
    schemas_path = Path(__file__).parent.joinpath("schemas")
    
    def retrieve_data(uri: str) -> Resource:
        path = schemas_path / uri.removeprefix("http://localhost/")
        return DRAFT202012.create_resource(json.loads(path.read_text()))

    path = schemas_path / "config.json"
    schemas_dict = json.loads(path.read_text())
    
    validator = Draft202012Validator(
        schemas_dict, registry=Registry(retrieve=retrieve_data)
    )

    validator.validate(instance=config)