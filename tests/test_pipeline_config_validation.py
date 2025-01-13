import pytest
from jsonschema import ValidationError

from ska_sdp_batch_preprocess.config import parse_config


def test_config_with_missing_steps_entry_is_rejected():
    """
    Self-explanatory.
    """
    conf = {}
    with pytest.raises(ValidationError):
        parse_config(conf)


def test_config_with_top_level_section_other_than_steps_is_rejected():
    """
    Self-explanatory.
    """
    conf = {
        "steps": [],
        "hello": "world",
    }
    with pytest.raises(ValidationError):
        parse_config(conf)


def test_step_entry_with_more_than_one_key_is_rejected():
    """
    Self-explanatory.
    """
    conf = {
        "steps": [
            {"Msin": {}},
            {"Averager": {}, "Whatever": {}},
        ]
    }
    with pytest.raises(ValidationError):
        parse_config(conf)


def test_step_entry_with_invalid_name_is_rejected():
    """
    Self-explanatory.
    """
    conf = {
        "steps": [
            {"InvalidStepName": {}},
        ]
    }
    with pytest.raises(ValidationError):
        parse_config(conf)


def test_config_with_two_msin_steps_is_rejected():
    """
    Self-explanatory.
    """
    conf = {
        "steps": [
            {"Msin": {}},
            {"Msin": {}},
        ]
    }
    with pytest.raises(ValidationError):
        parse_config(conf)


def test_config_with_two_msout_steps_is_rejected():
    """
    Self-explanatory.
    """
    conf = {
        "steps": [
            {"MsOut": {}},
            {"MsOut": {}},
        ]
    }
    with pytest.raises(ValidationError):
        parse_config(conf)
