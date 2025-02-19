class InvalidH5Parm(Exception):
    """
    Raised when the schema/layout of an H5Parm file on disk does not conform to
    expectations.
    """


def assert_or_invalid_h5parm(condition: bool, error_message: str):
    """
    Assert `condition` is True, raise `InvalidH5Parm(error_message)` otherwise.
    """
    if not condition:
        raise InvalidH5Parm(error_message)
