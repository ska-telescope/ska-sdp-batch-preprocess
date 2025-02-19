import numpy as np
import pytest

from ska_sdp_batch_preprocess.h5parm import H5Parm, InvalidH5Parm, Soltab


@pytest.fixture(name="parm")
def fixture_complex_h5parm() -> H5Parm:
    axes = {
        "time": np.arange(10),
        "freq": np.linspace(1.0e9, 2.0e9, 20),
        "ant": ["a", "b", "c", "d"],
    }
    val = np.zeros(shape=(10, 20, 4), dtype=complex)
    weight = np.ones_like(val, dtype=float)
    return H5Parm.from_complex_gain_data(
        axes=axes,
        val=val,
        weight=weight,
    )


def test_save_load_roundtrip_preserves_h5parm_contents(
    tmp_path_factory: pytest.TempPathFactory, parm: H5Parm
):
    """
    Self-explanatory.
    """
    tmpdir = tmp_path_factory.mktemp("h5parm")
    path = tmpdir / "complex.h5parm"
    parm.save(path)
    reloaded = H5Parm.load(path)
    assert parm == reloaded


def test_h5parm_repr(parm: H5Parm):
    """
    Self-explanatory.
    """
    expected_lines = (
        "H5Parm(",
        "    Soltab(name=None, soltype='amplitude', dimensions={'time': 10, 'freq': 20, 'ant': 4})",  # noqa: E501
        "    Soltab(name=None, soltype='phase', dimensions={'time': 10, 'freq': 20, 'ant': 4})",  # noqa: E501
        ")",
    )
    assert repr(parm) == "\n".join(expected_lines)


def test_incomplete_full_jones_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    phase = Soltab(
        "phase",
        axes={
            "time": [1, 2, 3],
            "pol": ["XX", "XY", "YX", "YY"],
        },
        val=np.zeros(shape=(3, 4)),
        weight=np.ones(shape=(3, 4)),
    )

    with pytest.raises(InvalidH5Parm):
        H5Parm([phase])


def test_h5parm_without_soltabs_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    with pytest.raises(
        InvalidH5Parm, match="Number of soltabs must be 1 or 2"
    ):
        H5Parm([])


def test_h5parm_with_more_than_two_soltabs_raises_invalid_h5parm():
    """
    Self-explanatory.
    """
    axes = {"time": np.arange(10)}
    val = np.zeros(shape=(10,))
    weight = np.ones_like(val, dtype=float)
    soltab = Soltab("phase", axes, val, weight)

    with pytest.raises(
        InvalidH5Parm, match="Number of soltabs must be 1 or 2"
    ):
        H5Parm([soltab, soltab, soltab])
