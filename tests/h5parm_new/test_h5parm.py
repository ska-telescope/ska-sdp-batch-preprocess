import numpy as np
import pytest

from ska_sdp_batch_preprocess.h5parm_new import H5Parm, Soltab


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
