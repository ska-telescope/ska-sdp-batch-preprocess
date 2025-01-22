from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pytest

from ska_sdp_batch_preprocess.config import parse_config
from ska_sdp_batch_preprocess.pipeline import Pipeline

from .common import skip_unless_dp3_available
from .h5parm_generation import (
    create_diagonal_complex_identity_h5parm,
    create_fulljones_identity_h5parm,
    create_scalarphase_identity_h5parm,
)
from .ms_reading import (
    load_antenna_names_from_msv2,
    load_visibilities_from_msv2,
)


def make_config(h5parm_filenames: Iterable[str]) -> dict[str, Any]:
    """
    Make a pipeline configuration dictionary with one ApplyCal step per given
    h5parm file.
    """
    steps = [{"applycal": {"parmdb": fname}} for fname in h5parm_filenames]
    return {"steps": steps}


@skip_unless_dp3_available
def test_applycal_steps_with_identity_gain_tables_preserve_visibilities(
    tmp_path_factory: pytest.TempPathFactory, input_ms: Path
):
    """
    Run the pipeline with multiple applycal steps, each associated with
    an H5Parm file that carries an identity gain table. Each table has a
    different schema (scalarphase, diagonal, fulljones) that must be handled
    differently when generating the parameters for the DP3 call; that way, we
    trigger all associated code paths.
    """
    antenna_names = load_antenna_names_from_msv2(input_ms)
    solutions_dir = tmp_path_factory.mktemp("applycal_solutions_dir")
    create_scalarphase_identity_h5parm(
        solutions_dir / "scalarphase.h5", antenna_names
    )
    create_diagonal_complex_identity_h5parm(
        solutions_dir / "diagonal.h5", antenna_names
    )
    create_fulljones_identity_h5parm(
        solutions_dir / "fulljones.h5", antenna_names
    )
    config = make_config(["scalarphase.h5", "diagonal.h5", "fulljones.h5"])

    output_dir = tmp_path_factory.mktemp("applycal_outdir")
    output_ms = output_dir / input_ms.name

    steps = parse_config(config, solutions_dir)
    pipeline = Pipeline(steps)
    pipeline.run(input_ms, output_ms)

    assert output_ms.is_dir()

    vis_in = load_visibilities_from_msv2(input_ms)
    vis_out = load_visibilities_from_msv2(output_ms)

    assert np.allclose(vis_in, vis_out)
