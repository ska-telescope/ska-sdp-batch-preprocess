.. _pipeline:

**************
Pipeline Usage
**************

The pipeline is a CLI app, a complete description of the arguments is available via:

.. code-block:: text
    
    ska-sdp-batch-preprocess --help


At the moment, the pipeline is only a stub and just copies the provided input directories
to the output directory specified via ``--outdir``. For example:

.. code-block:: text

    ska-sdp-batch-preprocess --output-dir /path/to/output_dir input1/ input2/
