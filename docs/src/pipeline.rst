.. _pipeline:

**************
Pipeline Usage
**************

Interface
=========

The pipeline is a CLI app; the typical usage is:

.. code-block:: text
    
    ska-sdp-batch-preprocess
        --config myConfig.yaml
        --output-dir /path/to/base_output_dir
        input1.ms input2.ms

The pipeline works on a **single configuration, multiple data basis**: the sequence of
pre-processing steps defined by the configuration file is applied to each of the
input measurement sets.

**Positional arguments:**

- One or more input MeasurementSet(s) -- in MSv2 format.

**Required keyword arguments:**

- A configuration file in YAML format (see below), that specifies which pre-processing steps
  should be run, in which order and their parameters.
- An output directory for the pre-processed output MeasurementSet(s).

For each input ``<BASE_INPUT_NAME>.ms``, the associated output MSv2 path is
``<OUTPUT_DIR>/<BASE_INPUT_NAME>.ms``.


Configuration file
==================

The batch pre-processing pipeline is quite simple: it translates the
configuration file into a sequence of calls to DP3, one per input MSv2, and
executes them as subprocesses. The configuration file schema reflects this: it
provides the means to specify a list of DP3 steps and their parameters.

.. warning::

   The configuration file is currently **not** schema-validated -- this will be implemented later.
   However, DP3 will raise a helpful error message if a parameter was incorrectly named or
   formatted.


An example is provided as part of the repository, reproduced below for convenience:

.. literalinclude:: ../../config/config.yaml
