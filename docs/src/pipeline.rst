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

Example
-------

.. literalinclude:: ../../config/config.yaml
  :language: yaml


Schema
------

The config file layout rules are:

- There *must* be a ``steps`` section, which must be a list of step specifications (see below) or an empty list.
  An empty list corresponds to a pipeline that just copies the input data.
- Steps are specified as a dictionary ``{step_type: {step_params_dict}}``. ``step_params_dict`` can be omitted,
  which means run the associated step with default parameters. All the following examples are valid:

  .. code-block:: yaml

    # OK, use default params
    steps:
      - MsIn:

  .. code-block:: yaml

    # OK, use default params
    steps:
      - MsIn: {}

  .. code-block:: yaml

    # OK, override msin.datacolumn
    steps:
      - MsIn:
          datacolumn: CORRECTED_DATA

- Parameters of a step must be provided in their natural data type.
- Step types are *not* case-sensitive.
- Steps are executed in the order they are specified.
- The following DP3 step types are allowed:
  ``msin``, ``msout``, ``preflagger``, ``aoflagger``, ``averager``.
- Any step parameter recognized by DP3 is accepted. Note that some parameters are managed by the
  pipeline and will be overriden, input and output file paths for example. For a list of parameters
  names, see the `DP3 steps documentation. <https://dp3.readthedocs.io/en/latest/>`_.
- Specifying ``msin`` or ``msout`` is optional, these steps are always automatically added. However,
  specifying them allows to override some of their default options. Both ``msin`` and ``msout`` can
  be specified at most once.


.. note::

  Step parameters are schema-validated to a limited extent. However, DP3 will aise a helpful error
  message if a parameter is incorrectly named or formatted.
