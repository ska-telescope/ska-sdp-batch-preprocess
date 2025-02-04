.. _pipeline:

**************
Pipeline Usage
**************

Interface
=========

The pipeline is a CLI app; a typical usage might be:

.. code-block:: text
    
    ska-sdp-batch-preprocess
        --config myConfig.yaml
        --output-dir /path/to/base_output_dir
        --solutions-dir /path/to/solution_tables_dir
        --dask-scheduler localhost:8786
        input1.ms
        input2.ms
        ...

The pipeline works on a **single configuration, multiple data basis**: the same
sequence of pre-processing steps defined by the configuration file is applied
to each of the input measurement sets.

For each input ``<BASE_INPUT_NAME>.ms``, the associated output MSv2 path is
``<OUTPUT_DIR>/<BASE_INPUT_NAME>.ms``.

**Positional arguments:**

- One or more input MeasurementSet(s) -- in MSv2 format.

**Required keyword arguments:**

- ``--config``: A configuration file in YAML format (see below), that specifies which pre-processing steps
  should be run, in which order and their parameters.
- ``--output-dir``: An output directory for the pre-processed output MeasurementSet(s).

**Optional keyword arguments:**

- ``--solutions-dir``: Optional path to a directory where the calibration solution tables to apply
  are stored. This argument exists so that the user can avoid writing a new configuration file
  for each pipeline run, as the path to the solution tables may change on a per-dataset basis.
  Any solution table paths that appear in the config file and that are not absolute paths will be
  preprended with this directory.
- ``--dask-scheduler``: Optional network address of a dask scheduler. If provided, the associated
  dask workers are used for parallel processing.

.. note::

  When using distribution, the pipeline expects workers to define a
  `dask resource <https://distributed.dask.org/en/latest/resources.html#worker-resources>`_
  called ``process`` and each worker to hold exactly 1 of it. Make sure to
  launch workers with the command below. See :ref:`dask` section for details.

  .. code-block:: bash

    dask worker <SCHEDULER_ADDRESS> <OPTIONS> --resources "process=1"


Configuration file
==================

The batch pre-processing pipeline is simple: it translates the
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
  ``msin``, ``msout``, ``preflagger``, ``aoflagger``, ``applycal``, ``averager``.
- Any step parameter recognized by DP3 is accepted. Note that some parameters are managed by the
  pipeline and will be overriden, input and output file paths for example. For a list of parameters
  names, see the `DP3 steps documentation <https://dp3.readthedocs.io/en/latest/>`_.
- Specifying ``msin`` or ``msout`` is optional, these steps are always automatically added. However,
  specifying them allows to override some of their default options. Both ``msin`` and ``msout`` can
  be specified at most once.


Notes on ApplyCal
=================

DP3 can apply existing calibration solutions stored in so-called H5Parm files,
which are HDF5 files following a certain schema. There are a few things to be
aware of:

- H5Parm files can store an arbitrary number of solution tables, and DP3 
  needs to be told which one(s) to apply.

- The exact ApplyCal options that must be given to DP3 depend on the type of
  solution table to apply -- there are at least 3 different cases to handle.

The caller of DP3 must therefore know precisely what is inside an H5Parm file
to properly configure ApplyCal step(s). The good news is that the batch
pre-processing pipeline takes care of this process; one only needs to provide
the H5Parm file path to apply when specifying an ApplyCal step, via the ``parmdb``
configuration parameter. Here are two valid examples:

  .. code-block:: yaml

    steps:
      - ApplyCal:
        parmdb: /absolute/path/to/somefile.h5


  .. code-block:: yaml

    steps:
      - ApplyCal:
        # Relative paths get preprended by the --solutions-dir CLI argument
        parmdb: somefile.h5

**This ease of use, however, comes at the following price:**

.. warning::

  The batch pre-processing pipeline will only accept H5Parm files with a
  schema/layout such that there is only one possible way of applying them.

An error message will be raised if the ApplyCal configuration cannot be
deduced from the contents of the H5Parm.

H5Parm restrictions
-------------------

Some documentation about H5Parm and its schema can be found in the
`LOFAR Imaging Cookbook <https://support.astron.nl/LOFARImagingCookbook/losoto.html#h5parm>`_.
The batch pre-processing pipeline enforces the following additional restrictions
on the H5Parm files it accepts for its ApplyCal steps:

- Only one solution set (solset)
- Either 1 or 2 solution tables (soltab) in the solset.
- Soltab types must be either "amplitude" or "phase"; the soltab type is stored in its ``TITLE`` attribute.
- If there are 2 soltabs, they must represent amplitude and phase, and their
  number of polarisations must be identical.
- If there is only 1 soltab, it can only represent the phase or amplitude part
  of a scalar or diagonal solution table.


.. _dask:

Dask distribution
=================

In distributed mode, the batch pre-processing pipeline runs multiple DP3 tasks
in parallel on a dask cluster. Dask has no mechanism to detect how many threads
a task uses, and assumes that every task uses 1 thread from the worker's own
Python ``ThreadPool``. This is problematic when running C/C++ code spawning its
own pool of threads on the side, like DP3.

The only reliable solution is to use
`worker resources <https://distributed.dask.org/en/latest/resources.html#worker-resources>`_.
The batch pre-processing pipeline assumes that all workers define a resource
called ``process``; each worker holds 1, and each DP3 task is defined as
requiring 1. When a DP3 task reaches a worker, DP3 is launched with the same
number of threads as the worker officially owns. A worker thus only ever runs
one task at a time, and all threads are used without risk of over-subscription.

The drawback is that resources can only be defined when the workers are
launched; make sure to add ``--resources "process=1"`` to the command when you
do so:

.. code-block:: bash

  dask worker <SCHEDULER_ADDRESS> <OPTIONS> --resources "process=1"

.. warning::

  If the ``process`` resource is not defined on any worker, the pipeline
  (or rather, the dask scheduler) will hang indefinitely.
