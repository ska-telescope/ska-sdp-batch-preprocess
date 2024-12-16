.. _installation:

************
Installation
************

Installing DP3
==============

The batch pre-processing pipeline needs to call `DP3 <https://dp3.readthedocs.io/en/latest/>`_
(LOFAR's own preprocessing pipeline) as a subprocess. All that matters is that
the DP3 executable is available in the `PATH`.

Detailed DP3 installation instructions will be provided in the future. At the moment, we recommend
building it with `Spack <https://spack.io/>`_,
using the DP3 build recipe provided as part of the
`ska-sdp-spack repository <https://gitlab.com/ska-telescope/sdp/ska-sdp-spack>`_.


Installing poetry
=================

``poetry`` is the official Python package manager for SKAO repositories. The recommended method
for installing ``poetry`` is via ``pipx``, because ``pipx`` allows running poetry in its own separate
virtual environment while making it available system-wide.

Instructions for `installing pipx can be found here. <https://github.com/pypa/pipx>`_ Once that is done,
just run:

.. code-block:: text

    pipx install poetry


Installing the batch pre-processing pipeline
============================================

Clone the repository
---------------------

Navigate to the parent directory of your choice, clone preferably with SSH if you plan on contributing: 

.. code-block:: text

    git clone git@gitlab.com:ska-telescope/sdp/science-pipeline-workflows/ska-sdp-batch-preprocess.git


Create a Python environment
---------------------------

It is highly recommended to install the pipeline inside a dedicated environment;
here we use ``venv``, which part of the standard library -- again, alternatives are available.
We will store the pipeline's virtual environment in a ``.venv/`` sub-directory of the repository,
which is what ``poetry`` expects by default.

.. code-block:: text

    cd ska-sdp-batch-preprocess/
    python -m venv .venv/

We can now install the pipeline and its dependencies; they will all be placed inside the ``.venv/``
directory.

.. code-block:: text

    poetry install


Activate the environment and verify the installation
----------------------------------------------------

The environment we just created needs to be activated before we can use the pipeline:

.. code-block:: text

    source .venv/bin/activate

It should now be possible to run:

.. code-block:: text

    ska-sdp-batch-preprocess --version
