.. _devguide:

***************
Developer Guide
***************

This section contains information for code contributors.


Additional installation steps
=============================

Install Git LFS
---------------

The pipeline's test suite contains large files which are versioned using
the ``git-lfs`` extension, which you will need to download them.
On Debian/Ubuntu Linux:

.. code-block::

    sudo apt-get install git-lfs

Other installation methods are provided on the `Git LFS website <https://git-lfs.com/>`_.


Cloning the repository
----------------------

Contributors should clone with SSH; the ``--recurse-submodules`` option ensures that the SKAO CI/CD
Makefiles submodule is also pulled:

.. code-block:: text

    git clone --recurse-submodules git@gitlab.com:ska-telescope/sdp/science-pipeline-workflows/ska-sdp-batch-preprocess.git

After that, just follow the rest of the instructions in :ref:`installation`.


CI Pipeline simulation
======================

Before pushing new commits, you should check whether they will pass the CI pipeline as follows:

.. code-block::

    make ci

This runs the following stages:

    1. Lint
    2. Test
    3. Docs build with all warnings treated as errors
