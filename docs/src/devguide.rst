.. _devguide:

Developer Guide
===============

This section contains information for code contributors.


CI Pipeline simulation
----------------------

Before pushing, you can check whether the CI pipeline should pass as follows:

.. code-block::

    make ci

This runs the following stages:

    1. Lint
    2. Test
    3. Docs build with all warnings treated as errors
