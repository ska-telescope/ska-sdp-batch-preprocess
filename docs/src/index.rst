.. SKA Batch Preprocessing Pipeline documentation master file, created by
   sphinx-quickstart on Mon Jul 22 16:39:14 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SKA Batch Preprocessing Pipeline
============================================================
Pre-processing involves steps that generally must be done on all data before imaging/calibration.
It includes RFI Flagging, RFI Masking, Averaging (time/frequency) and demixing. RFI mitigation is needed to remove
the frequency channels that have been corrupted by sources causing interference during the observation; it is implemented through masking and flagging 
known and unknown sources of RFI using the FluctuFlagger and the RFI-Masking processing function (from `ska-sdp-func` and `ska-sdp-func-python`).
Averaging on time and frequency is usually intended to overcome computational bottlenecks via reducing data sizes, but it is also used in reducing noise within the data. Averaging 
is made available through `ska-sdp-func-python`.

The documentation describes how the Batch Preprocessing Pipeline works, how to use it and how to distribute the operations on multiple nodes.

- See the :ref: 'installation' page for installation instructions.
- See the :ref: 'pipeline' page to see how everything connects and the overall workflow.
- See the :ref: 'measurement_set' page to understand how measurment sets are handled by the pipeline.
- See the :ref: 'processing_funcs' page for more information on the processing functions available.
- See the :ref: 'csd3_workflow' page to see an example of how to run the pipeline on CSD3.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   installation
   pipeline
   measurement_set
   processing_funcs
   csd3_workflow

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
