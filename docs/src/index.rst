********************************
SKA Batch Preprocessing Pipeline
********************************

The Batch Preprocessing Pipeline prepares visibility data in MSv2 format
before they can be sent off for self-calibration and imaging. The stages are
listed below, in the typical order they should be run:

- **Static flagging**: flagging a set of user-provided channel frequency ranges
- **Dynamic flagging**: smarter flagging of visibility data based on various heuristics
- **Application of calibration solutions**, derived from observing calibrator sources and provided by the instrumental calibration pipeline
- **Averaging of the data** in both time and frequency

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   pipeline
   devguide
