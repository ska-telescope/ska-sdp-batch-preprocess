.. SKA Batch Preprocessing Pipeline documentation master file, created by
   sphinx-quickstart on Mon Jul 22 16:39:14 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SKA Batch Preprocessing Pipeline
============================================================
Pre-processing involves steps that must be done on all data before they can be used for calibration and imaging.
It includes RFI Flagging, RFI Masking, Averaging (Time and Frequency) and demixing. RFI mitigation is needed to remove
the frequency channels that have been corrupted by sources that cause interfernce during the observation, it is implemented by masking and flagging 
known and unknown sources of RFI using the FluctuFlagger and the RFI-Masking processing function implemented in ska-sdp-func and in ska-sdp-func-python.
Averaging on time and frequency is usually to reduce the data size for computational reasons but it is also used for reducing noise within the data, it 
is similarly implemented in the ska-sdp-func-python repo. 

The documentation describes how the Batch Preprocessing Pipeline works and how it's implemented using the functions from ska-sdp-func and ska-sdp-func-python.

.. toctree::
   :maxdepth: 2
   :caption: Contents:




* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
