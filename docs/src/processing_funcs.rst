
********************
Processing Functions
********************

All the processing functions currently take SKA data model visibility as input and produces output in the same format.

RFI-Masking
============

This function applies a range of static flags to the channels known to be prone to RFI (e.g. because of a tv station or a satellite, etc). The masks can be provided as a parameter
to the pipeline's YAML configuration file. Please see the default configuration file in the pipeline as an example on how to provide the masks.

RFI-Flagging
============

We currently have two options of algorithms for detecting and flagging RFI in the visibility data:

* FluctuFlagger: A lightweight RFI flagger that conducts a statistical analysis on the data and detects anamolies in the visibility magnitudes. The methods in the algorithm can detect these types of anomalies:
excessively large values at each time sample across all channels, excessively fluctuating channels in the 'recent' time samples, and time samples where all or most channels 
have an excessively higher than usual values. Therefore, a wide range of strong, weak, and broadband types of RFI can be detected. The algorithm is designed to balance between accuracy and 
computation cost. The default values for the parameters are fairly suitable for most scenarios, however, the user of the pipeline can easily modify these values if needed. Please see the corresponding 
documentations in the SKA Python processing functions documentations in https://developer.skao.int/projects/ska-sdp-func-python/en/latest/

* AOFlagger: A widely used RFI flagging package in different telescopes. Details can be found in its documentations at https://aoflagger.readthedocs.io/en/latest/

Averaging
=========

We have two functions for averaging in time and frequency axis. These functions will take care of other quantities that may change during averaging (depending on the axis)
such as uvw's, channel frequencies and bandwidths, integration times, etc.
