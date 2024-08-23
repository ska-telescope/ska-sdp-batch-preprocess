.. _pipeline:

**************
Pipeline Usage
**************

Introduction
============

Preprocessing is a step mostly to be executed before calibration and imaging. As a consequence of the high sensitivity of radio interferometers, the signal received is highly
susceptible to radio frequency interfernce (RFI) which needs to be removed. Additonally especially in low frequency interferometers, the signal is dominated by a few sources which need to be removed.
There is also the need to reduce the overall computational cost of the entire data reduction pipeline and decreasing the noise level of the signal. To achieve these tasks, the following
processing steps are executed:

* RFI-Masking - Masks the frequencies containing interference.
* RFI-Flagging - Detects the frequencies containing interference and flags them to be ignored in further processing stages. 
* Time Averaging - Averages the data on the time axis.
* Frequency Averaging - Averages the data on the frequency axis.
* Demixing - Detects and carefully removes the impact of bright sources from the data.

The pipeline here aims to distribute these processing steps on multiple nodes to increase scalability and process the data in a controlled manner to be progressed onto the next stage of 
calibration and imaging. To achieve this we have implemented a pipeline that can read MSv2 and MSv4 datasets and chunk them on time or frequency axis, and later be preprocessed according
to a config file produced by the user's needs. 

Here is a flowchart that describes a simplified workflow of the pipeline:
.. image:: _static/preprocess_pipeline.png
   :width: 100%


