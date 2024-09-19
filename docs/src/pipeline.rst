.. _pipeline:

**************
Pipeline Usage
**************

Introduction
============

Pre-processing is a step mostly to be executed before calibration and imaging. As a consequence of the high sensitivity of radio interferometers, the signal received is highly
susceptible to radio frequency interfernce (RFI) which needs to be removed. Additonally, especially in low frequency interferometers, the signal is dominated by a few sources which need to be removed.
There is also a need to reduce the overall computational cost of the entire data reduction pipeline and decreasing the noise level of the signal. To achieve these tasks, the following
processing steps can be executed:

* RFI-Masking - Masks the frequencies containing interference.
* RFI-Flagging - Detects the frequencies containing interference and flags them to be ignored in further processing stages. 
* Time Averaging - Averages the data on the time axis.
* Frequency Averaging - Averages the data on the frequency axis.
* Demixing - Detects and carefully removes the impact of bright sources from the data.

This pipeline distributes the aforementioned processing steps on multiple nodes to increase scalability and process the data in a controlled manner prior to imaging/calibration. It reads both MSv2 and MSv4, chunks the datasets on time/frequency axes, and processes them based on
user-defined steps enclosed within and passed as a configuration file.

Here is a flowchart that describes a simplified workflow of the pipeline:

.. image:: _static/preprocess_pipeline.png
   :height: 200px
   :width: 60%

The general pipeline usage entails installation (see relevant page), followed by navigating to the parent directory and running the following:

.. code-block:: bash
$ python3 ./src/ska-sdp-batch-preprocess [--options] ${path/to/your/input/measurement_set.ms}

Options include:
 
 * `--config` - Directory for the `.yml` configuration file (defaults to `../ska-sdp-batch-preprocess/config/config_default.yml`).
 * `--cmd_logs` - No arguments taken. Prompts the pipeline to output logs on the command-line in addition to logging into a logfile.
 * `--scheduler` - Address of a DASK scheduler to use for distribution (defaults to the local cluster).

User-defined configuration
==========================

The pipeline enables (and demands) user-defined configuration as to how it is supposed to process the input data. This is done within a `.yml` file which currently takes the following options: 

 * `processing_chain:` -  Define this to tell the pipeline to run a chain of processing functions, taking the following sub-options:
  * `load_ms:` - If loading MSv2, pass any desired optional arguments listed in `ska-sdp-datamodels` (https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/visibility/vis_io_ms.py?ref_type=heads#L242); if loading MSv4, pass any desired optional arguments listed in `xradio` (https://github.com/casangi/xradio/blob/e8b2d63f0e23fcf324a0ca7602de6af4a448c59c/src/xradio/vis/read_processing_set.py#L9). Take care not to conflict the operations of this pipeline as described here with external optional arguments.
  * `axis:` - Choose between `"frequency"` or `"time"` to chunk the visibility data on.
  * `chunksize:` - Define the number of `"frequency"` or `"time"` chunks. Defaults to `10` `"frequency"` chunks.
  * `apply_rfi_masks:` - Provide a list of frequency ranges to be masked; this must be under a sub-option `rfi_frequency_masks:` and the list provided should be of shape `N*2`.
  * `averaging_frequency:` - Average in frequency providing a `freqstep:` and a `flag_threshold:` sub-arguments, whose deafults are `4` and `0.4`, respectively.
  * `averaging_time:` - Average in time providing a `timestep:` and a `flag_threshold:` sub-arguments, whose deafults are `4` and `0.4`, respectively.
  * `rfi_flagger:` - Where desired, prodide `alpha:`, `magnitude:`, `variation:`, `broadband:`, `sampling:`, `window:` and/or `median_history:` sub-arguments.
  * `export_to_msv2:` - Export the processed data as MSv2. Pass any desired optional arguments listed in `ska-sdp-datamodels` (https://gitlab.com/ska-telescope/sdp/ska-sdp-datamodels/-/blob/main/src/ska_sdp_datamodels/visibility/vis_io_ms.py?ref_type=heads#L93).

 * `convert_msv2_to_msv4:` - Pass any desired optional arguments listed in `xradio` (https://github.com/casangi/xradio/blob/e8b2d63f0e23fcf324a0ca7602de6af4a448c59c/src/xradio/vis/convert_msv2_to_processing_set.py#L11). Take care not to conflict the operations of this pipeline as described here with external optional arguments.
   https://github.com/casangi/xradio/blob/c683d8927c431e2b81a1e0ae1fdabc36b77f05d4/src/xradio/vis/convert_msv2_to_processing_set.py#L13

Pass your `.yml` file into the pipeline via calling the optional command-line flag `--config` and passing the path.