# SKA SDP Batch Preprocessing Pipeline
Distributed preprocessing pipeline with the functionality for averaging on time, frequency; rfi masking and rfi flagging. 

## Description
This is a DASK distributed pipeline for preprocessing. It can average on time/frequency, add rfi masks, and flag rfi. The pipeline can read `MSv2` or `MSv4` and can execute the preprocessing functions in any configuration with user defined parameters through a `.yml` configuration file. The processing functions can be distributed on either time or frequency axes depending on what is demanded; similarly, the chunk size can be defined by the user in the configuration file. The pipeline can also export processed data as `MSv2`, convert `MSv2` to `MSv4` on disk, and interchangeably convert between `MSv2` and `MSv4` in-memory. Additionally, there is functionality to map processing functions on a single node if they are embarrassingly parallel.

## Installation
To install the pipeline please make sure to use a virtual environment either with `conda` or `python`, and use the following command in the root directory of the repository:

```
poetry install
``` 

This will install the dependencies of the pipeline and install the latest release.

## Usage
Parameters expected from the pipeline are:
- Measurement Set Directory
- Configuration File Directory (optional)
- DASK Scheduler Address (optional)
- Command Logs Prompt (optional)

Example pipeline run command from the root directory:

```
$ python3 src/ska_sdp_batch_preprocess/ --cmd_logs --config [CONFIG] --scheduler [SCHEDULER_ADDRESS]  {ms_dir}
```

To run the pipeline, the `.yml` configuration file needs to include certain commands promting the various operations. These are detailed in the pipeline documentation. If no configuration file is passed, pipeline will run `config_default.yml` by default.

## Dependencies
- `casacore` and `ska-sdp-datamodels` (for MSv2 functionality)
- `xradio` (for MSv4 functionality)
- `ska-sdp-func-python`
- `ska-sdp-func`

Normally these dependencies should be resolved with poetry, however since some of these repositories are still under active development, the pipeline might break due to unforeseen changes.

## Notes
The pipeline does not employ `aoflagger` for rfi flagging in it's current state, but instead, rfi flagging utilises `ska-sdp-func`.