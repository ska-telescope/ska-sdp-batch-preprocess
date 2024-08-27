# SKA SDP Batch Preprocessing Pipeline
Distributed preprocessing pipeline with the functionality for averaging on time, frequency; rfi masking and rfi flagging. 
## Description
This is a dask distributed pipeline for preprocessing, specifically the pipeline can average on time and frequency, it can add rfi masks, and has the ability to flag rfi. The pipeline can read `MSv2` or `MSv4` and can execute the preprocessing functions in any configuration with user defined parameters through a `yaml` config file. The processing functions can be distributed on either the time or frequency axis depending on the user, similarly the chunk size can be defined by the user in the config file. The pipeline has the ability to export the processed Measurement Sets in the `MSv2` or `MSv4` format. The pipeline can also convert between `MSv2` or `MSv4` either in memory or on disk.  Additionally, there is functionality to map processing functions on a single node if they are embarrassingly parallel. 

## Installation
To install the pipeline please make sure to use a virtual environment either with `conda` or `python`, and use the following command in the root directory of the repository:
 ```
poetry install
``` 
This will install the dependencies of the pipeline and install the latest release. 
## Usage
Parameters expected from the pipeline are:
- Measurement Set Directory
- Config File Directory (optional)
- Dask Scheduler Address (optional)
- Command Logs (optional)

Example pipeline run command from the root directory:
```
python3 src/ska_sdp_batch_preprocess/ ms_dir --config config_dir --cmd_logs --scheduler dask_scheduler_address
```


To run the pipeline, the `yaml`config file needs to include the processing functions and the commands to read,write or convert measurement sets. Further arguments for the pipeline can be added via the config according to user needs. Further instructions on how to structure the config file can be found in the default `config_default.yml` file.


## Dependencies
- casacore (for MSv2 functionality)
- XRadio (for MSv4 functionality)
- ska-sdp-func-python
- ska-sdp-func
- ska-sdp-datamodels

Normally these dependencies should be resolved with poetry, however since some of these repos are still under active development the pipeline might break due to unforeseen changes.
## Notes

The pipeline does not employ aoflagger for rfi flagging in it's current state. The RFI Flagger utilised here is the RFI Flagger implemented in ska-sdp-func, the pipeline can certainly include aoflagger in the future but since it's a dependency that is hard to resolve for poetry it would need major changes, mainly depending on singularity images.