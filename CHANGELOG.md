# Changelog

## 2.3.0 - 2025-03-19

CLI interface change in preparation of adding Demixing.

### Changed

- Renamed CLI argument `--solutions-dir` to `--extra-inputs-dir`


## 2.2.0 - 2025-02-04

Added option to use dask distribution.

### Added

- Ability to distribute over multiple dask workers, where one input MS corresponds to one task.
  CLI app now accepts an optional `--dask-scheduler` argument followed by the network address of
  the scheduler to use.


## 2.1.1 - 2025-01-31

Improved H5Parm validator.

### Added

- H5Parm validator is now much stricter and will raise a helpful error message even on unlikely corner cases.


## 2.1.0 - 2025-01-29

ApplyCal step is now available to run.

### Added

- Ability to run ApplyCal step
- CLI arguments are now grouped into "required" and "optional" groups, which makes the
  batch pre-processing app help text clearer.

### Fixed

- The pipeline now checks that there are no duplicate input MeasurementSet names; if there are,
  it raises an error. This is necessary because two input paths `dir1/data.ms` and `dir2/data.ms`
  correspond to the same pre-processed output path. Previously, such a situation would have
  resulted in either a crash or some output measurement sets being overwritten.


## 2.0.0 - 2025-01-20

Preliminary release following a complete rewrite. The batch pre-processing pipeline now wraps DP3.

### Added

- Command line app can now be called via the command `ska-sdp-batch-preprocess`
- Ability to run PreFlagger step
- Ability to run AOFlagger step

### Changed

- New command line interface
- New configuration file format, where step names and options map (almost) directly to what DP3 expects.

### Removed

- Support for MSv4
- Distribution with dask; will be added back in an upcoming release


## 1.0.1 - 2024-11-29

Dummy release that was required to comply with the SKAO release process.


## 1.0.0 - 2024-11-05

Major release with the following additions:

* Bug fixed: MSv2 output did not capture all changes conducted by the requested chain of processing functions.
* New `classmethod` introduced to `MeasurementSet` allowing instances to be called directly with `list[Visibility]` inputs.
* Relevant improvements to documentation.

Progress:

* Bug fixed, enabling the output of a given processing function in the chain to be correctly passed into the next function [MR20].
* Automated release enabled including addition of `CHANGELOG.md` [MR19].


## 0.1.0 - 2024-09-30

Initial test release:

* New pipeline to preprocess visibilities within MSv2 & MSv4.
* Enables user-configurable processing functions chains.
* Supports distributed data processing via `dask`.
* Supports on-disk MSv2 --> MSv4 conversion via `xradio`.
* Supports in-memory MSv2 <--> MSv4 convertibility. 


Progress:

* Pipeline release & further documentation [MR18].
* Further improvement to pipeline documentation [MR17].
* Improve pipeline documentation and improve code structure for `.yml` configurability [MR14].
* Enable `slurm` support [MR13].
* Improve code structure for Dask distrubution functionality [MR12].
* Enable auto-detection of MS version [MR11].
* Enable loading/writing MS [MR10].
* Processing functions introduced & harmonised with the configurability of the pipeline [MR9]
* Pipeline logging structure & handling of exceptions introduced + onboarding changes in `xradio` [MR8].
* First prototype for Dask distribution deployed [MR7].
* Repository restructured & `.yml` configuration/functionality added [MR6].
* Pipeline created with classes to handle MSv2 & MSv4 in-memory [MR5].
* Distributed (Dask-based) machinery created. Minimal documentation added [MR2].
