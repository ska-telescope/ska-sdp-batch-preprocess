.. _csd3_workflow:

**************
CSD3 Workflow
**************

An example of a standard workflow on CSD3 would be:

* Load correct python module:
    .. code-block::
        module load python/3.11.0-icl

* Create and activate virtual environment:    
    .. code-block::
        python3 -m venv venv_name
        source venv_name/bin/activate
    
* Install poetry: 
    .. code-block::
        pip3 install poetry
        
* Clone git repo and install the pipeline: 
    .. code-block::
        git clone https://gitlab.com/ska-telescope/sdp/science-pipeline-workflows/ska-sdp-batch-preprocess.git
        cd ska-sdp-batch-preprocess
        poetry install

* Run batch script:
    .. code-block::
        sbatch script.sh

An exampe batch script for CSD3 can be found in slurm/csd3.sh

