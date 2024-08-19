##!/bin/bash
#
# Batch script for submitting Batch Pre-Processing jobs on CSD3
# Adjust requested resources and variables as necessary
#
####Inspired by the CIP batch script by Vincent Morello########
#
#SBATCH --job-name=preprocess
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=76
#SBATCH --time=00:15:00
#SBATCH --exclusive
#SBATCH --partition=icelake
#SBATCH --account=SKA-SDHP-SL2-CPU
#SBATCH --signal=B:TERM@120

###############################################################################
# Variables to adjust
###############################################################################
# Input measurement set directory
INPUT_MS="${HOME}/rds/hpc-work/test_data/"
REPO_DIR= "${HOME}/rds/hpc-work/pre_process/"
ACTIVATION_DIR= "${REPO_DIR}/ska-sdp-batch-preprocess-/src/ska_sdp_batch_preprocess/"
# Activation command for the Python env in which the pipeline is installed
# You may have to replace this with the equivalent command
# for the virtualenv manager you are using
VENV_ACTIVATION_COMMAND="source ${HOME}/rds/hpc-work/pre_process/preprocess/bin/activate"
OUTPUT_DIR= "${HOME/rds/hpc-work/pre_process/dask_output}"
# Dask options
DASK_WORKERS_PER_NODE=1

###############################################################################
# Anything below should not be edited
###############################################################################

# Load required base modules for icelake + recent Python
module purge
module load rhel8/default-icl
module load python/3.11.0-icl

set -x

mkdir -p $OUTPUT_DIR

# Fetch list of nodes
NODES=($(scontrol show hostnames))
HEAD_NODE="$(hostname)"

echo "Allocated nodes: ${NODES[*]}"
echo "Head node: $HEAD_NODE"

DASK_SCHEDULER_PORT=8786
DASK_SCHEDULER_ADDRESS="${HEAD_NODE}:${DASK_SCHEDULER_PORT}"
DASK_RESOURCES_ARGUMENT="--resources processing_slots=1"
DASK_WORKER_COMMAND="dask worker ${DASK_SCHEDULER_ADDRESS} --nworkers ${DASK_WORKERS_PER_NODE} ${DASK_RESOURCES_ARGUMENT}"
DASK_LOGS_DIR=${OUTPUT_DIR}

##### Launch dask scheduler on head node #####
${VENV_ACTIVATION_COMMAND}
dask scheduler --port ${DASK_SCHEDULER_PORT} >$DASK_LOGS_DIR/scheduler_$HEAD_NODE.log 2>&1 &
echo "Started dask scheduler on $DASK_SCHEDULER_ADDRESS"

##### Start dask workers on all nodes via ssh (including on head node, it's fine) #####
# We need to make a shell script to:
# 1. Activate the same python environment on all nodes
# 2. Launch the dask workers
DASK_WORKER_LAUNCH_SCRIPT=${OUTPUT_DIR}/launch_dask_worker.sh
echo "#!/bin/bash" >$DASK_WORKER_LAUNCH_SCRIPT
echo $VENV_ACTIVATION_COMMAND >>$DASK_WORKER_LAUNCH_SCRIPT
echo $DASK_WORKER_COMMAND >>$DASK_WORKER_LAUNCH_SCRIPT
chmod u+x $DASK_WORKER_LAUNCH_SCRIPT

for node in "${NODES[@]}"; do
    logfile=$DASK_LOGS_DIR/worker_$node.log
    echo "Starting dask worker on $node"
    ssh $node ${DASK_WORKER_LAUNCH_SCRIPT} >$logfile 2>&1 &
done

echo "Waiting for workers to start"
sleep 30

### Launch pipeline
cd ${REPO_DIR}
python3 ${ACTIVATION_DIR} ${INPUT_MS} \
    --scheduler ${DASK_SCHEDULER_ADDRESS} \

