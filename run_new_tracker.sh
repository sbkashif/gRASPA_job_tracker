#!/bin/bash
#SBATCH --job-name="dashboard"
#SBATCH --partition=gpuA100x4
#SBATCH --mem=50G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus-per-node=1
#SBATCH --gpu-bind=closest
#SBATCH --account=bcvz-delta-gpu
#SBATCH --no-requeue
#SBATCH -t 5:00:00

cd $SLURM_SUBMIT_DIR
module purge
module load anaconda3_gpu
source deactivate graspa
export LD_LIBRARY_PATH=$HOME/software/gcc-13.3.0/lib64:$LD_LIBRARY_PATH
source activate graspa

gRASPA_job_tracker -c examples/config-coremof-clean.yaml --min-batch 421 --max-batch 450 --no-confirm

