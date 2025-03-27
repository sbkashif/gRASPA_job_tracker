#!/bin/bash
#SBATCH --job-name="BATCH_${BATCH_NUMBER}"
#SBATCH --partition=gpuA100x4
#SBATCH --mem=50G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=${NUM_SAMPLES}
#SBATCH --gpus-per-node=1
#SBATCH --gpu-bind=closest
#SBATCH --account=bcvz-delta-gpu
#SBATCH --no-requeue
#SBATCH -t 5:00:00
#SBATCH -o batch386_N20_%j.out

# Header for easier identification in output file
echo "=========================================================="
echo "  MOF ADSORPTION SIMULATION - BATCH 386"
echo "  Number of samples: 20"
echo "  Job ID: $SLURM_JOB_ID"
echo "  Started: $(date)"
echo "=========================================================="

hostname
echo "Using $SLURM_CPUS_ON_NODE CPUs"
grep -m 1 'model name' /proc/cpuinfo
nvidia-smi --query-gpu=name --format=csv,noheader

cd $SLURM_SUBMIT_DIR
module purge
module load anaconda3_gpu
source deactivate graspa
export LD_LIBRARY_PATH=$HOME/software/gcc-13.3.0/lib64:$LD_LIBRARY_PATH
source activate graspa

# Run the batch processing script
./run_batch_num_w_track.sh ${BATCH_NUMBER} ${NUM_SAMPLES} $SLURM_JOB_ID
