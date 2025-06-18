#!/bin/bash

#SBATCH --job-name=g_500
#SBATCH -o /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/job_logs/batch_500_%j.out
#SBATCH -e /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/job_logs/batch_500_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=1
#SBATCH --cpus-per-task=20
#SBATCH --gpu-bind=closest
#SBATCH --no-requeue

#SBATCH --account=bcvz-delta-gpu
#SBATCH --partition=gpuA100x4
#SBATCH --time=5:00:00
#SBATCH --nodes=1
#SBATCH --mem=50GB

cd $SLURM_SUBMIT_DIR
module load anaconda3_gpu
source deactivate graspa
export LD_LIBRARY_PATH=$HOME/software/gcc-13.3.0/lib64:$LD_LIBRARY_PATH
source activate graspa


# Set GRASPA environment variables
export GRASPA_SCRIPTS_DIR="gRASPA_job_tracker/scripts"
export GRASPA_ROOT=""

# Set environment variables for forcefield paths
export FF_FORCE_FIELD_MIXING_RULES="/projects/bcvz/sbinkashif/gRASPA_job_tracker/forcefields/N2-Forcefield/force_field_mixing_rules.def"
export FF_FORCE_FIELD="/projects/bcvz/sbinkashif/gRASPA_job_tracker/forcefields/N2-Forcefield/force_field.def"
export FF_PSEUDO_ATOMS="/projects/bcvz/sbinkashif/gRASPA_job_tracker/forcefields/N2-Forcefield/pseudo_atoms.def"
export FF_CO2="/projects/bcvz/sbinkashif/gRASPA_job_tracker/forcefields/N2-Forcefield/CO2.def"
export FF_N2="/projects/bcvz/sbinkashif/gRASPA_job_tracker/forcefields/N2-Forcefield/N2.def"

# Handle simulation template and variables
export TEMPLATE_SIMULATION_INPUT="/projects/bcvz/sbinkashif/gRASPA_job_tracker/templates/simulation.input"
export SIM_VAR_NUMBEROFINITIALIZATIONCYCLES="2000000"
export SIM_VAR_NUMBEROFPRODUCTIONCYCLES="2000000"
export SIM_VAR_MOVIESEVERY="3000000"
# Create list of CIF files for this batch
cat > /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/cif_file_list.txt << 'EOF'
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WIYWUY_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WIYZOU_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WIZDEP_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WIZKOH_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBBOG_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBCAT_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBCIB_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBFOK_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBHEB01_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBHEB_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBHOL_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBHOM_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOBQEL_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOCGIG_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WOCWAN_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WODFOL_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WODPUC_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WODQUD_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WODRAK_clean.cif
/projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/raw/WODREO_clean.cif
EOF

echo 'Starting job for batch 500 with 20 CIF files'
echo 'Job started at: ' `date`

echo 'Step 1: Partial Charge'
mkdir -p /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/partial_charge
python -m gRASPA_job_tracker.scripts.generate_partial_charge 500 /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/cif_file_list.txt /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/partial_charge
partial_charge_status=$?
if [ $partial_charge_status -ne 0 ]; then
    echo 'partial_charge failed'
    echo '500' >> /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/failed_batches.txt
    exit 1
fi

echo $? > /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/partial_charge/exit_status.log
echo 'Step 2: Simulation'
mkdir -p /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/simulation
# Change to output directory
cd /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/simulation
cp /projects/bcvz/sbinkashif/gRASPA_job_tracker/gRASPA_job_tracker/scripts/mps_run.sh ./simulation_mps_run.sh
chmod +x ./simulation_mps_run.sh
# Copy and modify template with variables
cp /projects/bcvz/sbinkashif/gRASPA_job_tracker/templates/simulation.input ./simulation_template.input
# Simple variable replacement for template
if grep -q "^NumberOfInitializationCycles" ./simulation_template.input; then
  # Replace existing variable
  sed -i "s/^NumberOfInitializationCycles.*/NumberOfInitializationCycles 2000000/" ./simulation_template.input
else
  # Add variable if it doesn't exist
  echo "NumberOfInitializationCycles 2000000" >> ./simulation_template.input
fi
if grep -q "^NumberOfProductionCycles" ./simulation_template.input; then
  # Replace existing variable
  sed -i "s/^NumberOfProductionCycles.*/NumberOfProductionCycles 2000000/" ./simulation_template.input
else
  # Add variable if it doesn't exist
  echo "NumberOfProductionCycles 2000000" >> ./simulation_template.input
fi
if grep -q "^MoviesEvery" ./simulation_template.input; then
  # Replace existing variable
  sed -i "s/^MoviesEvery.*/MoviesEvery 3000000/" ./simulation_template.input
else
  # Add variable if it doesn't exist
  echo "MoviesEvery 3000000" >> ./simulation_template.input
fi
export TEMPLATE_SIMULATION_INPUT="$(pwd)/simulation_template.input"
# Execute script locally
# Using previous step output directory as input
bash ./simulation_mps_run.sh 500 /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/partial_charge /projects/bcvz/sbinkashif/gRASPA_job_tracker/gRASPA_job_tracker/scripts .
script_status=$?
if [ $script_status -eq 0 ]; then
    # Clean up unnecessary files on success
    rm -f ./simulation_mps_run.sh
fi
cd -
# Avoid exit for simulation scripts to prevent premature job termination
simulation_status=$?
if [ $simulation_status -ne 0 ]; then
    echo 'simulation failed'
    echo '500' >> /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/failed_batches.txt
    exit 1
fi

# Ensure transition to the next step after simulation
echo 'Simulation step completed. Proceeding to analysis...'

echo $? > /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/simulation/exit_status.log
echo 'Step 3: Analysis'
mkdir -p /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/analysis
python -m gRASPA_job_tracker.scripts.analyze_batch_output 500 /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/simulation /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/analysis
analysis_status=$?
if [ $analysis_status -ne 0 ]; then
    echo 'analysis failed'
    echo '500' >> /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/failed_batches.txt
    exit 1
fi

echo $? > /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/analysis/exit_status.log

# Write completion status
echo $? > /projects/bcvz/sbinkashif/gRASPA_job_tracker/examples/data/coremof_clean/results/batch_500/exit_status.log
echo 'Job completed at: ' `date`
