#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Parse command line arguments
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <batch_id> <input_dir> [output_dir]"
  echo "  batch_id: Batch ID number"
  echo "  input_dir: Directory containing partial charge results"
  echo "  scripts_dir: Directory containing auxiliary scripts"
  echo "  output_dir: Optional - Output directory for simulation results (defaults to current directory)"
  exit 1
fi

batch_id=$1
input_dir=$2
scripts_dir=$3
output_dir="${4:-.}"  # Use current directory if not specified

echo "Batch ID: $batch_id"
echo "Input directory: $input_dir"
echo "Output directory: $output_dir"

# Get current directory (where the script is being run from)
currentdir=$(pwd)

# Create output directory if it doesn't exist and not already in it
if [ "$output_dir" != "." ] && [ "$output_dir" != "$currentdir" ]; then
  mkdir -p "$output_dir"
  cd "$output_dir"
fi


# Ensure input directory exists
if [ ! -d "$input_dir" ]; then
  echo "❌ Input directory $input_dir does not exist. Exiting..."
  exit 1
fi

# Collect names of processed files in input_dir (with .cif extension)
processed_files=($(ls $input_dir/*.cif 2>/dev/null | xargs -n 1 basename | sed 's/\.cif$//'))
file_count=${#processed_files[@]}

if [ $file_count -eq 0 ]; then
  echo "❌ No valid CIF structures found in $input_dir. Exiting..."
  exit 1
fi

echo "Found $file_count CIF structure(s) in the input directory."
echo "The following structures will be simulated:"
echo "${processed_files[@]}"

# Now copy the cif files
echo "Copying cif files..."
for cif_file in "$input_dir"/*.cif; do
  if [ -f "$cif_file" ]; then
    cp -v "$cif_file" .
  fi
done

# Check if all the cif files in run_names_array exist
for run_name in "${processed_files[@]}"; do
  if [ ! -f "${run_name}.cif" ]; then
    echo "❌ File ${run_name}.cif does not exist" >&2
    exit 1
  fi
done

echo "Starting simulations..."

# Write basic node information
hostname

# Print GPU information if available
if command -v nvidia-smi &> /dev/null; then
  nvidia-smi --query-gpu=name --format=csv,noheader
fi

# Set gRASPA executable path from environment variables
if [ -n "$gRASPA_executable" ]; then
  # If gRASPA_executable is set, use it directly
  gRASPA_binary="$gRASPA_executable"
else
  echo "❌ Could not find gRASPA executable. Please specify either gRASPA_executable in environment_setup section in config file."
  exit 1
fi

# Validate that the gRASPA binary exists and is executable
if [ ! -f "$gRASPA_binary" ] || [ ! -x "$gRASPA_binary" ]; then
  echo "❌ gRASPA binary not found or not executable at: $gRASPA_binary"
  exit 1
fi

echo "✅ Using gRASPA binary: $gRASPA_binary"

# Check for required auxiliary scripts and copy them from scripts directory
required_scripts=("start_as_root.sh" "stop_as_root.sh" "update_unit_cells.sh" "mincell.py")
for script in "${required_scripts[@]}"; do
  if [ -f "$scripts_dir/$script" ]; then
    cp -v "$scripts_dir/$script" .
    chmod +x "$script"
  elif [ ! -f "./$script" ]; then
    echo "❌ Required script $script not found in $scripts_dir or current directory"
    exit 1
  fi
done

# Copy forcefield files from environment variables
echo "Copying forcefield files from environment variables..."

# Mandatory forcefield files - these must be present
mandatory_forcefield_vars=(
  "FF_FORCE_FIELD_MIXING_RULES"
  "FF_FORCE_FIELD"
  "FF_PSEUDO_ATOMS"
)

# Check mandatory forcefield files first
for var in "${mandatory_forcefield_vars[@]}"; do
  if [ -n "${!var}" ] && [ -f "${!var}" ]; then
    echo "Copying mandatory file ${!var}"
    cp -v "${!var}" .
  else
    echo "❌ Error: Mandatory forcefield file ${var} not found or path is empty"
    exit 1
  fi
done

# Copy all additional molecule-specific forcefield files (any environment variable starting with FF_)
echo "Copying molecule-specific forcefield files..."
for var in $(env | grep ^FF_ | grep -v "FF_FORCE_FIELD_MIXING_RULES\|FF_FORCE_FIELD\|FF_PSEUDO_ATOMS" | cut -d= -f1); do
  if [ -n "${!var}" ] && [ -f "${!var}" ]; then
    echo "Copying ${var} = ${!var}"
    cp -v "${!var}" .
  else
    echo "⚠️ Warning: Optional forcefield file ${var} (${!var}) not found or path is empty"
  fi
done

# Run start_as_root if it exists
if [ -x "./start_as_root.sh" ]; then
  ./start_as_root.sh
fi

# Function to replace variables in simulation.input file
replace_variables() {
  local input_file=$1
  
  echo "Replacing variables in $input_file..."
  # Find all environment variables with prefix SIM_VAR_
  for var in $(env | grep ^SIM_VAR_ | cut -d= -f1); do
    # Extract the variable name without prefix
    local var_name=$(echo $var | sed 's/^SIM_VAR_//')
    # Get the variable value
    local var_value=${!var}
    echo "Replacing \${$var_name} with $var_value"
    # Replace ${VAR_NAME} with the variable value
    sed -i "s|\${$var_name}|$var_value|g" "$input_file"
  done
}

# Now loop through the run names and run the simulations
for run_name in "${processed_files[@]}"; do
  # Extract the basename without extension for the simulation directory
  base_name=$(basename "$run_name" .cif)
  echo "Starting simulation $base_name"
  mkdir -p "$base_name"
  
  # Copy necessary files - .def files from current directory (where we copied the forcefield files)
  cp -v ./*.def "$base_name/" 2>/dev/null || echo "⚠️ Warning: No .def files found"
  cp -v "${base_name}.cif" "$base_name/"
  
  # Copy and process simulation.input template 
  if [ -n "$TEMPLATE_SIMULATION_INPUT" ] && [ -f "$TEMPLATE_SIMULATION_INPUT" ]; then
    # Copy template and apply variable substitution
    cp -v "$TEMPLATE_SIMULATION_INPUT" "$base_name/simulation.input"
    replace_variables "$base_name/simulation.input"
  else
    echo "❌ Simulation input template not found. Please specify TEMPLATE_SIMULATION_INPUT."
    exit 1
  fi
  
  cd "$base_name"

  # Update unit cells
  cp -v "../update_unit_cells.sh" update_unit_cells.sh
  cp -v "../mincell.py" .

  # If the cif file contains '.' in its name, rename it
  if [[ $base_name == *"."* ]]; then
    new_run_name=$(echo $base_name | sed 's/\./_/g')
    mv -v "$base_name.cif" "$new_run_name.cif"
    base_name=$new_run_name
  fi
  
  # Update unit cells
  source update_unit_cells.sh -i simulation.input -c "${base_name}.cif"

  # Clean up
  rm -v update_unit_cells.sh
  rm -v mincell.py

  # Update  the FrameworkName in the simulation.input file
  
  #Now update the framework name in the simulation.input file
  if ! sed -i "s/^FrameworkName.*/FrameworkName $base_name/" simulation.input; then
    echo "Failed to update FrameworkName in input file: $base_name/simulation.input" >&2
    exit 1
  fi

  # Record start time for this process
  start_time=$(date +%s)
    
  # Run simulation in the background and log its PID
  ("$gRASPA_binary" > result; echo $? > exit_status.log) &
  pid=$!
  
  # Save the PID and start time for this process
  echo "$pid $start_time" >> "../process_times_${batch_id}.log"
  
  cd ..
done

# Wait for all simulations to complete
wait

# Process timing information
rm -f "timing_${batch_id}.log" 2>/dev/null || echo "Warning: Could not remove timing log file"

process_times_file="process_times_${batch_id}.log"
if [ -f "$process_times_file" ]; then
  while read pid start_time; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "❌ Process $pid is still running. Something went wrong." >> "timing_${batch_id}.log"
      continue
    fi
    end_time=$(date +%s)
    elapsed_time=$((end_time - start_time))
    echo "Simulation with PID $pid completed in $elapsed_time seconds" >> "timing_${batch_id}.log"
  done < "$process_times_file"
fi

# Run stop_as_root if it exists
if [ -x "./stop_as_root.sh" ]; then
  ./stop_as_root.sh
fi

echo "Simulations completed. Now checking for errors..."

# Check for missing or non-zero exit_status.log files
missing_logs=0
failed_simulations=0

for run_name in "${processed_files[@]}"; do
  # Look in the correct location after move_completed_jobs.sh has run
  log_path="$run_name/exit_status.log"
  
  if [ ! -f "$log_path" ]; then
    echo "❌ Missing exit_status.log for simulation $run_name"
    missing_logs=$((missing_logs + 1))
  elif [ "$(cat "$log_path" | tr -d '\n')" != "0" ]; then
    echo "❌ Simulation $run_name failed with non-zero exit code"
    failed_simulations=$((failed_simulations + 1))
  fi
done

if [ "$missing_logs" -gt 0 ]; then
  echo "❌ $missing_logs simulations are missing exit_status.log files"
  exit 1
fi

if [ "$failed_simulations" -gt 0 ]; then
  echo "❌ $failed_simulations simulations failed with non-zero exit codes"
  exit 1
fi

# Clean up inside each simulation directory first
echo "Cleaning up simulation directories..."
for run_name in "${processed_files[@]}"; do
  if [ -d "$run_name" ]; then
    echo "Cleaning directory: $run_name"

    #First copy the output files from the Output directory to the run_name directory
    #cp -v "$run_name"/Output/System_*.data "$run_name"/.
    # Keep only output data files and simulation input
    find "$run_name" -type f ! -name "System_*.data" ! -name "simulation.input" ! -name "exit_status.log" -delete
    # Clean empty subdirectories if any
    find "$run_name" -type d -empty -delete
  fi
done

# Clean up temporary files in the main directory
echo "Cleaning up temporary files..."
rm -v "$process_times_file" || echo "⚠️ Warning: Could not remove process times log"
rm -v *.def || echo "⚠️ Warning: Could not remove .def files"
rm -v *.cif || echo "⚠️ Warning: Could not remove .cif files"
rm -v *.log || echo "⚠️ Warning: Could not remove log files"
rm -v *.py || echo "⚠️ Warning: Could not remove auxiliary Python scripts"
rm -v *.sh || echo "⚠️ Warning: Could not remove auxiliary scripts"

# if no missing logs or failed simulations, print success message
if [ "$missing_logs" -eq 0 ] && [ "$failed_simulations" -eq 0 ]; then
  echo "✅ All simulations completed successfully"
else
  echo "❌ Some simulations failed or are missing logs. Please check the output."
  exit 1
fi