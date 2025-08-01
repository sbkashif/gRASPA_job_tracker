#!/bin/bash

# Ensure the script is run with bash
if [ -z "$BASH_VERSION" ]; then
    echo "This script must be run with bash, not sh."
    exit 1
fi
set -e  # Exit immediately if a command exits with a non-zero status

batch_id=$1

# Parse command line arguments
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <batch_id> <input_file_list> [output_dir] [template_file]"
  echo "  batch_id: Batch ID number"
  echo "  input_file_list: File containing list of CIF files (one per line, full path)"
  echo "  output_dir: Optional - Output directory for simulation results (defaults to current directory)"
  echo "  template_file: Optional - Template file for simulation input"
  exit 1
fi

batch_id=$1
input_file_list=$2
output_dir="${3:-.}"  # Use current directory if not specified
template_file="${4:-}"  # Optional template file

echo "=== RASPA Simulation Script ==="
echo "Batch ID: $batch_id"
echo "Input file list: $input_file_list"
echo "Output directory: $output_dir"
echo "Template file: $template_file"
echo "==============================="

# Write basic node information
echo "=== Node Information ==="
hostname

# Check if we're on LSB (LSF) or SLURM system
if [ -n "$LSB_MCPU_HOSTS" ]; then
    # LSF system
    nproc=$(echo $LSB_MCPU_HOSTS | cut -d " " -f2)
    echo "LSF CPUs: $nproc"
elif [ -n "$SLURM_CPUS_ON_NODE" ]; then
    # SLURM system
    echo "SLURM CPUs: $SLURM_CPUS_ON_NODE"
else
    # Fallback to nproc command
    echo "CPUs: $(nproc)"
fi

grep -m 1 'model name' /proc/cpuinfo

# Load necessary modules
echo "=== Loading Modules ==="
module load ccrsoft/2023.01
module load gcc/11.3.0
module load openmpi/4.1.4

# Specify location of modified code
export RASPA_DIR=/projects/academic/kaihangs/salmanbi/software/RASPA2-2.0.35_changed_sysctl.h/bin

# Validate RASPA executable
if [ ! -f "$RASPA_DIR/simulate" ] || [ ! -x "$RASPA_DIR/simulate" ]; then
    echo "❌ RASPA executable not found or not executable at: $RASPA_DIR/simulate"
    exit 1
fi

echo "✅ Using RASPA executable: $RASPA_DIR/simulate"

# Get current directory (where the script is being run from)
currentdir=$(pwd)

# Create output directory if it doesn't exist and change to it
if [ "$output_dir" != "." ]; then
    mkdir -p "$output_dir"
    cd "$output_dir"
fi

# Copy forcefield files from environment variables (set by job scheduler)
echo "=== Copying Forcefield Files ==="
# Copy all FF_* environment variables (dynamically find all forcefield files)
for var in $(env | grep '^FF_' | cut -d'=' -f1); do
    ff_file=${!var}
    if [ -n "$ff_file" ] && [ -f "$ff_file" ]; then
        cp -v "$ff_file" .
        echo "✅ Copied: $(basename $ff_file)"
    else
        echo "⚠️  Warning: Forcefield file not found: $ff_file (from $var)"
    fi
done


# Copy only CIF files listed in input_file_list
echo "=== Copying CIF Files Listed in File ==="
cif_files_found=0
if [ -f "$input_file_list" ]; then
    while IFS= read -r cif_path; do
        if [ -f "$cif_path" ]; then
            cp -v "$cif_path" .
            cif_files_found=1
            echo "✅ Copied: $(basename $cif_path)"
        else
            echo "⚠️  Warning: CIF file not found: $cif_path"
        fi
    done < "$input_file_list"
else
    echo "❌ CIF file list not found: $input_file_list"
    exit 1
fi

if [ $cif_files_found -eq 0 ]; then
    echo "❌ No CIF files found as listed in: $input_file_list"
    exit 1
fi

# Handle simulation input template
echo "=== Setting up Simulation Input ==="
if [ -n "$template_file" ] && [ -f "$template_file" ]; then
    cp -v "$template_file" simulation.input
    echo "✅ Using template: $template_file"
    
    # Apply variable substitutions from environment variables
    echo "=== Applying Variable Substitutions ==="
    for var in $(env | grep '^PARAM_VAR_' | cut -d'=' -f1); do
        var_name=$(echo $var | sed 's/^PARAM_VAR_//')
        var_value=${!var}
        echo "Replacing ${var_name} (case-insensitive) with ${var_value}"
        # Use sed with # as delimiter, no unnecessary escaping, case-insensitive
        sed -i "s#\${$var_name}#${var_value}#gI" simulation.input
    done
    
elif [ -n "$TEMPLATE_SIMULATION_INPUT" ] && [ -f "$TEMPLATE_SIMULATION_INPUT" ]; then
    cp -v "$TEMPLATE_SIMULATION_INPUT" simulation.input
    echo "✅ Using template from environment: $TEMPLATE_SIMULATION_INPUT"
    
    # Apply variable substitutions from environment variables
    echo "=== Applying Variable Substitutions ==="
    for var in $(env | grep '^PARAM_VAR_' | cut -d'=' -f1); do
        var_name=$(echo $var | sed 's/^PARAM_VAR_//')
        var_value=${!var}
        echo "Replacing ${var_name} with ${var_value}"
        sed -i "s#\${$var_name}#${var_value}#g" simulation.input
    done
else
    echo "❌ No simulation input template found. Please provide template_file or set TEMPLATE_SIMULATION_INPUT."
    exit 1
fi

# Initialize success tracker
overall_success=0
failed_count=0
total_count=0

# Process each CIF file
echo "=== Running Simulations ==="
for cif_file in *.cif; do
    if [ -f "$cif_file" ]; then
        total_count=$((total_count + 1))
        base_name=$(basename "$cif_file" .cif)
        
        echo "----------------------------------------"
        echo "Starting simulation for: $base_name"
        echo "----------------------------------------"
        
        # Create directory for this simulation
        mkdir -p "$base_name"
        
        # Copy necessary files to simulation directory
        cp -v "$cif_file" "$base_name/"
        cp -v simulation.input "$base_name/"
        cp -v *.def "$base_name/" 2>/dev/null || echo "⚠️  Warning: No .def files found"
        cp -v *.block "$base_name/" 2>/dev/null || echo "⚠️  Warning: No .block files found"

        # verbose change to simulation directory
        cd "$base_name"
        
        # Update framework name in simulation.input
        if ! sed -i "s/^FrameworkName.*/FrameworkName $base_name/" simulation.input; then
            echo "❌ Failed to update FrameworkName in simulation.input for $base_name"
            failed_count=$((failed_count + 1))
            echo $failed_count
            overall_success=1
            cd ..
            continue
        fi
        echo "✅ Updated FrameworkName in simulation.input for $base_name"
        # Count the number of Al atoms in the CIF file
        al_count=$(grep -c 'Al' "$cif_file" || true)
        echo "Replacing \${n_cations} with $al_count in simulation.input"
        sed -i "s#\${n_cations}#$al_count#g" simulation.input

        # Run simulation
        echo "Running RASPA simulation for $base_name..."
        start_time=$(date +%s)
        
        "$RASPA_DIR/simulate" > raspa_output.log 2>&1
        end_time=$(date +%s)
        elapsed_time=$((end_time - start_time))
        if grep -qi 'error' raspa_output.log; then
            echo "❌ Simulation failed after $elapsed_time seconds (error found in log)"
            echo "1" > exit_status.log
            failed_count=$((failed_count + 1))
            overall_success=1
        else
            echo "✅ Simulation completed successfully in $elapsed_time seconds"
            echo "0" > exit_status.log
        fi
        # Return to main directory
        cd ..
    fi
done

# Final status reporting
echo "==============================="
echo "=== Simulation Summary ==="
echo "Total structures processed: $total_count"
echo "Successful simulations: $((total_count - failed_count))"
echo "Failed simulations: $failed_count"
echo "==============================="

# Clean up inside each simulation directory first
echo "Cleaning up simulation directories..."
for cif_file in *.cif; do
    if [ -f "$cif_file" ]; then
        base_name=$(basename "$cif_file" .cif)
        if [ -d "$base_name" ]; then
            echo "Cleaning directory: $base_name"
            # Keep only output data files, simulation input, and exit_status.log
            find "$base_name" -type f ! -name "output_*.data" ! -name "simulation.input" ! -name "exit_status.log" -delete
            # Clean empty subdirectories if any
            find "$base_name" -type d -empty -delete
        fi
    fi
done

# Clean up temporary files in the main directory
echo "Cleaning up temporary files..."
rm -v *.def 2>/dev/null || echo "⚠️ Warning: Could not remove .def files"
rm -v *.cif 2>/dev/null || echo "⚠️ Warning: Could not remove .cif files"
rm -v *.log 2>/dev/null || echo "⚠️ Warning: Could not remove log files"
rm -v *.py 2>/dev/null || echo "⚠️ Warning: Could not remove auxiliary Python scripts"
rm -v *.sh 2>/dev/null || echo "⚠️ Warning: Could not remove auxiliary scripts"
# Write final exit status
if [ $overall_success -eq 0 ]; then
    echo "✅ All simulations completed successfully"
    echo "0" > exit_status.log
    exit 0
else
    echo "❌ $failed_count out of $total_count simulations failed"
    echo "1" > exit_status.log
    exit 1
fi
