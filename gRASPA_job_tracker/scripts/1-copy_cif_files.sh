#!/bin/bash
currentdir=$(pwd)
batch=$1
sample_dir=$currentdir/../../../system_setup/data/pacmof_batch_${batch}

# Ensure the sample directory exists and contains CIF files
if [ ! -d "$sample_dir" ] || [ -z "$(ls -A "$sample_dir"/*.cif 2>/dev/null)" ]; then
  echo "No CIF files found in $sample_dir. Exiting..."
  exit 1
fi

num_files=$(ls "$sample_dir"/*.cif | wc -l)
echo "Copying $num_files CIF files from $sample_dir to the current directory..."
cp -v "$sample_dir"/*.cif .