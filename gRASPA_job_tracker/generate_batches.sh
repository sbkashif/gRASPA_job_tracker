#!/bin/bash
# filepath: /scratch/bcvz/sbinkashif/coremof_co2_n2_adsorption/example_workflow/generate_batches.sh

# Enable strict error handling
set -e
set -o pipefail

# Check for required arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <NUMSAMPLES>"
    exit 1
fi

NUMSAMPLES=$1
source_dir="../system_setup/data/coremof_clean"
pointer_file="../system_setup/last_sampled_index.txt"
batch_number_file="../system_setup/last_batch_number.txt"
sorted_list_file="../system_setup/sorted_files.txt"

# Ensure source directory exists
if [ ! -d "$source_dir" ]; then
    echo "❌ Error: Source directory $source_dir does not exist."
    exit 1
fi

# Create or use pre-saved sorted list of files
if [ ! -f "$sorted_list_file" ]; then
    echo "Creating sorted list of files in '$sorted_list_file'..."
    find "$source_dir" -maxdepth 1 -type f | sort > "$sorted_list_file"
fi

# Read sorted list into an array
mapfile -t sorted_files < "$sorted_list_file"
total_files=${#sorted_files[@]}
if [ $total_files -eq 0 ]; then
    echo "❌ No files found in $source_dir."
    exit 1
fi

# Generate batches in a loop
while true; do
    # Read the pointer to determine the starting index
    if [ -f "$pointer_file" ]; then
        current_pointer=$(<"$pointer_file")
    else
        echo "❌ Pointer file '$pointer_file' does not exist."
        echo "Please create '$pointer_file' with an initial index (e.g., 0) and run the script again."
        exit 1
    fi

    # Read the last batch number
    if [ -f "$batch_number_file" ]; then
        last_batch=$(<"$batch_number_file")
        batch=$((last_batch + 1))
    else
        echo "❌ Batch number file '$batch_number_file' does not exist."
        echo "Please create '$batch_number_file' with an initial batch number (e.g., 1) and run the script again."
        exit 1
    fi

    # Check if there are enough samples remaining
    if [ $current_pointer -ge $total_files ]; then
        echo "✅ All files have been processed. Exiting..."
        exit 0
    fi

    # Compute new pointer position
    end_index=$((current_pointer + NUMSAMPLES))
    if [ $end_index -gt $total_files ]; then
        end_index=$total_files  # Adjust to process remaining files
    fi
    echo "$end_index" > "$pointer_file"

    # Save the new batch number
    echo "$batch" > "$batch_number_file"

    # Set up batch-specific directory
    new_samples_dir="../system_setup/data/coremof_clean_batch_${batch}"
    mkdir -p "$new_samples_dir"

    # Copy files for the new batch
    indices=($(seq $current_pointer $((end_index - 1))))
    for i in "${indices[@]}"; do
        file=$(basename "${sorted_files[$i]}")
        cp "$source_dir/$file" "$new_samples_dir"
    done

    echo "✅ Successfully generated batch $batch with $((end_index - current_pointer)) samples."
done