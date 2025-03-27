#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# This script cleans up framework directories by removing all files except Output*.data and simulation.input
# Usage: cleanup_results.sh [batch_id]
# If batch_id is provided, only that batch will be cleaned up

# Parse command line arguments
batch_id=$1
results_base_dir="${2:-$(pwd)}"  # Default to current directory if not provided

# Print usage information
echo "GRASPA Results Cleanup Utility"
echo "=============================="
echo "This script will remove all files except Output*.data and simulation.input"
echo "from framework directories to save disk space."
echo ""

# Count how many bytes were freed
total_freed=0
dirs_cleaned=0

# Function to clean a framework directory
clean_framework_dir() {
    local dir="$1"
    local before_size=$(du -sb "$dir" | cut -f1)
    
    echo "Cleaning directory: $dir"
    
    # Create a list of files to keep
    find "$dir" -type f \( -name "*.data" -o -name "simulation.input" \) > "$dir/keep_files.txt"
    
    # Create a list of files to delete
    find "$dir" -type f | grep -v -f "$dir/keep_files.txt" > "$dir/delete_files.txt"
    
    # Count files to be deleted
    local delete_count=$(wc -l < "$dir/delete_files.txt")
    echo "  Removing $delete_count files..."
    
    # Delete files
    if [ "$delete_count" -gt 0 ]; then
        xargs rm -f < "$dir/delete_files.txt"
    fi
    
    # Clean up the temporary files
    rm -f "$dir/keep_files.txt" "$dir/delete_files.txt"
    
    # Calculate freed space
    local after_size=$(du -sb "$dir" | cut -f1)
    local freed=$((before_size - after_size))
    total_freed=$((total_freed + freed))
    dirs_cleaned=$((dirs_cleaned + 1))
    
    echo "  Freed $(numfmt --to=iec-i --suffix=B $freed) of space"
}

# Check if specific batch_id was provided
if [ -n "$batch_id" ]; then
    echo "Cleaning up batch $batch_id only"
    batch_dir="${results_base_dir}/batch_${batch_id}"
    
    if [ ! -d "$batch_dir" ]; then
        echo "❌ Batch directory not found: $batch_dir"
        exit 1
    fi
    
    # Process all framework directories in this batch
    echo "Searching for framework directories in $batch_dir..."
    
    # Use find to locate directories that contain simulation.input files
    find "$batch_dir" -name "simulation.input" | while read -r input_file; do
        framework_dir=$(dirname "$input_file")
        clean_framework_dir "$framework_dir"
    done
else
    # Process all batches
    echo "Cleaning up all batches in $results_base_dir"
    
    # Find all batch directories
    for batch_dir in "$results_base_dir"/batch_*; do
        if [ -d "$batch_dir" ]; then
            batch_num=$(basename "$batch_dir" | sed 's/batch_//')
            echo "Processing batch $batch_num..."
            
            # Find framework directories in this batch
            find "$batch_dir" -name "simulation.input" | while read -r input_file; do
                framework_dir=$(dirname "$input_file")
                clean_framework_dir "$framework_dir"
            done
        fi
    done
fi

# Print summary
echo ""
echo "Cleanup complete!"
echo "----------------"
echo "Directories cleaned: $dirs_cleaned"
echo "Total space freed: $(numfmt --to=iec-i --suffix=B $total_freed)"
echo ""
echo "Only Output*.data files and simulation.input files were kept."
echo "To restore needed files, rerun the simulations."
