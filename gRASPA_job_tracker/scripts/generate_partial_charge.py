"""
Module that provides an interface to gen_partial_charge functionality.
This file exists to allow the module to be imported with the name 'generate_partial_charge'.
"""

from pacmof2 import pacmof2
import os
import sys
import argparse
import concurrent.futures
from functools import partial
from tqdm import tqdm
import glob

def is_already_processed(cif_path, output_dir):
    """Check if a CIF file has already been processed.
    
    Args:
        cif_path: Path to the CIF file
        output_dir: Directory where output should be saved
        
    Returns:
        bool: True if file has already been processed
    """
    base_name = os.path.splitext(os.path.basename(cif_path))[0]
    processed_file = os.path.join(output_dir, f"{base_name}_pacmof.cif")
    return os.path.exists(processed_file)

def get_memory_usage():
    """Get current memory usage of the process in MB"""
    import psutil
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # in MB

def process_cif(cif_path, output_dir):
    """Process a single CIF file to generate partial charges.
    
    Args:
        cif_path: Path to the CIF file
        output_dir: Directory where output should be saved
        
    Returns:
        tuple: (cif_path, status) where status is: 1 for success, 0 for failure, -1 for skipped
    """
    if is_already_processed(cif_path, output_dir):
        print(f"Skipping already processed file: {cif_path}")
        return (cif_path, -1)
    
    try:
        print(f"Memory before processing: {get_memory_usage():.2f} MB")
        print(f"Processing: {cif_path}")
        pacmof2.get_charges(cif_path, output_dir, identifier="_pacmof", multiple_cifs=False)
        print(f"Memory after processing: {get_memory_usage():.2f} MB")
        return (cif_path, 1)
    except Exception as e:
        print(f"Error processing {cif_path}: {str(e)}")
        return (cif_path, 0)

def count_completed_files(output_dir):
    """Count number of completed CIF files in output directory."""
    return len(glob.glob(os.path.join(output_dir, "*_pacmof.cif")))

def generate_charges(batch_id, input_file, output_dir):
    """Generate partial charges for CIF files.
    
    Args:
        batch_id (str): Batch ID number
        input_file (str): Path to file containing list of CIF files
        output_dir (str): Output directory for partial charges
    
    Returns:
        int: 0 if successful, 1 otherwise
    """
    print(f"Batch ID: {batch_id}")
    print(f"Input file list: {input_file}")
    print(f"Output directory: {output_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the list of CIF files
    input_cifs = []
    
    with open(input_file, 'r') as f:
        for line in f:
            cif_path = line.strip()
            if cif_path:
                input_cifs.append(cif_path)
    
    if not input_cifs:
        print(f"Error: No CIF files found in {input_file}")
        return 1
    
    print(f"Found {len(input_cifs)} CIF files to process")
    
    # At the beginning of generate_charges function
    completed_files = set()
    for file in glob.glob(os.path.join(output_dir, "*_pacmof.cif")):
        base_name = os.path.basename(file).replace("_pacmof.cif", "")
        completed_files.add(base_name)

    # Filter input_cifs to only include files that haven't been processed
    input_cifs = [cif for cif in input_cifs if os.path.splitext(os.path.basename(cif))[0] not in completed_files]
    
    # Conclude if all files have already been processed
    if not input_cifs:
        print("All files have already been processed")
        return 0

    # Set the number of workers equal to the number of CIF files
    num_workers = min(5,len(input_cifs))
    print(f"Using {num_workers} parallel workers (1 worker per CIF file)")
    
    # Process each CIF file in parallel
    successful = 0
    failed = 0
    skipped = 0
    
    # Create progress bar
    pbar = tqdm(total=len(input_cifs), desc="Processing structures")
    initial_completed = count_completed_files(output_dir)
    pbar.update(initial_completed)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        process_func = partial(process_cif, output_dir=output_dir)
        
        # Submit all tasks and gather results
        for cif_path, status in executor.map(process_func, input_cifs):
            if status == 1:
                successful += 1
            elif status == 0:
                failed += 1
            else:  # status == -1
                skipped += 1
            pbar.update(1)
    
    pbar.close()
    print(f"Successfully processed {successful} files")
    print(f"Skipped {skipped} already processed files")
    print(f"Failed to process {failed} files")
    
    # Create a record file listing the processed files
    record_file = os.path.join(output_dir, f"processed_samples_batch_{batch_id}.txt")
    with open(record_file, 'w') as f:
        for cif_path in input_cifs:
            f.write(f"{os.path.basename(cif_path)}\n")
    
    print(f"Partial charges generated successfully in {output_dir}")
    print(f"Created record file: {record_file}")
    
    # Return success only if all files were processed successfully
    return 0 if failed == 0 else 1

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate partial charges for CIF files")
    parser.add_argument("batch_id", help="Batch ID number")
    parser.add_argument("input_file", help="Path to file containing list of CIF files")
    parser.add_argument("output_dir", help="Output directory for partial charges")
    
    args = parser.parse_args()
    
    return generate_charges(args.batch_id, args.input_file, args.output_dir)

if __name__ == "__main__":
    sys.exit(main())
