#!/usr/bin/env python

import os
import sys
import csv
import argparse
import re
from typing import Dict, List, Tuple, Optional

def natural_sort_key(s):
    """
    Sort strings with embedded numbers in natural order.
    E.g., batch_1, batch_2, ..., batch_10 instead of batch_1, batch_10, batch_2
    """
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', s)]

def get_batch_num(filename):
    """
    Extract batch number from a filename or directory path
    
    Parameters
    ----------
    filename : str
        Filename or path containing batch number
        
    Returns
    -------
    int
        Batch number, or 0 if not found
    """
    # Try to extract from directory name first (for nested structures)
    parts = os.path.normpath(filename).split(os.sep)
    for part in parts:
        if part.startswith("batch_"):
            match = re.search(r'batch_(\d+)', part)
            if match:
                return int(match.group(1))
    
    # Try from filename
    match = re.search(r'batch_(\d+)', os.path.basename(filename))
    return int(match.group(1)) if match else 0

def normalize_structure_id(structure_id: str) -> Tuple[str, str]:
    """
    Normalize structure IDs by removing paths, file extensions, and suffixes
    
    For example:
    /path/to/RIPPEN_clean.cif -> RIPPEN_clean
    RIPPEN_clean_pacmof -> RIPPEN_clean
    
    Also handles the case where dots (.) in original filename were replaced with
    underscores (_) during simulation processing.
    
    Parameters
    ----------
    structure_id : str
        Original structure ID
        
    Returns
    -------
    Tuple[str, str]
        Normalized structure ID and version with dots replaced by underscores
    """
    # First, remove file path if present (keep only filename)
    structure_id = os.path.basename(structure_id)
    
    # Remove file extension if present
    structure_id = os.path.splitext(structure_id)[0]
    
    # Remove _pacmof suffix if present
    if '_pacmof' in structure_id:
        structure_id = structure_id.replace('_pacmof', '')
    
    # Create a version with dots replaced by underscores for comparison
    normalized_with_underscores = structure_id.replace('.', '_')
    
    return structure_id, normalized_with_underscores

def find_batch_result_files(input_dir: str, batch_dirs: List[str], output_dir: str, 
                           result_type: str = 'analysis', verbose: bool = False) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Find result files across multiple batch directories and identify missing structures
    
    Parameters
    ----------
    input_dir : str
        Base directory containing batch subdirectories
    batch_dirs : List[str]
        List of batch directory names to check
    output_dir : str
        Base output directory where standard batch files might be found
    result_type : str, optional
        Type of results to look for (e.g., 'analysis')
    verbose : bool, optional
        Whether to print detailed progress
    
    Returns
    -------
    Tuple[List[str], Dict[str, Dict]]
        List of paths to result files and dictionary of missing structures info
    """
    result_files = []
    missing_structures = {}
    
    # Define path to standard batches directory
    standard_batches_dir = os.path.join(output_dir, "batches")
    
    if verbose and os.path.isdir(standard_batches_dir):
        print(f"Found standard batches directory at {standard_batches_dir}")
    elif not os.path.isdir(standard_batches_dir) and verbose:
        print(f"⚠️ Warning: Standard batches directory not found at {standard_batches_dir}")
    
    for batch_dir in batch_dirs:
        batch_path = os.path.join(input_dir, batch_dir)
        batch_num = int(batch_dir.split("_")[1])
        
        # Track issues and missing structure details for this batch
        batch_issues = {"issues": [], "missing_structures": []}
        
        # Define the expected standard file location
        expected_file = os.path.join(standard_batches_dir, f"batch_{batch_num}.csv")
        expected_exists = os.path.isfile(expected_file)
        
        # For analysis results, look in the analysis subdirectory
        result_path = batch_path
        if result_type.lower() == 'analysis':
            result_path = os.path.join(batch_path, 'analysis')
            
        if not os.path.isdir(result_path):
            batch_issues["issues"].append(f"No {result_type} directory found")
            missing_structures[batch_dir] = batch_issues
            if verbose:
                print(f"⚠️ Warning: No {result_type} directory found for {batch_dir}")
            continue
            
        # Find all CSV files in the batch directory
        csv_files = [f for f in os.listdir(result_path) if f.endswith(".csv")]
        
        if not csv_files:
            batch_issues["issues"].append(f"No CSV files found in {result_type} directory")
            missing_structures[batch_dir] = batch_issues
            if verbose:
                print(f"⚠️ Warning: No CSV files found in {result_type} directory for {batch_dir}")
            continue
            
        # Add the first CSV file from each batch
        result_file = os.path.join(result_path, csv_files[0])
        result_files.append(result_file)
        
        # Compare with expected standard file if it exists
        if expected_exists:
            # Load structure IDs from both files to identify missing structures
            expected_structures = {}  # Store normalized ID -> original ID mapping
            actual_structures = {}    # Store normalized ID -> original ID mapping
            
            try:
                # Load expected structures from standard batch file
                with open(expected_file, 'r', newline='') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    
                    # Find the column with structure IDs (usually first column)
                    structure_col = 0
                    for i, col_name in enumerate(header):
                        if 'structure' in col_name.lower() or 'mof' in col_name.lower() or 'id' in col_name.lower():
                            structure_col = i
                            break
                            
                    # Read all structure IDs
                    for row in reader:
                        if row:  # Skip empty rows
                            orig_id = row[structure_col].strip()
                            norm_id, _ = normalize_structure_id(orig_id)
                            expected_structures[norm_id] = orig_id
                
                # Load actual structures from result file
                with open(result_file, 'r', newline='') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    
                    # Find the column with structure IDs
                    structure_col = 0
                    for i, col_name in enumerate(header):
                        if 'structure' in col_name.lower() or 'mof' in col_name.lower() or 'id' in col_name.lower():
                            structure_col = i
                            break
                            
                    # Read all structure IDs
                    for row in reader:
                        if row:  # Skip empty rows
                            orig_id = row[structure_col].strip()
                            norm_id, _ = normalize_structure_id(orig_id)
                            actual_structures[norm_id] = orig_id
                
                # Find missing structures using normalized IDs
                missing = []
                for norm_id, orig_id in expected_structures.items():
                    norm_id_regular, norm_id_underscored = normalize_structure_id(orig_id)
                    
                    # Check if either version of the normalized ID exists in actual structures
                    if norm_id_regular not in actual_structures and norm_id_underscored not in actual_structures:
                        missing.append(orig_id)
                
                if missing:
                    batch_issues["missing_structures"] = missing
                    if verbose:
                        print(f"  ⚠️ Warning: {len(missing)} structures missing from batch_{batch_num}")
                        if len(missing) <= 5:  # Show a few examples
                            missing_display = [os.path.basename(m) for m in sorted(missing)[:5]]
                            print(f"     Missing: {', '.join(missing_display)}")
                        else:
                            missing_display = [os.path.basename(m) for m in sorted(missing)[:5]]
                            print(f"     Missing: {', '.join(missing_display)}... and {len(missing)-5} more")
                else:
                    if verbose:
                        print(f"  ✓ All {len(expected_structures)} expected structures found in batch_{batch_num}")
                        
            except Exception as e:
                batch_issues["issues"].append(f"Error comparing structures: {str(e)}")
                if verbose:
                    print(f"  ❌ Error comparing structures for {batch_dir}: {str(e)}")
                
        else:
            batch_issues["issues"].append(f"Missing standard batch file at batches/batch_{batch_num}.csv")
            if verbose:
                print(f"  ⚠️ Warning: Missing standard batch file for batch_{batch_num}")
                
        if len(csv_files) > 1:
            batch_issues["issues"].append(f"Found {len(csv_files)} CSV files (using {csv_files[0]})")
            if verbose:
                print(f"  ⚠️ Note: Found {len(csv_files)} CSV files in {batch_dir}/{result_type}")
        
        # Only add to missing_structures if we found issues or missing structures
        if batch_issues["issues"] or batch_issues["missing_structures"]:
            missing_structures[batch_dir] = batch_issues
    
    return result_files, missing_structures


def concatenate_csv_files(input_dir: str, batch_range: Optional[Tuple[int, int]] = None, 
                          output_dir: Optional[str] = None, output_file_name: Optional[str] = None,
                          all_files: bool = False, result_type: str = 'analysis', 
                          expected_batch_size: Optional[int] = None, verbose: bool = True) -> bool:
    """
    Concatenate CSV files from multiple batches into a single file
    
    Parameters
    ----------
    input_dir : str
        Directory containing batch result directories
    batch_range : tuple, optional
        Range of batch numbers to concatenate (start, end)
    output_dir : str
        Directory where the output file will be stored
    output_file_name : str
        Name of the output file (without directory path)
    all_files : bool
        If True, concatenate all files (ignore batch_range)
    result_type : str
        Type of results to concatenate (e.g., 'analysis')
    expected_batch_size : int, optional
        Expected number of rows per batch for completeness checking
    verbose : bool
        Whether to print detailed progress
    
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    # Check required parameters
    if not output_dir:
        print("❌ Error: No output directory specified")
        return False
        
    if not output_file_name:
        print("❌ Error: No output file name specified")
        return False
    
    # Check if output directory exists
    if not os.path.isdir(output_dir):
        print(f"❌ Error: Output directory not found: {output_dir}")
        print("Make sure the output directory exists before concatenating results.")
        return False
    
    # Construct the full output file path
    output_file_path = os.path.join(output_dir, output_file_name)
    
    # Check if the input directory exists
    if not os.path.isdir(input_dir):
        print(f"❌ Error: Input directory not found: {input_dir}")
        return False
    
    # Find batch directories
    batch_dirs = [d for d in os.listdir(input_dir) 
                  if os.path.isdir(os.path.join(input_dir, d)) and d.startswith("batch_")]
    
    if not batch_dirs:
        print(f"❌ Error: No batch directories found in {input_dir}")
        return False
    
    # Sort batch directories by batch number
    batch_dirs.sort(key=lambda x: int(x.split("_")[1]))
    
    if verbose:
        print(f"Found {len(batch_dirs)} batch directories (batch_{int(batch_dirs[0].split('_')[1])} to batch_{int(batch_dirs[-1].split('_')[1])})")
    
    # Filter by batch range if specified
    if batch_range and not all_files:
        start_batch, end_batch = batch_range
        
        filtered_dirs = []
        for batch_dir in batch_dirs:
            batch_num = int(batch_dir.split("_")[1])
            if start_batch <= batch_num <= end_batch:
                filtered_dirs.append(batch_dir)
        
        if not filtered_dirs:
            print(f"❌ Error: No batch directories found in range {start_batch} to {end_batch}")
            return False
                    
        batch_dirs = filtered_dirs
        
        if verbose:
            print(f"Filtered to {len(batch_dirs)} batch directories in range {start_batch}-{end_batch}")
    
    # Find all result files, now passing the output_dir
    result_files, missing_structures = find_batch_result_files(input_dir, batch_dirs, output_dir, result_type, verbose)
    
    if not result_files:
        print(f"❌ Error: No result files found")
        return False
    
    if verbose:
        print(f"Found {len(result_files)} batch result files")
        if missing_structures:
            batch_count = len(missing_structures)
            struct_count = sum(len(info["missing_structures"]) for info in missing_structures.values())
            print(f"⚠️ Warning: {batch_count} batches had issues, with {struct_count} total missing structures")
    
    # Create CSV log of missing structures
    if missing_structures:
        # Create detailed log file with all issues
        log_file = os.path.join(output_dir, "missing_structures.log")
        with open(log_file, 'w') as log:
            log.write(f"Missing or unexpected structures for {result_type} files:\n\n")
            
            for batch_dir in sorted(missing_structures.keys(), key=lambda x: int(x.split('_')[1])):
                batch_info = missing_structures[batch_dir]
                batch_num = int(batch_dir.split("_")[1])
                
                log.write(f"Batch {batch_num}:\n")
                
                # Write issues
                if batch_info["issues"]:
                    log.write("  Issues:\n")
                    for issue in batch_info["issues"]:
                        log.write(f"    - {issue}\n")
                
                # Write missing structures
                if batch_info["missing_structures"]:
                    log.write(f"  Missing structures ({len(batch_info['missing_structures'])}):\n")
                    for structure in sorted(batch_info["missing_structures"]):
                        log.write(f"    - {structure}\n")
                
                log.write("\n")  # Add blank line between batches
        
        # Create CSV file with all missing structures
        csv_log_file = os.path.join(output_dir, "missing_structures.csv")
        with open(csv_log_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["batch", "structure_id", "issue"])
            
            for batch_dir in sorted(missing_structures.keys(), key=lambda x: int(x.split('_')[1])):
                batch_info = missing_structures[batch_dir]
                batch_num = int(batch_dir.split("_")[1])
                
                # Add rows for missing structures
                for structure in sorted(batch_info["missing_structures"]):
                    writer.writerow([batch_num, structure, "missing"])
                
                # Add rows for other issues
                for issue in batch_info["issues"]:
                    writer.writerow([batch_num, "", issue])
        
        if verbose:
            print(f"✓ Saved missing structure log to {log_file}")
            print(f"✓ Saved missing structure CSV to {csv_log_file}")
    
    # Sort files by batch number
    result_files.sort(key=get_batch_num)
    
    # Concatenate files
    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        
        with open(output_file_path, 'w', newline='') as outfile:
            header_written = False
            total_rows = 0
            writer = None
            
            # Process each file
            for file_path in result_files:
                batch_num = get_batch_num(file_path)
                batch_dir = f"batch_{batch_num}"
                
                if verbose:
                    print(f"Processing batch_{batch_num}: {os.path.basename(file_path)}")
                
                try:
                    with open(file_path, 'r', newline='') as infile:
                        reader = csv.reader(infile)
                        header = next(reader)  # Read header
                        
                        # Write header on first file only
                        if not header_written:
                            writer = csv.writer(outfile)
                            writer.writerow(header)
                            header_written = True
                        
                        # Write all data rows
                        rows = 0
                        for row in reader:
                            writer.writerow(row)
                            rows += 1
                        
                        total_rows += rows
                        
                        if verbose:
                            print(f"  ✓ Added {rows} rows from batch_{batch_num}")
                            
                        # Check if row count matches expected batch size
                        if expected_batch_size and rows < expected_batch_size:
                            if batch_dir not in missing_structures:
                                missing_structures[batch_dir] = {"issues": [], "missing_structures": []}
                            
                            missing_structures[batch_dir]["issues"].append(
                                f"Found only {rows} rows, expected {expected_batch_size}"
                            )
                            
                            if verbose:
                                print(f"  ⚠️ Warning: Batch {batch_num} has {rows} rows, expected {expected_batch_size}")
                        
                except Exception as e:
                    if batch_dir not in missing_structures:
                        missing_structures[batch_dir] = {"issues": [], "missing_structures": []}
                    
                    missing_structures[batch_dir]["issues"].append(f"Error processing file: {str(e)}")
                    
                    print(f"❌ Error processing {file_path}: {str(e)}")
                    continue
        
        # Update missing structures CSV if new issues were found during processing
        if missing_structures:
            csv_log_file = os.path.join(output_dir, "missing_structures.csv")
            with open(csv_log_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["batch", "structure_id", "issue"])
                
                for batch_dir in sorted(missing_structures.keys(), key=lambda x: int(x.split('_')[1])):
                    batch_info = missing_structures[batch_dir]
                    batch_num = int(batch_dir.split("_")[1])
                    
                    # Add rows for missing structures
                    for structure in sorted(batch_info["missing_structures"]):
                        writer.writerow([batch_num, structure, "missing"])
                    
                    # Add rows for other issues
                    for issue in batch_info["issues"]:
                        writer.writerow([batch_num, "", issue])
        
        print(f"\n✅ Successfully concatenated {len(result_files)} files with {total_rows} total rows")
        print(f"✅ Output saved to: {output_file_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error concatenating files: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Concatenate CSV results from multiple batches"
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing batch subdirectories"
    )
    parser.add_argument(
        "--output-file", "-o",
        required=True,
        help="Path to the output CSV file"
    )
    parser.add_argument(
        "--result-type", "-t",
        choices=['analysis', 'results'],
        default='analysis',
        help="Type of results to concatenate (default: analysis)"
    )
    
    # Batch selection options
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all", "-a",
        action="store_true",
        help="Concatenate all available batches"
    )
    group.add_argument(
        "--batch-range", "-b",
        nargs=2,
        type=int,
        metavar=('START', 'END'),
        help="Range of batch numbers to concatenate (e.g., -b 1 10 for batches 1-10)"
    )
    
    # Additional options
    parser.add_argument(
        "--expected-size", "-e",
        type=int,
        help="Expected number of rows per batch for completeness checking"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=True,
        help="Show detailed progress (default: on)"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_false",
        dest="verbose",
        help="Hide detailed progress"
    )
    parser.add_argument(
        "--output-dir", "-d",
        default=".",
        help="Directory to save the output file (default: current directory)"
    )
    
    args = parser.parse_args()
    
    # Set up batch range if specified
    batch_range = None
    if args.batch_range:
        batch_range = (args.batch_range[0], args.batch_range[1])
    
    # Handle output file path
    output_file_name = os.path.basename(args.output_file)
    output_dir = args.output_dir
    
    # If output_file contains a path, override output_dir
    if os.path.dirname(args.output_file):
        output_dir = os.path.dirname(args.output_file)
    
    # Run concatenation with the updated parameters
    success = concatenate_csv_files(
        args.input_dir,
        batch_range=batch_range,
        output_dir=output_dir,
        output_file_name=output_file_name,
        all_files=args.all,
        result_type=args.result_type,
        expected_batch_size=args.expected_size,
        verbose=args.verbose
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())