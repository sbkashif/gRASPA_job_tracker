#!/usr/bin/env python
# filepath: /scratch/bcvz/sbinkashif/coremof_co2_n2_adsorption/analysis/process_batch_output.py

import os
import sys
import glob
import csv
import json
import argparse
import re
import pandas as pd
from datetime import datetime

# Import the extract_averages function
from gRASPA_job_tracker.scripts.parse_graspa_output import extract_averages as original_extract_averages

def safe_extract_averages(data_file):
    """
    Wrapper for extract_averages that handles problematic values like '-nan'
    
    Parameters
    ----------
    data_file : str
        Path to the RASPA output data file
        
    Returns
    -------
    dict or None
        Dictionary of extracted data, or None if extraction failed
    """
    try:
        # Try to parse the file directly with our own handling for problematic values
        # This is to prevent the error before it happens
        with open(data_file, 'r') as file:
            content = file.read()
            
            # Replace problematic values before passing to the original function
            content = content.replace('-nan', '0.0')
            content = content.replace(' - ', ' 0.0 ')
            content = content.replace(' nan ', ' 0.0 ')
            
            # Create a temporary file with the fixed content
            temp_file = data_file + '.temp'
            with open(temp_file, 'w') as tf:
                tf.write(content)
                
            try:
                # Use the original function on our sanitized file
                data = original_extract_averages(temp_file)
                
                # Clean up temp file
                os.remove(temp_file)
                
                if not data:
                    print(f"  ⚠️ Failed to extract data (temp file returned None)")
                    return None
                    
                # Recursive function to fix any remaining string values
                def fix_value(value):
                    if isinstance(value, dict):
                        # Process nested dictionary
                        return {k: fix_value(v) for k, v in value.items()}
                    elif isinstance(value, str):
                        # Handle string values that should be numbers
                        if value.strip() == '-' or 'nan' in value.lower():
                            return 0.0
                        try:
                            return float(value)
                        except ValueError:
                            return 0.0
                    else:
                        # Keep other values unchanged
                        return value
                        
                # Fix all values in the data
                return fix_value(data)
            except Exception as inner_e:
                # Clean up temp file if it exists
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                print(f"  ⚠️ Failed with sanitized file: {str(inner_e)}")
                
                # Fall back to direct extraction with a more robust approach
                try:
                    # Use the original extract_averages function
                    data = original_extract_averages(data_file)
                    
                    if not data:
                        print(f"  ⚠️ Original extract_averages returned None")
                        return None
                    
                    # Make a deep copy to avoid modifying the original data
                    import copy
                    fixed_data = copy.deepcopy(data)
                    
                    # Go through all sections manually and fix values
                    for section in fixed_data:
                        if isinstance(fixed_data[section], dict):
                            for gas in fixed_data[section]:
                                if isinstance(fixed_data[section][gas], dict):
                                    for key in fixed_data[section][gas]:
                                        val = fixed_data[section][gas][key]
                                        if isinstance(val, str):
                                            if val.strip() == '-' or 'nan' in val.lower():
                                                fixed_data[section][gas][key] = 0.0
                                            else:
                                                try:
                                                    fixed_data[section][gas][key] = float(val)
                                                except ValueError:
                                                    fixed_data[section][gas][key] = 0.0
                    
                    return fixed_data
                except Exception as e:
                    print(f"  ❌ Manual fix failed: {str(e)}")
                    return None
        
    except Exception as e:
        print(f"  ❌ Error in safe_extract_averages: {str(e)}")
        return None

def process_batch(batch_id, input_dir, output_dir, write_json=True, update_job_status=False):
    """
    Process all gRASPA output files in a batch directory and extract all available data
    
    Parameters
    ----------
    batch_id : int
        Batch ID for naming the output files
    input_dir : str
        Path to the batch output directory containing System*.data files
    output_dir : str
        Path to save analysis results
    write_json : bool, optional
        Whether to write JSON output files (default: True)
    update_job_status : bool, optional
        Whether to update the job status file with completion status (default: False)
    
    Returns
    -------
    bool
        True if the analysis was successful, False otherwise
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Clean existing output files to ensure fresh analysis
    exit_status_file = os.path.join(output_dir, 'exit_status.log')
    batch_id_str = f"batch_{batch_id}_" if batch_id else ""
    csv_file = os.path.join(output_dir, f"{batch_id_str}all_results.csv")
    json_file = os.path.join(output_dir, f"{batch_id_str}all_results.json")
    failed_file = os.path.join(output_dir, f"{batch_id_str}failed_files.json")
    missing_file = os.path.join(output_dir, f"{batch_id_str}missing_structures.json")
    
    # Delete all existing output files
    for file_path in [exit_status_file, csv_file, json_file, failed_file, missing_file]:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"Deleted existing file: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"Warning: Could not remove existing file {os.path.basename(file_path)}: {e}")
    
    # Find all System*.data files
    data_files = glob.glob(os.path.join(input_dir, "**", "System*.data"), recursive=True)
    
    if not data_files:
        print(f"❌ Error: No System*.data files found in {input_dir}")
        # Write failure exit status
        with open(exit_status_file, 'w') as f:
            f.write("1")
        return False
        
    print(f"▶ Found {len(data_files)} output files to analyze")
    
    # Prepare results storage
    results = []
    failed_files = []
    processed_structures = []  # For tracking successfully found structures
    
    # Process each file
    successful_files = 0
    for data_file in data_files:
        structure_name = None
        try:
            # Extract structure name from file path
            file_name = os.path.basename(data_file)
            
            # Extract structure name based on the specific format
            # Format: System_0_ABAVIJ_clean_pacmof.cif_3_3_2_298.000000_100000.000000.data
            parts = file_name.split('_')
            if len(parts) >= 4 and parts[0] == "System":
                # Extract structure name (before .cif)
                structure_parts = []
                for i in range(2, len(parts)):
                    if '.cif' in parts[i]:
                        structure_parts.append(parts[i].split('.cif')[0])
                        break
                    structure_parts.append(parts[i])
                
                structure_name = '_'.join(structure_parts)
            else:
                # Fallback for other naming patterns
                structure_name = file_name.replace("System", "").split('.')[0]
            
            # Track the structure name for later comparison with batch file
            processed_structures.append(structure_name)
            
            # Process the file
            print(f"  Processing {file_name}...")
            
            # Use the safe wrapper function instead of direct call
            try:
                data = safe_extract_averages(data_file)
                
                if data and isinstance(data, dict):
                    # Verify that the data has the required sections
                    required_sections = ['loading_mol_kg', 'mole_fraction']
                    required_gases = ['CO2', 'N2']
                    
                    # Check if required sections and gases exist
                    missing_sections = []
                    for section in required_sections:
                        if section not in data:
                            missing_sections.append(section)
                        elif not isinstance(data[section], dict):
                            missing_sections.append(section)
                        else:
                            for gas in required_gases:
                                if gas not in data[section]:
                                    missing_sections.append(f"{section}.{gas}")
                    
                    if missing_sections:
                        # Data is missing required sections
                        failed_files.append({
                            'file': data_file,
                            'structure': structure_name,
                            'reason': f'Missing required data sections: {", ".join(missing_sections)}'
                        })
                        print(f"  ⚠️ Warning: Missing required data sections in {file_name}")
                        continue
                    
                    try:
                        # Start with basic structure info
                        result = {
                            'structure': structure_name,
                            'unit_cells': data.get('unit_cells', 1),
                            'framework_mass': data.get('framework_mass', 0.0),
                            'average_volume': data.get('average_volume', 0.0)
                        }
                        
                        # Include all sections data in the specified order
                        section_keys = [
                            'loading_mol_kg',
                            'loading_mg_g',
                            'loading_g_L',
                            'loading_num_molecules',
                            'heat_of_adsorption_kJ_mol',
                            'mole_fraction'
                        ]
                        
                        # Process each section that's available in the data
                        for section_key in section_keys:
                            if section_key in data and 'CO2' in data[section_key] and 'N2' in data[section_key]:
                                # Add CO2 data
                                result[f"{section_key}_co2_avg"] = data[section_key]['CO2']['average']
                                result[f"{section_key}_co2_err"] = data[section_key]['CO2']['error']
                                
                                # Add N2 data
                                result[f"{section_key}_n2_avg"] = data[section_key]['N2']['average']
                                result[f"{section_key}_n2_err"] = data[section_key]['N2']['error']
                                
                                # Calculate selectivity for this section
                                try:
                                    # Regular case: both gases have values
                                    if data[section_key]['N2']['average'] > 0:
                                        result[f"{section_key}_selectivity"] = (
                                            data[section_key]['CO2']['average'] / 
                                            data[section_key]['N2']['average']
                                        )
                                    else:
                                        # This will trigger the exception if N2 is zero
                                        result[f"{section_key}_selectivity"] = (
                                            data[section_key]['CO2']['average'] / 
                                            data[section_key]['N2']['average']
                                        )
                                except (ZeroDivisionError, TypeError):
                                    # Handle the division by zero cases
                                    if data[section_key]['CO2']['average'] > 0:
                                        # Case: CO2 adsorbs, N2 doesn't = perfect selectivity
                                        result[f"{section_key}_selectivity"] = float('inf')
                                    else:
                                        # Case: Neither gas adsorbs = undefined selectivity
                                        result[f"{section_key}_selectivity"] = float('nan')
                        
                        # Calculate true selectivity from mole fractions and mol/kg loadings
                        if 'mole_fraction' in data and 'loading_mol_kg' in data:
                            co2_loading = data['loading_mol_kg']['CO2']['average']
                            n2_loading = data['loading_mol_kg']['N2']['average']
                            co2_mole_fraction = data['mole_fraction']['CO2']['average']
                            n2_mole_fraction = data['mole_fraction']['N2']['average']
                            
                            try:
                                # Calculate adsorption selectivity using the correct formula
                                loading_ratio = co2_loading / n2_loading
                                mole_fraction_ratio = co2_mole_fraction / n2_mole_fraction
                                result['selectivity_(n_co2/n_n2)/(p_co2/p_n2)'] = loading_ratio / mole_fraction_ratio
                            except (ZeroDivisionError, TypeError):
                                # If any of the terms is zero, let Python set it to appropriate value
                                if co2_loading > 0 and co2_mole_fraction > 0:
                                    result['selectivity_(n_co2/n_n2)/(p_co2/p_n2)'] = float('inf')
                                else:
                                    result['selectivity_(n_co2/n_n2)/(p_co2/p_n2)'] = float('nan')
                        
                        # Calculate per unit cell values for molecule loading section
                        if 'loading_num_molecules' in data:
                            unit_cells = result['unit_cells']
                            result['loading_num_molecules_co2_per_uc'] = result['loading_num_molecules_co2_avg'] / unit_cells
                            result['loading_num_molecules_co2_per_uc_err'] = result['loading_num_molecules_co2_err'] / unit_cells
                            result['loading_num_molecules_n2_per_uc'] = result['loading_num_molecules_n2_avg'] / unit_cells
                            result['loading_num_molecules_n2_per_uc_err'] = result['loading_num_molecules_n2_err'] / unit_cells
                        
                        # Validate the final result
                        required_fields = ['selectivity_(n_co2/n_n2)/(p_co2/p_n2)', 'loading_mol_kg_co2_avg', 'loading_mol_kg_n2_avg']
                        missing_fields = [field for field in required_fields if field not in result]
                        
                        if missing_fields:
                            # Result is missing required fields
                            failed_files.append({
                                'file': data_file,
                                'structure': structure_name,
                                'reason': f'Missing required result fields: {", ".join(missing_fields)}'
                            })
                            print(f"  ⚠️ Warning: Missing required result fields in {file_name}")
                            continue
                        
                        results.append(result)
                        successful_files += 1
                        print(f"  ✓ Successfully processed {structure_name}")
                        
                    except Exception as processing_e:
                        # Error during data processing
                        failed_files.append({
                            'file': data_file,
                            'structure': structure_name,
                            'reason': f'Error processing data: {str(processing_e)}'
                        })
                        print(f"  ⚠️ Warning: Error processing data from {file_name}: {str(processing_e)}")
                        continue
                        
                else:
                    failed_files.append({
                        'file': data_file,
                        'structure': structure_name,
                        'reason': 'Could not extract data or data format is invalid'
                    })
                    print(f"  ⚠️ Warning: Could not extract valid data from {file_name}")
                    continue
                    
            except Exception as inner_e:
                # Catch and log any exceptions during data extraction
                failed_files.append({
                    'file': data_file,
                    'structure': structure_name,
                    'reason': f'Error extracting data: {str(inner_e)}'
                })
                print(f"  ⚠️ Warning: Error extracting data from {file_name}: {str(inner_e)}")
                # Continue to next file despite this error
                continue
                
        except Exception as e:
            # This is the outer exception handler to ensure we always proceed to the next file
            failed_files.append({
                'file': data_file,
                'structure': structure_name if structure_name else os.path.basename(data_file),
                'reason': f'Unexpected error: {str(e)}'
            })
            print(f"❌ Error processing {data_file}: {str(e)}")
            # Explicitly continue to the next file
            continue
    
    # Save failed files information regardless of success
    if failed_files:
        try:
            with open(failed_file, 'w') as f:
                json.dump(failed_files, f, indent=2)
            print(f"⚠️ {len(failed_files)} files failed to process. Details saved to: {failed_file}")
        except Exception as e:
            print(f"⚠️ Error saving failed files information: {e}")
            # Continue processing even if we can't save the failed files report
    
    # Always attempt to save any results we have, even if some or most files failed
    try:
        # If we have any results at all, write them out
        if results:
            # Get all field names from all results
            all_fields = set()
            for result in results:
                all_fields.update(result.keys())
            
            # Order fields: first core fields, then the rest alphabetically
            core_fields = [
                'structure', 
                'unit_cells',
                'framework_mass',
                'average_volume',
                'selectivity_(n_co2/n_n2)/(p_co2/p_n2)',
                'loading_mol_kg_co2_avg',
                'loading_mol_kg_co2_err',
                'loading_mol_kg_n2_avg',
                'loading_mol_kg_n2_err',
                'loading_mg_g_co2_avg',
                'loading_mg_g_co2_err',
                'loading_mg_g_n2_avg',
                'loading_mg_g_n2_err',
                'loading_g_L_co2_avg',
                'loading_g_L_co2_err',
                'loading_g_L_n2_avg',
                'loading_g_L_n2_err',
                'loading_num_molecules_co2_avg',
                'loading_num_molecules_co2_err',
                'loading_num_molecules_n2_avg',
                'loading_num_molecules_n2_err'
            ]
            
            # Create the fieldnames list with core fields first
            fieldnames = [f for f in core_fields if f in all_fields]
            
            # Save results to CSV
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                # Write each result row
                for result in results:
                    row = {field: result.get(field, '') for field in fieldnames}
                    writer.writerow(row)
                    
            print(f"✅ Results saved to CSV: {csv_file}")
            
            # Save results to JSON for easier parsing (if enabled)
            if write_json:
                with open(json_file, 'w') as f:
                    json.dump(results, f, indent=2)
                print(f"✅ Results saved to JSON: {json_file}")
    except Exception as e:
        print(f"⚠️ Error saving results: {e}")
        # We'll still try to continue with exit status and job status updates
    
    # Check for missing structures by comparing with batch file
    if batch_id:
        try:
            # Get list of successfully processed structure names
            processed_structure_names = [result['structure'] for result in results]
            
            # Find missing structures
            missing_structures = find_missing_batch_structures(batch_id, output_dir, processed_structure_names)
            
            if missing_structures:
                print(f"⚠️ {len(missing_structures)} structures from batch file not found in analysis results")
                
                # Save missing structures to JSON file
                missing_data = {
                    'batch_id': batch_id,
                    'expected_count': len(missing_structures) + len(processed_structure_names),
                    'found_count': len(processed_structure_names),
                    'missing_count': len(missing_structures),
                    'missing_structures': missing_structures
                }
                
                with open(missing_file, 'w') as f:
                    json.dump(missing_data, f, indent=2)
                print(f"⚠️ Missing structures information saved to: {missing_file}")
                
                # Print the first few missing structures
                if len(missing_structures) <= 5:
                    for structure in missing_structures:
                        print(f"  - Missing: {structure}")
                else:
                    for structure in missing_structures[:5]:
                        print(f"  - Missing: {structure}")
                    print(f"  - ... and {len(missing_structures) - 5} more")
            else:
                print("✅ All structures from batch file found in analysis results")
                
        except Exception as e:
            print(f"⚠️ Error checking for missing structures: {e}")
            # Continue despite errors in this section
    
    print(f"✅ Successfully processed data for {successful_files} out of {len(data_files)} files")
    
    # Determine exit status
    exit_status = "0"  # Complete success by default
    if successful_files < len(data_files):
        # Partial completion if some files failed but at least one succeeded
        exit_status = "2"  # Use "2" to indicate partially complete
    
    # Only mark as complete failure if no files were processed successfully
    if successful_files == 0:
        exit_status = "1"  # Complete failure
        
    # Always write an exit status, even if other parts failed
    try:
        with open(exit_status_file, 'w') as f:
            f.write(exit_status)
    except Exception as e:
        print(f"⚠️ Error writing exit status file: {e}")
    
    # Update job status file if requested
    if update_job_status and batch_id:
        try:
            update_job_status_for_batch(batch_id, exit_status)
        except Exception as e:
            print(f"Warning: Could not update job status: {e}")
    
    # Consider the batch successful as long as we processed at least one file
    return successful_files > 0

def update_job_status_for_batch(batch_id, exit_status):
    """
    Update the job status file with the analysis completion status
    
    Parameters
    ----------
    batch_id : int
        Batch ID to update
    exit_status : str
        Exit status code: "0" for success, "1" for failure, "2" for partial completion
    """
    import pandas as pd
    import os
    from datetime import datetime
    
    # Try to find the job status file
    possible_paths = [
        "./job_status.csv",
        "../job_status.csv",
        "../../job_status.csv",
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))), "job_status.csv")
    ]
    
    job_status_file = None
    for path in possible_paths:
        if os.path.exists(path):
            job_status_file = path
            break
    
    if not job_status_file:
        print(f"Warning: Could not find job_status.csv file to update")
        return
        
    # Read the job status file
    try:
        df = pd.read_csv(job_status_file)
        batch_rows = df[df['batch_id'] == batch_id]
        
        if batch_rows.empty:
            print(f"Warning: Batch {batch_id} not found in job status file")
            return
            
        # Update the status based on exit status code
        if exit_status == "0":
            new_status = "COMPLETED"
            workflow_stage = "completed"
        elif exit_status == "1":
            new_status = "FAILED"
            workflow_stage = "analysis (failed)"
        elif exit_status == "2":
            new_status = "PARTIALLY_COMPLETE"
            workflow_stage = "analysis (partially complete)"
            
        # Update the last row for this batch (most recent job)
        last_row_idx = batch_rows.index[-1]
        df.loc[last_row_idx, 'status'] = new_status
        df.loc[last_row_idx, 'workflow_stage'] = workflow_stage
        
        # Update completion time if not already set
        if pd.isna(df.loc[last_row_idx, 'completion_time']):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df.loc[last_row_idx, 'completion_time'] = timestamp
            
        # Save the updated file
        df.to_csv(job_status_file, index=False)
        print(f"Updated job status for batch {batch_id} to {new_status}")
        
    except Exception as e:
        print(f"Error updating job status file: {e}")

def process_batch_range(min_batch, max_batch, base_dir, write_json=True, update_job_status=False):
    """
    Process a range of batch output directories
    
    Parameters
    ----------
    min_batch : int
        Minimum batch ID to process
    max_batch : int
        Maximum batch ID to process
    base_dir : str
        Base directory containing batch_X folders
    write_json : bool, optional
        Whether to write JSON output files (default: True)
    update_job_status : bool, optional
        Whether to update job status file (default: False)
        
    Returns
    -------
    dict
        Dictionary with batch IDs as keys and success status as values
    """
    results = {}
    all_failed_structures = []
    all_missing_structures = []  # Track structures that were missing before analysis
    batch_status = {}  # Track full success, partial success, or failure for each batch
    
    print(f"Processing batches from {min_batch} to {max_batch}")
    for batch_id in range(min_batch, max_batch + 1):
        # Construct paths
        batch_dir = os.path.join(base_dir, f"batch_{batch_id}")
        input_dir = os.path.join(batch_dir, "simulation")
        output_dir = os.path.join(batch_dir, "analysis")
        
        # Check if simulation directory exists
        if not os.path.exists(input_dir):
            print(f"Skipping batch {batch_id}: Simulation directory not found: {input_dir}")
            results[batch_id] = False
            batch_status[batch_id] = "SKIPPED"
            continue
            
        print(f"\n=== Processing Batch {batch_id} ===")
        print(f"Input: {input_dir}")
        print(f"Output: {output_dir}")
        
        # Process this batch
        success = process_batch(
            batch_id, 
            input_dir, 
            output_dir, 
            write_json=write_json,
            update_job_status=update_job_status
        )
        
        results[batch_id] = success
        
        # Check for failed files to determine if this was partial or full success
        failed_file = os.path.join(output_dir, f"batch_{batch_id}_failed_files.json")
        batch_failed_structures = []
        
        if os.path.exists(failed_file):
            try:
                with open(failed_file, 'r') as f:
                    failed_data = json.load(f)
                    
                    # Track the batch status
                    if not success:
                        batch_status[batch_id] = "FAILED"  # All structures failed
                    elif len(failed_data) > 0:
                        batch_status[batch_id] = "PARTIAL"  # Some structures failed, some succeeded
                    else:
                        batch_status[batch_id] = "SUCCESS"  # All structures succeeded
                        
                    # Add batch ID to each failed entry for easier tracking
                    for entry in failed_data:
                        entry['batch_id'] = batch_id
                        batch_failed_structures.append(entry['structure'])
                        all_failed_structures.append(entry)
            except Exception as e:
                print(f"Could not read failed files data from {failed_file}: {e}")
                batch_status[batch_id] = "SUCCESS" if success else "FAILED"
                
                # If this is a failed batch with no files processed, add an entry
                if not success:
                    all_failed_structures.append({
                        'batch_id': batch_id,
                        'structure': 'all',
                        'reason': 'No structures could be processed'
                    })
        else:
            # If no failed_files.json exists but the process wasn't successful,
            # this is likely a batch with no files or all files failed
            if not success:
                batch_status[batch_id] = "FAILED"
                all_failed_structures.append({
                    'batch_id': batch_id,
                    'structure': 'all',
                    'reason': 'No structures could be processed'
                })
            else:
                batch_status[batch_id] = "SUCCESS"
        
        # Check for missing structures (structures missing before analysis)
        missing_file = os.path.join(output_dir, f"batch_{batch_id}_missing_structures.json")
        if os.path.exists(missing_file):
            try:
                with open(missing_file, 'r') as f:
                    missing_data = json.load(f)
                    
                    # Track total missing structures
                    for structure in missing_data.get('missing_structures', []):
                        all_missing_structures.append({
                            'batch_id': batch_id,
                            'structure': structure,
                            'reason': 'Structure not found in simulation output'
                        })
            except Exception as e:
                print(f"Could not read missing structures data from {missing_file}: {e}")
        
        # Print summary for this batch
        if batch_status[batch_id] == "SUCCESS":
            print(f"✅ Batch {batch_id} analysis completed successfully (all structures processed)")
        elif batch_status[batch_id] == "PARTIAL":
            print(f"⚠️ Batch {batch_id} analysis partially successful ({len(batch_failed_structures)} structures failed)")
            for structure in sorted(batch_failed_structures):
                print(f"   ❌ Failed: {structure}")
        else:
            print(f"❌ Batch {batch_id} analysis failed (no structures processed successfully)")
    
    # Print overall summary with improved categorization
    success_count = sum(1 for status in batch_status.values() if status == "SUCCESS")
    partial_count = sum(1 for status in batch_status.values() if status == "PARTIAL")
    failed_count = sum(1 for status in batch_status.values() if status == "FAILED")
    skipped_count = sum(1 for status in batch_status.values() if status == "SKIPPED")
    
    print(f"\n=== Batch Range Analysis Summary ===")
    print(f"Processed {len(results)} batches")
    print(f"Fully Successful: {success_count}")
    if partial_count > 0:
        print(f"Partially Successful: {partial_count}")
    print(f"Failed: {failed_count}")
    if skipped_count > 0:
        print(f"Skipped: {skipped_count}")
    
    # Print detailed failure information
    if all_failed_structures:
        print(f"\n=== Failed Structures Details ===")
        print(f"Total failed structures: {len(all_failed_structures)}")
        print("Structure failures by batch:")
        
        # Group failures by batch ID for better reporting
        failures_by_batch = {}
        for entry in all_failed_structures:
            batch_id = entry['batch_id']
            structure = entry['structure']
            if batch_id not in failures_by_batch:
                failures_by_batch[batch_id] = []
            failures_by_batch[batch_id].append(structure)
        
        # Print failures organized by batch
        for batch_id in sorted(failures_by_batch.keys()):
            structures = failures_by_batch[batch_id]
            print(f"Batch {batch_id}: {len(structures)} failed structures")
            
            # Only list individual structures if they're not "all"
            if len(structures) == 1 and structures[0] == 'all':
                print(f"  - All structures in batch failed or could not be processed")
            else:
                for structure in sorted(structures):
                    print(f"  - {structure}")
        
        # Save consolidated failed structures report
        report_file = os.path.join(base_dir, f"analysis_failed_structures_{min_batch}-{max_batch}.json")
        try:
            with open(report_file, 'w') as f:
                json.dump(all_failed_structures, f, indent=2)
            print(f"\nDetailed failure report saved to: {report_file}")
        except Exception as e:
            print(f"Could not save consolidated failure report: {e}")
    
    # Print missing structures information
    if all_missing_structures:
        print(f"\n=== Missing Structures Details ===")
        print(f"Total structures missing before analysis: {len(all_missing_structures)}")
        print("Missing structures by batch:")
        
        # Group missing structures by batch ID for better reporting
        missing_by_batch = {}
        for entry in all_missing_structures:
            batch_id = entry['batch_id']
            structure = entry['structure']
            if batch_id not in missing_by_batch:
                missing_by_batch[batch_id] = []
            missing_by_batch[batch_id].append(structure)
        
        # Print missing structures organized by batch
        for batch_id in sorted(missing_by_batch.keys()):
            structures = missing_by_batch[batch_id]
            print(f"Batch {batch_id}: {len(structures)} missing structures")
            
            # Only show up to 10 structures to avoid overwhelming output
            if len(structures) <= 10:
                for structure in sorted(structures):
                    print(f"  - {structure}")
            else:
                for structure in sorted(structures)[:10]:
                    print(f"  - {structure}")
                print(f"  - ... and {len(structures) - 10} more")
        
        # Save consolidated missing structures report
        missing_report_file = os.path.join(base_dir, f"analysis_missing_structures_{min_batch}-{max_batch}.json")
        try:
            with open(missing_report_file, 'w') as f:
                json.dump(all_missing_structures, f, indent=2)
            print(f"\nDetailed missing structures report saved to: {missing_report_file}")
        except Exception as e:
            print(f"Could not save consolidated missing structures report: {e}")
    
    return results

def find_missing_batch_structures(batch_id, output_dir, processed_structures):
    """
    Compare the structures in the original batch CSV file with those processed
    during analysis to identify structures that failed before reaching analysis stage.
    
    Parameters
    ----------
    batch_id : int
        Batch ID to check
    output_dir : str
        Directory containing the analysis results
    processed_structures : list
        List of structure names that were found and processed during analysis
        
    Returns
    -------
    list
        List of structures missing from the analysis phase
    """
    # Try to find the standard batch CSV file
    possible_batch_paths = [
        # Local batches directory
        f"./batches/batch_{batch_id}.csv",
        f"../batches/batch_{batch_id}.csv",
        f"../../batches/batch_{batch_id}.csv",
        # Higher level batches directory
        os.path.join(os.path.dirname(os.path.dirname(output_dir)), "batches", f"batch_{batch_id}.csv")
    ]
    
    batch_file = None
    for path in possible_batch_paths:
        if os.path.exists(path):
            batch_file = path
            break
    
    if not batch_file:
        print(f"Warning: Could not find batch file for batch {batch_id}. Cannot check for missing structures.")
        return []
    
    print(f"Found batch definition file: {batch_file}")
    
    # Read structures from batch file
    batch_structures = []
    try:
        df = pd.read_csv(batch_file)
        
        # Look for structure column (it could have different names)
        structure_col = None
        for col in df.columns:
            if 'structure' in col.lower() or 'file' in col.lower() or 'mof' in col.lower() or 'cif' in col.lower():
                structure_col = col
                break
        
        if not structure_col:
            # Default to first column if we couldn't identify the structure column
            structure_col = df.columns[0]
        
        # Get structures and normalize names
        for structure in df[structure_col]:
            # Clean structure names to make comparison easier
            clean_name = os.path.basename(structure)
            if '.cif' in clean_name:
                clean_name = clean_name.split('.cif')[0]
            batch_structures.append(clean_name)
            
        print(f"Found {len(batch_structures)} structures in batch file")
        
    except Exception as e:
        print(f"Error reading batch file: {e}")
        return []
    
    # Find missing structures by comparing with processed structures
    missing_structures = []
    processed_set = set(processed_structures)
    
    for structure in batch_structures:
        if structure not in processed_set:
            missing_structures.append(structure)
    
    return missing_structures

def main():
    # Set up command line argument parser
    parser = argparse.ArgumentParser(
        description="Process gRASPA output files from a batch of simulations"
    )
    parser.add_argument(
        "batch_id",
        type=int,
        help="Batch ID number"
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing the System*.data output files"
    )
    parser.add_argument(
        "output_dir",
        help="Directory to save the analysis results"
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Disable writing JSON output files"
    )
    parser.add_argument(
        "--update-status",
        action="store_true",
        help="Update job status file with analysis results"
    )
    parser.add_argument(
        "--batch-range",
        action="store_true",
        help="Process a range of batches (batch_id becomes min_batch)"
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        help="Maximum batch ID when using --batch-range"
    )
    parser.add_argument(
        "--results-dir",
        help="Base results directory when using --batch-range"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Handle batch range mode
    if args.batch_range:
        if not args.max_batch:
            print("Error: --max-batch is required when using --batch-range")
            return 1
            
        if not args.results_dir:
            print("Error: --results-dir is required when using --batch-range")
            return 1
            
        # Process the batch range
        results = process_batch_range(
            args.batch_id,  # Used as min_batch
            args.max_batch,
            args.results_dir,
            write_json=not args.no_json,
            update_job_status=args.update_status
        )
        
        # Return success if at least one batch succeeded
        return 0 if any(results.values()) else 1
    else:
        # Process a single batch
        success = process_batch(
            args.batch_id, 
            args.input_dir, 
            args.output_dir, 
            write_json=not args.no_json,
            update_job_status=args.update_status
        )
        
        # Return appropriate exit code
        return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())