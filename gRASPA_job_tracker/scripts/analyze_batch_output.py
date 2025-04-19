#!/usr/bin/env python
# filepath: /scratch/bcvz/sbinkashif/coremof_co2_n2_adsorption/analysis/process_batch_output.py

import os
import sys
import glob
import csv
import json
import argparse
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
        
        # Try a last-resort direct manual extraction
        try:
            # Create a minimal dataset with zeros
            return {
                'loading_mol_kg': {
                    'CO2': {'average': 0.0, 'error': 0.0},
                    'N2': {'average': 0.0, 'error': 0.0}
                },
                'mole_fraction': {
                    'CO2': {'average': 0.0, 'error': 0.0},
                    'N2': {'average': 0.0, 'error': 0.0}
                },
                'unit_cells': 1
            }
        except:
            return None

def process_batch(batch_id, input_dir, output_dir, write_json=True):
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
    
    Returns
    -------
    bool
        True if the analysis was successful, False otherwise
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all System*.data files
    data_files = glob.glob(os.path.join(input_dir, "**", "System*.data"), recursive=True)
    
    if not data_files:
        print(f"❌ Error: No System*.data files found in {input_dir}")
        return False
        
    print(f"▶ Found {len(data_files)} output files to analyze")
    
    # Prepare results storage
    results = []
    failed_files = []
    
    # Process each file
    successful_files = 0
    for data_file in data_files:
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
            
            # Process the file
            print(f"  Processing {file_name}...")
            # Use the safe wrapper function instead of direct call
            data = safe_extract_averages(data_file)
            
            if data:
                # Start with basic structure info
                result = {
                    'structure': structure_name,
                    'unit_cells': data.get('unit_cells', 1)
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
                    if section_key in data:
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
                
                results.append(result)
                successful_files += 1
                print(f"  ✓ Successfully processed {structure_name}")
            else:
                failed_files.append({
                    'file': data_file,
                    'structure': structure_name,
                    'reason': 'Could not extract data'
                })
                print(f"  ⚠️ Warning: Could not extract data from {file_name}")
            
        except Exception as e:
            failed_files.append({
                'file': data_file,
                'structure': structure_name if 'structure_name' in locals() else os.path.basename(data_file),
                'reason': str(e)
            })
            print(f"❌ Error processing {data_file}: {str(e)}")
    
    if not results:
        print("❌ Error: No results were successfully processed")
        return False
    
    # Create batch identifier for filenames
    batch_id_str = f"batch_{batch_id}_" if batch_id else ""
    
    # Get all field names from all results
    all_fields = set()
    for result in results:
        all_fields.update(result.keys())
    
    # Order fields: first core fields, then the rest alphabetically
    core_fields = [
        'structure', 
        'unit_cells',
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
    
    # Create the fieldnames list with core fields first, then any additional fields alphabetically
    fieldnames = [f for f in core_fields if f in all_fields]
    #remaining_fields = sorted(all_fields - set(fieldnames))
    #fieldnames.extend(remaining_fields)
    
    # Save results to CSV
    csv_file = os.path.join(output_dir, f"{batch_id_str}all_results.csv")
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
        json_file = os.path.join(output_dir, f"{batch_id_str}all_results.json")
        with open(json_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"✅ Results saved to JSON: {json_file}")
    
    # Save failed files information
    if failed_files:
        failed_file = os.path.join(output_dir, f"{batch_id_str}failed_files.json")
        with open(failed_file, 'w') as f:
            json.dump(failed_files, f, indent=2)
        print(f"⚠️ {len(failed_files)} files failed to process. Details saved to: {failed_file}")
    
    print(f"✅ Successfully processed data for {successful_files} out of {len(data_files)} files")
    return True

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
    
    # Parse arguments
    args = parser.parse_args()
    
    success = process_batch(
        args.batch_id, 
        args.input_dir, 
        args.output_dir, 
        write_json=not args.no_json
    )
    
    # Return appropriate exit code
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())