#!/usr/bin/env python3
"""
Manual testing utility for verifying gRASPA batch results.

This script allows users to manually compare their expected values against 
the actual values in the batch analysis CSV files.
"""

import os
import sys
import argparse
import pandas as pd
import json
import unittest
from colorama import Fore, Style, init
import yaml
from pathlib import Path

# Add project root to path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from gRASPA_job_tracker.config_parser import ConfigParser

# Initialize colorama
init(autoreset=True)

# Define columns to exclude from testing
EXCLUDED_COLUMNS = ["selectivity_(n_co2/n_n2)/(p_co2/p_n2)"]

# Tolerance for numeric comparisons (percentage)
TOLERANCE_PERCENT = 1.0

def find_config_file():
    """Try to find the config file in standard locations."""
    potential_locations = [
        "config.yaml",
        os.path.join(project_root, "config.yaml"),
        os.path.join(project_root, "examples", "config.yaml"),
        os.path.join(project_root, "examples", "config-coremof-clean.yaml")
    ]
    
    for loc in potential_locations:
        if os.path.exists(loc):
            return loc
    
    return None

def get_batch_results_path(config, batch_id):
    """Get the path to the batch results CSV file."""
    try:
        results_dir = config['output']['results_dir']
    except KeyError:
        try:
            # Try alternate structure with base_dir
            results_dir = os.path.join(config['output']['base_dir'], "results")
        except KeyError:
            print(f"{Fore.RED}Cannot determine results directory from config.")
            return None
    
    # Build path to batch analysis results
    analysis_dir = os.path.join(results_dir, f"batch_{batch_id}", "analysis")
    results_file = os.path.join(analysis_dir, f"batch_{batch_id}_all_results.csv")
    
    if not os.path.exists(results_file):
        print(f"{Fore.RED}Results file does not exist: {results_file}")
        return None
    
    return results_file

def list_available_batches(config):
    """List all available batch analysis files."""
    try:
        results_dir = config['output']['results_dir']
    except KeyError:
        try:
            results_dir = os.path.join(config['output']['base_dir'], "results")
        except KeyError:
            print(f"{Fore.RED}Cannot determine results directory from config.")
            return []
    
    if not os.path.exists(results_dir):
        print(f"{Fore.RED}Results directory does not exist: {results_dir}")
        return []
    
    # Find all batch_XXX directories
    batch_dirs = [d for d in os.listdir(results_dir) if d.startswith("batch_")]
    available_batches = []
    
    for batch_dir in batch_dirs:
        batch_id = batch_dir.replace("batch_", "")
        analysis_dir = os.path.join(results_dir, batch_dir, "analysis")
        results_file = os.path.join(analysis_dir, f"batch_{batch_id}_all_results.csv")
        
        if os.path.exists(results_file):
            try:
                df = pd.read_csv(results_file)
                structure_count = len(df)
                available_batches.append((batch_id, structure_count))
            except Exception as e:
                print(f"{Fore.YELLOW}Warning: Could not read {results_file}: {e}")
    
    return sorted(available_batches, key=lambda x: int(x[0]))

def list_structures(batch_results_file):
    """List all structures in a batch results file."""
    try:
        df = pd.read_csv(batch_results_file)
        return df["structure"].tolist()
    except Exception as e:
        print(f"{Fore.RED}Error reading results file: {e}")
        return []

def get_expected_values_from_input(column_names):
    """Get expected values from user input."""
    expected_values = {}
    
    print(f"{Fore.CYAN}Please enter the expected values for the structure:")
    print("(Enter a value for each metric, or press Enter to skip)")
    
    for col in column_names:
        if col == "structure" or col == "unit_cells" or col in EXCLUDED_COLUMNS:
            continue
            
        # Format column name for display
        display_name = col.replace("_", " ").title()
        
        # Ask for user input
        while True:
            try:
                value_input = input(f"{display_name}: ")
                if not value_input:
                    break
                
                # Convert to float
                expected_values[col] = float(value_input)
                break
            except ValueError:
                print(f"{Fore.RED}Invalid number format. Please try again.")
    
    return expected_values

def load_expected_values_from_json(json_file, structure_name=None):
    """Load expected values from a JSON file."""
    # Try to resolve the JSON file path if it doesn't exist
    if not os.path.exists(json_file):
        # Try common locations
        potential_paths = [
            json_file,  # Original path
            os.path.join(os.getcwd(), json_file),  # Current directory
            os.path.join(project_root, json_file),  # Project root
            os.path.join(project_root, "tests", os.path.basename(json_file)),  # Tests directory
            os.path.join(project_root, "examples", os.path.basename(json_file))  # Examples directory
        ]
        
        for path in potential_paths:
            if os.path.exists(path):
                json_file = path
                print(f"{Fore.GREEN}Found JSON file at: {json_file}")
                break
        else:
            print(f"{Fore.RED}Error: Could not find JSON file at any of these locations:")
            for path in potential_paths:
                print(f"  - {path}")
            return None, {}
    
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # If structure name provided, return only that structure's data
        if structure_name:
            for batch_id, batch_data in data.items():
                if structure_name in batch_data:
                    # Return the structure data and its batch ID
                    return batch_id, batch_data[structure_name]
            
            print(f"{Fore.YELLOW}Warning: Structure '{structure_name}' not found in JSON file.")
            return None, {}
        
        return data, {}
    except Exception as e:
        print(f"{Fore.RED}Error reading JSON file: {e}")
        return None, {}

def create_json_template(output_json_file, batch_id, structures, columns):
    """Create a JSON template file with structure names and column names."""
    template = {batch_id: {}}
    
    # Create a template entry for each structure
    for structure in structures:
        template[batch_id][structure] = {}
        for col in columns:
            if col != "structure" and col != "unit_cells" and col not in EXCLUDED_COLUMNS:
                template[batch_id][structure][col] = None
    
    try:
        with open(output_json_file, 'w') as f:
            json.dump(template, f, indent=2)
        print(f"{Fore.GREEN}Created JSON template at: {output_json_file}")
        print(f"{Fore.CYAN}Edit this file to add your expected values, then run the test with:")
        print(f"{Fore.CYAN}python test_batch_results.py --config <config_file> --json {output_json_file}")
    except Exception as e:
        print(f"{Fore.RED}Error creating JSON template: {e}")
        return False
    
    return True

def compare_results(actual_values, expected_values):
    """Compare expected values against actual values."""
    results = []
    
    for col, expected in expected_values.items():
        if col in actual_values and expected is not None and col not in EXCLUDED_COLUMNS:
            actual = actual_values[col]
            
            # Calculate percent difference
            if actual != 0:
                percent_diff = abs((expected - actual) / actual) * 100
            else:
                percent_diff = float('inf') if expected != 0 else 0
            
            # Determine if the values match (within 1%)
            matches = percent_diff < 1.0
            
            results.append({
                "column": col,
                "expected": expected,
                "actual": actual,
                "percent_diff": percent_diff,
                "matches": matches
            })
    
    return results

def print_comparison_results(comparison_results):
    """Print the comparison results in a formatted table."""
    if not comparison_results:
        print(f"{Fore.YELLOW}No values to compare.")
        return
    
    # Calculate column widths
    col_width = max(len(r["column"]) for r in comparison_results) + 2
    
    # Print header
    print("\n" + "=" * 80)
    print(f"{'Column':<{col_width}} {'Expected':<15} {'Actual':<15} {'Diff %':<10} {'Match?'}")
    print("-" * 80)
    
    # Print comparison results
    for result in comparison_results:
        color = Fore.GREEN if result["matches"] else Fore.RED
        match_indicator = "✓" if result["matches"] else "✗"
        
        print(f"{result['column']:<{col_width}} "
              f"{result['expected']:<15.6g} "
              f"{result['actual']:<15.6g} "
              f"{result['percent_diff']:<10.2f} "
              f"{color}{match_indicator}")
    
    # Print summary
    matches = sum(1 for r in comparison_results if r["matches"])
    total = len(comparison_results)
    
    print("=" * 80)
    if matches == total:
        print(f"{Fore.GREEN}All {total} values match! (within 1% tolerance)")
    else:
        print(f"{Fore.YELLOW}Matches: {matches}/{total} values match (within 1% tolerance)")
    print("=" * 80)

def test_single_structure(config, batch_id, structure_name, expected_values=None, json_file=None):
    """Test a single structure."""
    # Get batch results path
    batch_results_file = get_batch_results_path(config, batch_id)
    if not batch_results_file:
        return False
        
    print(f"{Fore.GREEN}Using batch results file: {batch_results_file}")
    
    # Load batch results
    df = pd.read_csv(batch_results_file)
    structure_row = df[df["structure"] == structure_name]
    
    if structure_row.empty:
        print(f"{Fore.RED}Structure '{structure_name}' not found in results.")
        return False
    
    # Get actual values
    actual_values = structure_row.iloc[0].to_dict()
    
    # Show key data about the structure
    print(f"\n{Fore.CYAN}Structure: {Fore.WHITE}{structure_name}")
    print(f"{Fore.CYAN}Unit cells: {Fore.WHITE}{actual_values['unit_cells']}")
    
    # Get expected values from JSON file if provided and not loaded yet
    if json_file and expected_values is None:
        structure_batch_id, expected_values = load_expected_values_from_json(json_file, structure_name)
        if structure_batch_id and structure_batch_id != str(batch_id):
            print(f"{Fore.YELLOW}Warning: Structure is in batch {structure_batch_id} in JSON, but testing against batch {batch_id}.")
    
    # Get expected values from user input if not from JSON
    if not expected_values:
        expected_values = get_expected_values_from_input(df.columns)
    
    # Compare results
    comparison_results = compare_results(actual_values, expected_values)
    
    # Print comparison results
    print_comparison_results(comparison_results)
    
    # Determine if all tests passed
    all_passed = all(r["matches"] for r in comparison_results) if comparison_results else False
    return all_passed

def test_all_structures_from_json(config, json_file):
    """Test all structures specified in a JSON file."""
    # Load the JSON data
    batch_data, _ = load_expected_values_from_json(json_file)
    if not batch_data:
        print(f"{Fore.RED}No valid data found in JSON file.")
        return False
    
    # Track overall test results
    passed_structures = []
    failed_structures = []
    
    # Process each batch in the JSON file
    for batch_id, structures in batch_data.items():
        print(f"\n{Fore.CYAN}Testing batch: {batch_id}")
        
        # Get batch results path
        batch_results_file = get_batch_results_path(config, batch_id)
        if not batch_results_file:
            print(f"{Fore.RED}Could not locate results for batch {batch_id}, skipping this batch.")
            continue
        
        print(f"{Fore.GREEN}Using batch results file: {batch_results_file}")
        
        # Load batch results
        df = pd.read_csv(batch_results_file)
        
        # Test each structure in this batch
        for structure_name, expected_values in structures.items():
            print(f"\n{Fore.CYAN}Testing structure: {structure_name}")
            
            # Check if structure exists in the batch results
            structure_row = df[df["structure"] == structure_name]
            if structure_row.empty:
                print(f"{Fore.RED}Structure '{structure_name}' not found in batch {batch_id} results.")
                failed_structures.append((batch_id, structure_name))
                continue
            
            # Get actual values
            actual_values = structure_row.iloc[0].to_dict()
            
            # Show key data about the structure
            print(f"{Fore.CYAN}Unit cells: {Fore.WHITE}{actual_values['unit_cells']}")
            
            # Compare results
            comparison_results = compare_results(actual_values, expected_values)
            
            # Print comparison results
            print_comparison_results(comparison_results)
            
            # Track if all tests passed for this structure
            if all(r["matches"] for r in comparison_results) and comparison_results:
                passed_structures.append((batch_id, structure_name))
            else:
                failed_structures.append((batch_id, structure_name))
    
    # Print overall summary
    print("\n" + "=" * 80)
    print(f"{Fore.CYAN}Overall Test Results:")
    print(f"{Fore.GREEN}Passed: {len(passed_structures)} structures")
    print(f"{Fore.RED}Failed: {len(failed_structures)} structures")
    
    if failed_structures:
        print(f"\n{Fore.RED}Failed structures:")
        for batch_id, structure in failed_structures:
            print(f"  - Batch {batch_id}: {structure}")
    print("=" * 80)
    
    return len(failed_structures) == 0

def test_against_csv_file(csv_file, json_file):
    """Test expected values against a direct CSV file rather than batch results."""
    print(f"Testing against CSV file: {csv_file}")
    
    # Load the JSON file with expected values
    try:
        batch_data, _ = load_expected_values_from_json(json_file)
        if not batch_data:
            print(f"⚠️ ERROR: No test data found in {json_file}")
            return False
    except Exception as e:
        print(f"⚠️ ERROR loading JSON file: {e}")
        return False
    
    # Load the CSV file
    try:
        df = pd.read_csv(csv_file)
        print(f"Loaded CSV with {len(df)} structure records")
    except Exception as e:
        print(f"⚠️ ERROR loading CSV file: {e}")
        return False
    
    # Track results
    all_passed = True
    total_tests = 0
    passed_tests = 0
    
    # Process each batch and structure in the expected values
    for batch_id, structures in batch_data.items():
        print(f"\nTesting structures from batch {batch_id}:")
        
        for structure_name, expected_values in structures.items():
            # Find the structure in the CSV
            structure_row = df[df["structure"] == structure_name]
            
            if structure_row.empty:
                print(f"  ⚠️ Structure '{structure_name}' not found in CSV file")
                all_passed = False
                continue
                
            # Get actual values
            actual_values = structure_row.iloc[0].to_dict()
            
            # Compare results
            comparison_results = compare_results(actual_values, expected_values)
            total_tests += len(comparison_results)
            passed_tests += sum(1 for r in comparison_results if r["matches"])
            
            # Print results for this structure
            structure_passed = all(r["matches"] for r in comparison_results)
            status = f"{Fore.GREEN}✓" if structure_passed else f"{Fore.RED}✗"
            print(f"  {status} {structure_name}: {sum(1 for r in comparison_results if r['matches'])}/{len(comparison_results)} tests passed{Style.RESET_ALL}")
            
            if not structure_passed:
                # Show failed tests
                for result in comparison_results:
                    if not result["matches"]:
                        print(f"    {Fore.RED}✗ {result['column']}: Expected {result['expected']}, got {result['actual']} (diff: {result['percent_diff']:.2f}%){Style.RESET_ALL}")
                all_passed = False
    
    # Print summary
    print(f"\n{Fore.CYAN}=== Test Summary ==={Style.RESET_ALL}")
    print(f"Total structures tested: {sum(len(structures) for structures in batch_data.values())}")
    print(f"Total tests: {total_tests}")
    print(f"Tests passed: {passed_tests}/{total_tests} ({passed_tests/total_tests*100:.1f}%)")
    
    if all_passed:
        print(f"{Fore.GREEN}All tests passed!{Style.RESET_ALL}")
    else:
        print(f"{Fore.RED}Some tests failed.{Style.RESET_ALL}")
    
    return all_passed

class BatchResultsTestCase(unittest.TestCase):
    """Unit test case for batch results verification."""
    
    def __init__(self, config, batch_id, structure_name, expected_values, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.batch_id = batch_id
        self.structure_name = structure_name
        self.expected_values = expected_values
        self._loadActualValues()
    
    def _loadActualValues(self):
        """Load actual values from the batch results file."""
        batch_results_file = get_batch_results_path(self.config, self.batch_id)
        if not batch_results_file:
            self.skipTest(f"Results file for batch {self.batch_id} not found")
            return
        
        # Load batch results
        df = pd.read_csv(batch_results_file)
        structure_row = df[df["structure"] == self.structure_name]
        
        if structure_row.empty:
            self.skipTest(f"Structure '{self.structure_name}' not found in batch {self.batch_id}")
            return
        
        # Get actual values
        self.actual_values = structure_row.iloc[0].to_dict()
    
    def runTest(self):
        """Run the test by comparing expected values with actual values."""
        # Test each expected value
        for col, expected in self.expected_values.items():
            if col in EXCLUDED_COLUMNS or col == "structure" or col == "unit_cells" or expected is None:
                continue
            
            self.assertIn(col, self.actual_values, f"Column '{col}' not found in actual results")
            
            actual = self.actual_values[col]
            
            # Calculate percent difference for messaging
            if actual != 0:
                percent_diff = abs((expected - actual) / actual) * 100
            else:
                percent_diff = float('inf') if expected != 0 else 0
            
            # Use assertAlmostEqual with a percentage-based tolerance
            self.assertTrue(
                percent_diff < TOLERANCE_PERCENT,
                f"Values for '{col}' differ by {percent_diff:.2f}%: expected {expected}, got {actual}"
            )

def run_unittest_suite(config, json_file):
    """Run unit tests for all structures in the JSON file."""
    # Load the JSON data
    batch_data, _ = load_expected_values_from_json(json_file)
    if not batch_data:
        print(f"{Fore.RED}No valid data found in JSON file.")
        return False
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add tests for each structure in each batch
    for batch_id, structures in batch_data.items():
        for structure_name, expected_values in structures.items():
            # Create a test case for this structure
            test_case = BatchResultsTestCase(
                config=config,
                batch_id=batch_id,
                structure_name=structure_name,
                expected_values=expected_values,
                methodName='runTest'
            )
            suite.addTest(test_case)
    
    # Run the test suite with a test runner that creates more detailed output
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(suite)
    
    # Return True if all tests passed
    return result.wasSuccessful()

def main():
    parser = argparse.ArgumentParser(description='Test batch results manually or via unit tests')
    parser.add_argument('--config', '-c', type=str, help='Path to configuration file')
    parser.add_argument('--batch', '-b', type=str, help='Batch ID to test')
    parser.add_argument('--structure', '-s', type=str, help='Structure name to test')
    parser.add_argument('--list-batches', '-l', action='store_true', help='List available batches')
    parser.add_argument('--json', '-j', type=str, help='JSON file with expected values')
    parser.add_argument('--create-template', '-t', type=str, help='Create a JSON template file with this filename')
    parser.add_argument('--unittest', '-u', action='store_true', help='Run as unit tests with the Python unittest framework')
    parser.add_argument('--csv', type=str, help='Test against a direct CSV file')
    
    args = parser.parse_args()
    
    # Try to find config file if not specified
    config_file = args.config
    if not config_file:
        config_file = find_config_file()
        if not config_file:
            print(f"{Fore.RED}No configuration file found. Please specify with --config.")
            sys.exit(1)
    
    try:
        # Parse config
        config_parser = ConfigParser(config_file)
        config = config_parser.get_config()
        print(f"{Fore.GREEN}Using config file: {config_file}")
        
        # List batches if requested
        if args.list_batches:
            batches = list_available_batches(config)
            if not batches:
                print(f"{Fore.RED}No batch results found.")
                sys.exit(1)
                
            print(f"{Fore.CYAN}Available batches with analysis results:")
            for batch_id, structure_count in batches:
                print(f"  Batch {batch_id}: {structure_count} structures")
            sys.exit(0)
        
        # If using unittest mode with JSON file
        if args.unittest and args.json:
            print(f"{Fore.CYAN}Running in unittest mode with JSON file: {args.json}")
            success = run_unittest_suite(config, args.json)
            sys.exit(0 if success else 1)
        
        # If JSON file is provided without batch and structure, test all structures in JSON
        if args.json and not args.batch and not args.structure and not args.unittest:
            success = test_all_structures_from_json(config, args.json)
            sys.exit(0 if success else 1)
        
        # Test against a direct CSV file if provided
        if args.csv and args.json:
            success = test_against_csv_file(args.csv, args.json)
            sys.exit(0 if success else 1)
        
        # Get batch ID
        batch_id = args.batch
        if not batch_id:
            batches = list_available_batches(config)
            if not batches:
                print(f"{Fore.RED}No batch results found.")
                sys.exit(1)
                
            print(f"{Fore.CYAN}Available batches:")
            for i, (batch_id, structure_count) in enumerate(batches):
                print(f"  {i+1}. Batch {batch_id}: {structure_count} structures")
            
            while True:
                try:
                    choice = int(input(f"{Fore.CYAN}Select a batch number (1-{len(batches)}): "))
                    if 1 <= choice <= len(batches):
                        batch_id = batches[choice-1][0]
                        break
                    else:
                        print(f"{Fore.RED}Invalid choice. Please try again.")
                except ValueError:
                    print(f"{Fore.RED}Invalid input. Please enter a number.")
        
        # Get batch results path
        batch_results_file = get_batch_results_path(config, batch_id)
        if not batch_results_file:
            sys.exit(1)
        
        # Load batch results file to get structures and column names
        df = pd.read_csv(batch_results_file)
        structures = df["structure"].tolist()
        
        # Create a JSON template if requested
        if args.create_template:
            create_json_template(args.create_template, batch_id, structures, df.columns)
            sys.exit(0)
        
        # Test single structure or all from JSON file
        if args.structure:
            # Test a specific structure
            if args.structure not in structures:
                print(f"{Fore.RED}Structure '{args.structure}' not found in batch {batch_id}.")
                sys.exit(1)
            
            success = test_single_structure(config, batch_id, args.structure, json_file=args.json)
            sys.exit(0 if success else 1)
        else:
            # Select a structure interactively
            print(f"{Fore.CYAN}Structures in batch {batch_id}:")
            for i, structure in enumerate(structures):
                print(f"  {i+1}. {structure}")
            
            while True:
                try:
                    choice = int(input(f"{Fore.CYAN}Select a structure (1-{len(structures)}): "))
                    if 1 <= choice <= len(structures):
                        structure_name = structures[choice-1]
                        break
                    else:
                        print(f"{Fore.RED}Invalid choice. Please try again.")
                except ValueError:
                    print(f"{Fore.RED}Invalid input. Please enter a number.")
            
            success = test_single_structure(config, batch_id, structure_name)
            sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"{Fore.RED}Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
