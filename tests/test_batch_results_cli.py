#!/usr/bin/env python3
"""
Unit tests for gRASPA batch results.

This module integrates with Python's unittest framework and can be run with:
python -m unittest discover
"""

import os
import sys
import json
import unittest
import pandas as pd
from pathlib import Path

# Add project root to path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from gRASPA_job_tracker.config_parser import ConfigParser

# Import testing utilities
from test_batch_results import (
    get_batch_results_path, 
    load_expected_values_from_json, 
    EXCLUDED_COLUMNS, 
    TOLERANCE_PERCENT
)

class BatchResultsTest(unittest.TestCase):
    """Test case for comparing batch results with expected values."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test class by loading config and expected values."""
        # Try to find a config file
        potential_config_files = [
            os.path.join(project_root, "config.yaml"),
            os.path.join(project_root, "examples", "config.yaml"),
            os.path.join(project_root, "examples", "config-coremof-clean.yaml")
        ]
        
        cls.config_file = None
        for config_file in potential_config_files:
            if os.path.exists(config_file):
                cls.config_file = config_file
                break
        
        if not cls.config_file:
            raise FileNotFoundError("No configuration file found")
        
        # Parse config
        config_parser = ConfigParser(cls.config_file)
        cls.config = config_parser.get_config()
        
        # Try to find a test JSON file
        cls.json_file = os.path.join(project_root, "tests", "expected_values.json")
        if not os.path.exists(cls.json_file):
            # Try the template if the actual file doesn't exist
            template_file = os.path.join(project_root, "tests", "expected_values_template.json")
            if os.path.exists(template_file):
                cls.json_file = template_file
        
        # Load expected values if JSON file exists
        if hasattr(cls, 'json_file') and os.path.exists(cls.json_file):
            cls.batch_data, _ = load_expected_values_from_json(cls.json_file)
        else:
            cls.batch_data = {}

    def _get_actual_values(self, batch_id, structure_name):
        """Get actual values for a structure from the batch results file."""
        batch_results_file = get_batch_results_path(self.config, batch_id)
        if not batch_results_file:
            self.skipTest(f"Results file for batch {batch_id} not found")
        
        # Load batch results
        df = pd.read_csv(batch_results_file)
        structure_row = df[df["structure"] == structure_name]
        
        if structure_row.empty:
            self.skipTest(f"Structure '{structure_name}' not found in batch {batch_id}")
        
        # Return actual values
        return structure_row.iloc[0].to_dict()

def generate_test_methods():
    """Generate test methods for each structure in the expected values file."""
    if not hasattr(BatchResultsTest, 'batch_data') or not BatchResultsTest.batch_data:
        # Add a placeholder test method if no batch data is available
        def test_no_data(self):
            self.skipTest("No test data available. Create an expected_values.json file in the tests directory.")
        setattr(BatchResultsTest, 'test_no_data', test_no_data)
        return

    # Create a test method for each structure
    for batch_id, structures in BatchResultsTest.batch_data.items():
        for structure_name, expected_values in structures.items():
            # Define a test method for this structure
            def create_test_method(b_id, s_name, exp_values):
                def test_method(self):
                    # Get actual values
                    actual_values = self._get_actual_values(b_id, s_name)
                    
                    # Test each expected value
                    for col, expected in exp_values.items():
                        if col in EXCLUDED_COLUMNS or col == "structure" or col == "unit_cells" or expected is None:
                            continue
                            
                        self.assertIn(col, actual_values, f"Column '{col}' not found in actual results")
                        
                        actual = actual_values[col]
                        
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
                
                return test_method
            
            # Create a unique test method name
            test_method_name = f"test_batch_{batch_id}_{structure_name.replace('-', '_').replace('.', '_')}"
            
            # Add the test method to the class
            setattr(BatchResultsTest, test_method_name, 
                    create_test_method(batch_id, structure_name, expected_values))

# Generate test methods
generate_test_methods()

if __name__ == "__main__":
    unittest.main()
