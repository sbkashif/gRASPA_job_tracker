"""
Parameter Matrix Manager for multi-layered job allocation
"""

import os
import itertools
from typing import Dict, List, Any, Optional, Tuple
import json

class ParameterMatrix:
    """
    Manages parameter combinations for multi-layered job allocation.
    Each batch can have multiple parameter sets, creating sub-jobs.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the parameter matrix manager
        
        Args:
            config: Configuration dictionary containing parameter_matrix section
        """
        self.config = config
        self.parameter_matrix = config.get('parameter_matrix', {})
        
        # Extract parameter definitions
        self.parameters = self.parameter_matrix.get('parameters', {})
        self.combinations = self.parameter_matrix.get('combinations', 'all')  # 'all' or 'custom'
        self.custom_combinations = self.parameter_matrix.get('custom_combinations', [])
        
        # Generate parameter combinations
        self.param_combinations = self._generate_combinations()
        
        # Output structure
        self.output_path = config['output']['output_dir']
        self.param_matrix_file = os.path.join(self.output_path, 'parameter_matrix.json')
        
        # Create output directory
        os.makedirs(self.output_path, exist_ok=True)
        
        # Save parameter matrix for reference
        self._save_parameter_matrix()
    
    def _generate_combinations(self) -> List[Dict[str, Any]]:
        """
        Generate all parameter combinations based on configuration
        
        Returns:
            List of parameter combination dictionaries
        """
        if not self.parameters:
            # No parameter matrix defined, return single default combination
            return [{'param_id': 0, 'name': 'default', 'parameters': {}}]
        
        combinations = []
        
        if self.combinations == 'all':
            # Generate all possible combinations
            param_keys = list(self.parameters.keys())
            param_values = [self.parameters[key] for key in param_keys]
            
            for i, combo in enumerate(itertools.product(*param_values)):
                param_dict = dict(zip(param_keys, combo))
                combinations.append({
                    'param_id': i,
                    'name': self._generate_param_name(param_dict),
                    'parameters': param_dict
                })
        
        elif self.combinations == 'custom':
            # Use custom combinations
            for i, combo in enumerate(self.custom_combinations):
                combinations.append({
                    'param_id': i,
                    'name': combo.get('name', f'custom_{i}'),
                    'parameters': combo.get('parameters', {})
                })
        
        return combinations
    
    def _generate_param_name(self, param_dict: Dict[str, Any]) -> str:
        """
        Generate a descriptive name for a parameter combination
        
        Args:
            param_dict: Dictionary of parameter values
            
        Returns:
            Descriptive name string
        """
        name_parts = []
        for key, value in param_dict.items():
            # Create consistent, readable parameter abbreviations
            if key.lower() == 'temperature':
                name_parts.append(f"T{value}")
            elif key.lower() == 'pressure':
                name_parts.append(f"P{value}")
            elif 'co2' in key.lower():
                name_parts.append(f"CO2{value}")
            elif 'n2' in key.lower():
                name_parts.append(f"N2{value}")
            else:
                # For other parameters, use first 3 chars of key + value (no underscore)
                short_key = key[:3].upper() if len(key) > 3 else key.upper()
                name_parts.append(f"{short_key}{value}")
        
        return "_".join(name_parts) if name_parts else "default"
    
    def _save_parameter_matrix(self):
        """Save parameter matrix to file for reference"""
        matrix_data = {
            'parameters': self.parameters,
            'combinations': self.combinations,
            'custom_combinations': self.custom_combinations,
            'generated_combinations': self.param_combinations
        }
        
        with open(self.param_matrix_file, 'w') as f:
            json.dump(matrix_data, f, indent=2)
    
    def get_parameter_combinations(self) -> List[Dict[str, Any]]:
        """
        Get all parameter combinations
        
        Returns:
            List of parameter combination dictionaries
        """
        return self.param_combinations
    
    def get_sub_job_id(self, batch_id: int, param_id: int) -> str:
        """
        Generate a unique sub-job ID for a batch-parameter combination
        
        Args:
            batch_id: Batch ID
            param_id: Parameter combination ID
            
        Returns:
            Unique sub-job ID string
        """
        return f"batch_{batch_id}_param_{param_id}"
    
    def get_sub_job_name(self, batch_id: int, param_id: int) -> str:
        """
        Generate a descriptive name for a sub-job
        
        Args:
            batch_id: Batch ID
            param_id: Parameter combination ID
            
        Returns:
            Descriptive sub-job name with batch ID prefix
        """
        if param_id < len(self.param_combinations):
            param_name = self.param_combinations[param_id]['name']
            return f"B{batch_id}_{param_name}"
        else:
            return f"B{batch_id}_param_{param_id}"
    
    def get_sub_job_output_dir(self, batch_id: int, param_id: int) -> str:
        """
        Get the output directory for a specific sub-job
        
        Args:
            batch_id: Batch ID
            param_id: Parameter combination ID
            
        Returns:
            Output directory path
        """
        results_dir = self.config['output']['results_dir']
        sub_job_name = self.get_sub_job_name(batch_id, param_id)
        return os.path.join(results_dir, sub_job_name)
    
    def get_parameters_for_combination(self, param_id: int) -> Dict[str, Any]:
        """
        Get parameter values for a specific combination
        
        Args:
            param_id: Parameter combination ID
            
        Returns:
            Dictionary of parameter values
        """
        if param_id < len(self.param_combinations):
            return self.param_combinations[param_id]['parameters']
        else:
            return {}
    
    def is_enabled(self) -> bool:
        """
        Check if parameter matrix is enabled
        
        Returns:
            True if parameter matrix is defined and enabled
        """
        return bool(self.parameter_matrix and self.parameters)
    
    def get_total_jobs_for_batch(self, batch_id: int) -> int:
        """
        Get total number of sub-jobs for a batch
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Number of sub-jobs (parameter combinations)
        """
        return len(self.param_combinations)
    
    # Note: Template parameter application is now handled by sed operations in job_scheduler.py
    # Parameters are converted to SIM_VAR_ environment variables for consistent processing
