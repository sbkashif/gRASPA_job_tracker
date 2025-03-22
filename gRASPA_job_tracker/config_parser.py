import yaml
import os
from typing import Dict, Any

class ConfigParser:
    """Parse and validate configuration files for GRASPA job tracking"""
    
    def __init__(self, config_path: str):
        """
        Initialize the config parser
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load the configuration from a YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    def _validate_config(self):
        """Validate that the configuration has all required fields"""
        required_fields = [
            'database_path',
            'output_path',
            'batch_size',
            'max_concurrent_jobs',
            'slurm',
            'partial_charge_script',
            'simulation_script',
            'simulation_input_file'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")
        
        # Validate slurm config
        required_slurm_fields = ['account', 'partition', 'time', 'nodes', 'ntasks_per_node']
        for field in required_slurm_fields:
            if field not in self.config['slurm']:
                raise ValueError(f"Missing required SLURM configuration field: {field}")
        
        # Validate paths exist
        if not os.path.isdir(self.config['database_path']):
            raise ValueError(f"Database path does not exist: {self.config['database_path']}")
        
        if not os.path.exists(self.config['partial_charge_script']):
            raise ValueError(f"Partial charge script not found: {self.config['partial_charge_script']}")
        
        if not os.path.exists(self.config['simulation_script']):
            raise ValueError(f"Simulation script not found: {self.config['simulation_script']}")
        
        if not os.path.exists(self.config['simulation_input_file']):
            raise ValueError(f"Simulation input file not found: {self.config['simulation_input_file']}")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.config['output_path'], exist_ok=True)
    
    def get_config(self) -> Dict[str, Any]:
        """Get the parsed configuration"""
        return self.config
