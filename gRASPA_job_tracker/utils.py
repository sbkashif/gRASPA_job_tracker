import os
import yaml
from typing import Dict, Any

def create_default_config(output_path: str) -> Dict[str, Any]:
    """
    Create a default configuration
    
    Args:
        output_path: Path where to save the default config
        
    Returns:
        Default configuration dictionary
    """
    config = {
        'database_path': '/path/to/cif/files',
        'output_path': '/path/to/output/directory',
        'batch_size': 100,
        'max_concurrent_jobs': 5,
        'partial_charge_script': '/path/to/partial_charge_script.py',
        'simulation_script': '/path/to/simulation_script.sh',
        'simulation_input_file': '/path/to/simulation_input.dat',
        'slurm': {
            'account': 'your_account',
            'partition': 'normal',
            'time': '24:00:00',
            'nodes': 1,
            'ntasks_per_node': 16,
            'mem': '32GB'
        },
        'environment_setup': '# Add environment setup commands here\nmodule load conda\nsource activate my_env'
    }
    
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write the default config to file
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    return config

def check_slurm_available() -> bool:
    """
    Check if SLURM is available on the system
    
    Returns:
        True if SLURM commands are available, False otherwise
    """
    try:
        import subprocess
        subprocess.run(['sinfo'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except (FileNotFoundError, subprocess.SubprocessError):
        return False
