import os
import yaml
import shutil
import subprocess
import importlib.util
from pathlib import Path
from typing import Dict, Any, Optional, List

def create_default_config(output_path: str, template_config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a default configuration
    
    Args:
        output_path: Path where to save the default config
        template_config_path: Optional path to an existing config file to use as template
        
    Returns:
        Default configuration dictionary
    """
    # Try to read configuration from template file if provided
    if template_config_path and os.path.exists(template_config_path):
        try:
            with open(template_config_path, 'r') as f:
                config = yaml.safe_load(f)
            print(f"Using configuration template from: {template_config_path}")
            
        except Exception as e:
            print(f"Error reading template configuration: {e}")
            print("Falling back to default configuration.")
            config = get_hardcoded_default_config()
    else:
        config = get_hardcoded_default_config()
    
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    # Write the default config to file
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    return config

def get_hardcoded_default_config() -> Dict[str, Any]:
    """
    Returns a hardcoded default configuration dictionary
    
    Returns:
        Default configuration dictionary
    """
    # Find current directory to set relative paths
    current_dir = os.path.abspath(os.getcwd())
    project_name = os.path.basename(current_dir)
    
    return {
        'project': {
            'name': project_name,
            'description': 'GRASPA simulation project'
        },
        'database': {
            'path': os.path.join(current_dir, 'data', 'raw'),
            'remote_url': '',  # URL to download database if not present
            'extract': True,  # Whether to extract downloaded archive
        },
        'output': {
            'base_dir': os.path.join(current_dir, 'data', 'processed'),
            'batches_dir': os.path.join(current_dir, 'data', 'processed', 'batches'),
            'partial_charge_dir': os.path.join(current_dir, 'data', 'processed', 'partial_charges'),
            'simulations_dir': os.path.join(current_dir, 'data', 'processed', 'simulations'),
            'analysis_dir': os.path.join(current_dir, 'data', 'processed', 'analysis'),
        },
        'batch': {
            'size': 100,  # Number of structures per batch
            'max_concurrent_jobs': 5,  # Maximum number of concurrent jobs
            'strategy': 'alphabetical',  # Options: alphabetical, size_based, random
            'size_thresholds': [],  # File sizes in bytes for batching if strategy is size_based
            'copy_files': False,  # Whether to copy CIF files to batch directories
        },
        'scripts': {
            'partial_charge': os.path.join(current_dir, 'scripts', 'gen_partial_charge.py'),
            'simulation': os.path.join(current_dir, 'scripts', 'mps_run'),
            'analysis': '',
        },
        'file_templates': {
            'simulation_input_template': os.path.join(current_dir, 'templates', 'simulation.input'),
            'slurm_template': os.path.join(current_dir, 'templates', 'slurm_template.sh'),
        },
        'slurm_config': {
            'account': 'your_account',
            'partition': 'normal',
            'time': '24:00:00',
            'nodes': 1,
            'ntasks_per_node': 16,
            'mem': '32GB',
        },
        'environment_setup': '# Add environment setup commands here\nmodule load conda\nsource activate my_env'
    }

def check_slurm_available() -> bool:
    """
    Check if SLURM is available on the system
    
    Returns:
        True if SLURM commands are available, False otherwise
    """
    try:
        result = subprocess.run(['sinfo', '--version'], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              timeout=2)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired):
        return False

def create_project_structure(base_dir: str, project_name: str) -> Dict[str, str]:
    """
    Create a standard directory structure for a new project
    
    Args:
        base_dir: Base directory to create the structure in
        project_name: Name of the project
        
    Returns:
        Dictionary with paths to created directories
    """
    # Define directory structure
    paths = {
        'project_root': os.path.join(base_dir, project_name),
        'data': os.path.join(base_dir, project_name, 'data'),
        'data_raw': os.path.join(base_dir, project_name, 'data', 'raw'),
        'data_processed': os.path.join(base_dir, project_name, 'data', 'processed'),
        'scripts': os.path.join(base_dir, project_name, 'scripts'),
        'templates': os.path.join(base_dir, project_name, 'templates'),
        'logs': os.path.join(base_dir, project_name, 'logs'),
    }
    
    # Create directories
    for path_name, path in paths.items():
        os.makedirs(path, exist_ok=True)
        print(f"Created directory: {path}")
    
    # Create basic README
    readme_path = os.path.join(paths['project_root'], 'README.md')
    with open(readme_path, 'w') as f:
        f.write(f"# {project_name}\n\n")
        f.write("Project created with GRASPA Job Tracker\n\n")
        f.write("## Directory Structure\n\n")
        f.write("- `data/raw/`: Raw CIF files and databases\n")
        f.write("- `data/processed/`: Processed simulation results\n")
        f.write("- `scripts/`: Processing scripts\n")
        f.write("- `templates/`: Templates for simulation inputs\n")
        f.write("- `logs/`: Log files\n")
    
    print(f"Created README: {readme_path}")
    
    # Copy example templates if they exist in the package
    package_dir = os.path.dirname(os.path.abspath(__file__))
    example_templates = {
        'simulation.input': os.path.join(package_dir, '..', 'templates', 'simulation.input'),
        'slurm_template.sh': os.path.join(package_dir, '..', 'templates', 'slurm_template.sh')
    }
    
    for template_name, template_path in example_templates.items():
        if os.path.exists(template_path):
            dest_path = os.path.join(paths['templates'], template_name)
            shutil.copy(template_path, dest_path)
            print(f"Copied template: {template_name} to {dest_path}")
    
    # Create default config file
    config_path = os.path.join(paths['project_root'], 'config.yaml')
    create_default_config(config_path)
    print(f"Created default config: {config_path}")
    
    return paths


def resolve_installed_script_and_type(module_path: str):
    """
    Given a module-like path (e.g., 'gRASPA_job_tracker.scripts.mps_run'),
    find the installed file and determine if it's a Python or shell script.
    Returns (absolute_path, 'python' or 'shell' or None)
    """
    # Get the root package name
    parts = module_path.split('.')
    if not parts:
        return None, None

    # Find the installed location of the root package
    try:
        spec = importlib.util.find_spec(parts[0])
        if not spec or not spec.submodule_search_locations:
            return None, None
        package_dir = list(spec.submodule_search_locations)[0]
    except Exception:
        return None, None

    # Build the relative path under the package
    rel_path = os.path.join(*parts[1:])
    py_path = os.path.join(package_dir, rel_path + '.py')
    sh_path = os.path.join(package_dir, rel_path + '.sh')

    if os.path.isfile(py_path):
        return py_path, 'python'
    elif os.path.isfile(sh_path):
        return sh_path, 'bash'
    else:
        return None, None
