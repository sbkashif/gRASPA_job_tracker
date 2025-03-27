import yaml
import os
import subprocess
import importlib.util
import sys
import shutil
import requests
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse
from tqdm import tqdm

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
        self._process_variables()
        self._set_default_paths()  # Set default paths BEFORE validation
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load the configuration from a YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config

    def _process_variables(self):
        """Process variables in the configuration"""
        # Find the project root (git repository root) if possible
        project_root = self._find_project_root()
        
        # Create a dictionary of known variables
        variables = {
            'PROJECT_ROOT': project_root
        }
        
        # Add project variables
        if 'project' in self.config:
            for key, value in self.config['project'].items():
                variables[f'project.{key}'] = value
        
        # Function to replace variables in strings
        def replace_vars(obj):
            if isinstance(obj, str):
                result = obj
                for var_name, var_value in variables.items():
                    result = result.replace(f"${{{var_name}}}", str(var_value))
                return result
            elif isinstance(obj, dict):
                return {k: replace_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_vars(i) for i in obj]
            else:
                return obj
        
        # Replace variables in config
        self.config = replace_vars(self.config)
    
    def _find_project_root(self):
        """Find the Git repository root directory"""
        # Default to one level up from config file location
        default_root = os.path.abspath(os.path.join(os.path.dirname(self.config_path), ".."))
        
        # Try to find Git root if git is available
        try:
            # Start from the config file directory
            start_dir = os.path.dirname(os.path.abspath(self.config_path))
            os.chdir(start_dir)
            
            # Run git rev-parse to find the repository root
            result = subprocess.run(
                ['git', 'rev-parse', '--show-toplevel'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                git_root = result.stdout.strip()
                print(f"Found Git repository root at: {git_root}")
                return git_root
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
        
        print(f"Using default project root: {default_root}")
        return default_root

    def _validate_config(self):
        """Validate that the configuration has all required fields and create directories"""
        required_fields = [
            'project',
            'database',
            'output',
            'batch',
            'scripts',
            'run_file_templates',
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")
        
        # Create output directories
        self._create_output_directories()
        
        # Validate scripts
        self._validate_scripts()
        
        # Validate template files
        self._validate_templates()
        
        # Validate slurm config
        required_slurm_fields = ['account', 'partition', 'time', 'nodes']
        for field in required_slurm_fields:
            if field not in self.config['slurm_config']:
                raise ValueError(f"Missing required SLURM configuration field: {field}")

    def _ensure_database_directory(self):
        """Create the database directory if it doesn't exist"""
        db_path = self.config['database']['path']
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    def _create_output_directories(self):
        """Create all necessary output directories"""
        # Create any directory specified in the output section
        for dir_key, path in self.config['output'].items():
            if path:
                os.makedirs(path, exist_ok=True)
                print(f"Created directory: {path}")
    
    def _validate_scripts(self):
        """Validate script paths and check for module/executable availability"""
        if 'scripts' not in self.config:
            return
            
        for script_name, script_path in self.config['scripts'].items():
            # Skip empty script paths
            if not script_path:
                continue
                
            # Check if it's a Python module path
            if '.' in script_path and not os.path.exists(script_path):
                try:
                    module_parts = script_path.split('.')
                    # Try importing the module
                    module_name = '.'.join(module_parts[:-1])
                    try:
                        importlib.import_module(module_name)
                        print(f"✓ Python module validated: {script_path}")
                    except ImportError:
                        raise ValueError(f"Script module not found: {script_path}")
                except:
                    # If it's not a valid module path, check if it's a file
                    if not os.path.exists(script_path):
                        raise ValueError(f"Script not found: {script_path}")
            else:
                # It's a file path
                if not os.path.exists(script_path):
                    raise ValueError(f"Script not found: {script_path}")
                
                # Check if the script is executable (for Bash scripts)
                if script_path.endswith(('.sh', '.bash')):
                    if not os.access(script_path, os.X_OK):
                        print(f"Warning: Script {script_path} is not executable. Attempting to make it executable...")
                        try:
                            os.chmod(script_path, 0o755)
                            print(f"✓ Made script executable: {script_path}")
                        except PermissionError:
                            print(f"⚠️ Could not make script executable due to permissions: {script_path}")
                print(f"✓ Script file validated: {script_path}")
    
    def _validate_templates(self):
        """Validate template files"""
        for template_name, template_config in self.config['run_file_templates'].items():
            if not isinstance(template_config, dict) or 'file_path' not in template_config:
                raise ValueError(f"Template {template_name} is missing the 'file_path' field")
            
            template_path = template_config['file_path']
            if not os.path.exists(template_path):
                raise ValueError(f"Template file not found: {template_path}")
            print(f"✓ Template validated: {template_name} -> {template_path}")
    
    def _set_default_paths(self):
        """Set default paths if not specified in config"""
        # Ensure output section exists
        if 'output' not in self.config:
            self.config['output'] = {}
            
        # If base_dir is provided, use it as the foundation for all paths
        base_dir = self.config['output'].get('base_dir', None)
        
        # If base_dir not specified but project name is, create default base_dir
        if not base_dir and 'project' in self.config and 'name' in self.config['project']:
            project_name = self.config['project']['name']
            base_dir = os.path.join(os.getcwd(), 'data', project_name)
            self.config['output']['base_dir'] = base_dir
        
        # Set output_dir if not specified
        if 'output_dir' not in self.config['output']:
            self.config['output']['output_dir'] = base_dir
            
        output_dir = self.config['output']['output_dir']
        
        # Auto-generate directory structure
        if 'batches_dir' not in self.config['output']:
            self.config['output']['batches_dir'] = os.path.join(output_dir, 'batches')
            
        # Add other auto-generated directories
        self.config['output']['scripts_dir'] = os.path.join(output_dir, 'job_scripts')
        self.config['output']['logs_dir'] = os.path.join(output_dir, 'job_logs')
        self.config['output']['results_dir'] = os.path.join(output_dir, 'results')
        
        # Only set database path if not already specified
        # This allows users to set an explicit path in the config
        if 'database' in self.config and 'path' not in self.config['database']:
            self.config['database']['path'] = os.path.join(output_dir, 'raw')
        
        # Ensure all output directories exist
        for key, path in self.config['output'].items():
            if isinstance(path, str) and key.endswith('_dir'):
                os.makedirs(path, exist_ok=True)
        
        # Only create database directory if the path is specified
        # This makes database_path optional unless download is requested
        if 'database' in self.config and 'path' in self.config['database']:
            db_path = self.config['database']['path']
            if db_path:
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    def get_config(self) -> Dict[str, Any]:
        """Get the parsed configuration"""
        return self.config
