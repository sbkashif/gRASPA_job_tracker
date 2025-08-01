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
    def _evaluate_parameter_matrix(self):
        """Evaluate '!eval ...' expressions in parameter_matrix.parameters, allowing numpy functions."""
        import numpy as np
        param_matrix = self.config.get('parameter_matrix', {})
        if not param_matrix or 'parameters' not in param_matrix:
            return
        params = param_matrix['parameters']
        for key, value in params.items():
            if isinstance(value, str) and value.strip().startswith('!eval '):
                expr = value.strip()[6:].strip()
                # Only allow np.<func> and numpy.<func>
                allowed_names = {'np': np, 'numpy': np}
                try:
                    result = eval(expr, {"__builtins__": {}}, allowed_names)
                except Exception as e:
                    raise ValueError(f"Failed to evaluate parameter expression '{value}': {e}")
                params[key] = result
        self.config['parameter_matrix']['parameters'] = params
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
        self._evaluate_parameter_matrix()  # Evaluate parameter matrix expressions
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load the configuration from a YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config

    def _process_variables(self):
        """Process variables in the configuration, supporting user-defined variables and recursive substitution"""
        import gRASPA_job_tracker


        # Set PACKAGE_PATH to the repo root (parent of the package dir)
        package_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(gRASPA_job_tracker.__file__)), ".."))

        # Add PWD: current working directory
        pwd = os.getcwd()

        # Start with built-in variables
        variables = {
            'PACKAGE_PATH': package_path,
            'PWD': pwd,
        }

        # Add project variables
        if 'project' in self.config:
            for key, value in self.config['project'].items():
                variables[f'project.{key}'] = value

        # Add user-defined variables from the config (e.g., base_dir)
        if 'output' in self.config:
            for key, value in self.config['output'].items():
                variables[key] = value

        # Multi-pass substitution: repeat until no variables remain or max passes
        def substitute_all(obj, variables):
            if isinstance(obj, str):
                prev = None
                result = obj
                # Always substitute PACKAGE_PATH first
                result = result.replace('${PACKAGE_PATH}', str(package_path))
                passes = 0
                while prev != result and passes < 10:
                    prev = result
                    for var_name, var_value in variables.items():
                        if var_name != 'PACKAGE_PATH':
                            result = result.replace(f"${{{var_name}}}", str(var_value))
                    passes += 1
                return result
            elif isinstance(obj, dict):
                return {k: substitute_all(v, variables) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [substitute_all(i, variables) for i in obj]
            else:
                return obj

        # First pass: collect all user-defined variables (e.g., base_dir)
        # This allows referencing base_dir elsewhere in the config
        if 'output' in self.config:
            for key, value in self.config['output'].items():
                if isinstance(value, str):
                    # Substitute built-in variables in user-defined variables
                    variables[key] = substitute_all(value, variables)

        # Now recursively substitute everywhere in the config
        self.config = substitute_all(self.config, variables)

    def _flatten(self, obj):
        """Flatten nested dict/list for variable search"""
        if isinstance(obj, dict):
            for v in obj.values():
                yield from self._flatten(v)
        elif isinstance(obj, list):
            for i in obj:
                yield from self._flatten(i)
        else:
            yield obj
    
    # _find_project_root removed: PROJECT_ROOT is deprecated and no longer supported

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

        # Check if base_dir exists, if not, create it and copy/download database to raw
        base_dir = self.config['output'].get('base_dir')
        raw_dir = os.path.join(base_dir, 'raw')
        db_source = self.config['database'].get('source', None)
        db_url = self.config['database'].get('remote_url', None)
        # Only copy/download if raw_dir does not exist
        if base_dir and not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)

        if not os.path.exists(raw_dir):
            if db_source:
                if os.path.exists(db_source):
                    if os.path.isdir(db_source):
                        shutil.copytree(db_source, raw_dir)
                    else:
                        os.makedirs(raw_dir, exist_ok=True)
                        shutil.copy2(db_source, raw_dir)
                    print(f"✓ Database copied from {db_source} to {raw_dir}")
                else:
                    raise FileNotFoundError(f"Database source does not exist: {db_source}")
            elif db_url:
                os.makedirs(raw_dir, exist_ok=True)
                filename = os.path.basename(urlparse(db_url).path)
                dest_path = os.path.join(raw_dir, filename)
                print(f"Downloading database from {db_url} to {dest_path} ...")
                try:
                    with requests.get(db_url, stream=True) as r:
                        r.raise_for_status()
                        total = int(r.headers.get('content-length', 0))
                        with open(dest_path, 'wb') as f, tqdm(
                            desc=filename, total=total, unit='B', unit_scale=True, unit_divisor=1024
                        ) as bar:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    bar.update(len(chunk))
                    print(f"✓ Database downloaded from {db_url} to {dest_path}")
                except Exception as e:
                    raise RuntimeError(f"Failed to download database from {db_url}: {e}")
                # Auto-extract if archive
                if filename.endswith('.zip'):
                    import zipfile
                    with zipfile.ZipFile(dest_path, 'r') as zip_ref:
                        zip_ref.extractall(raw_dir)
                    print(f"✓ Extracted zip archive to {raw_dir}")
                    os.remove(dest_path)
                elif filename.endswith(('.tar.gz', '.tgz', '.tar')):
                    import tarfile
                    with tarfile.open(dest_path, 'r:*') as tar_ref:
                        tar_ref.extractall(raw_dir)
                    print(f"✓ Extracted tar archive to {raw_dir}")
                    os.remove(dest_path)
            else:
                raise FileNotFoundError(
                    "Database directory does not exist and neither 'source' nor 'remote_url' provided in config."
                )

        # Fail if the database path still does not exist
        if not os.path.exists(raw_dir):
            raise FileNotFoundError(f"Database directory does not exist: {raw_dir}")

        # Create output directories
        self._create_output_directories()

        # Validate scripts
        self._validate_scripts()

        # Validate template files
        self._validate_templates()

        # Validate slurm config
        required_slurm_fields = ['partition', 'time', 'nodes']
        for field in required_slurm_fields:
            if field not in self.config['slurm_config']:
                raise ValueError(f"Missing required SLURM configuration field: {field}")

    def _ensure_database_directory(self):
        """Create the database directory if it doesn't exist"""
        db_path = self.config['database']['path']
        
        # Create the parent directory
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Also create the actual database directory if it doesn't exist
        # This is needed for directory-style database paths ending with '/'
        os.makedirs(db_path, exist_ok=True)
        
        # If database path is a directory and doesn't exist, create it
        if not os.path.exists(db_path):
            os.makedirs(db_path, exist_ok=True)
            print(f"Created empty database directory: {db_path}")

    def _create_output_directories(self):
        """Create all necessary output directories, only print if actually created"""
        for dir_key, path in self.config['output'].items():
            if path:
                if not os.path.exists(path):
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
