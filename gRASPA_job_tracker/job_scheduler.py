import os
import subprocess
import importlib
import sys
import re
from typing import Dict, Any, List, Optional, Callable, Union, Tuple, Set
import tempfile
from pathlib import Path
import importlib.util
from .parameter_matrix import ParameterMatrix

class JobScheduler:
    """Handle SLURM job submission and management with support for Python and Bash scripts"""
    
    def __init__(self, config: Dict[str, Any], batch_range: Optional[Tuple[int, int]] = None):
        """
        Initialize the job scheduler
        
        Args:
            config: Configuration dictionary
            batch_range: Optional tuple of (min_batch_id, max_batch_id) to limit which batches are processed
        """
        self.config = config
        self.slurm_config = config['slurm_config']
        
        # Get output paths from config with auto-generated values
        self.output_path = config['output']['output_dir']
        self.scripts_dir = config['output']['scripts_dir']
        self.logs_dir = config['output']['logs_dir']
        
        # Scripts and templates
        self.scripts = config.get('scripts', {})
        self.templates = config.get('run_file_templates', {})
        
        # Create directories for job scripts and logs
        os.makedirs(self.scripts_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Set batch range limits if provided
        self.min_batch_id = None
        self.max_batch_id = None
        if batch_range:
            self.min_batch_id, self.max_batch_id = batch_range
            
        # Initialize parameter matrix for multi-layered job allocation
        self.parameter_matrix = ParameterMatrix(config)
        
        # Dictionary to track batch_id to job_id mapping
        self.batch_job_map = {}
        
        # Create a file to store batch-job mappings
        self.batch_job_map_file = os.path.join(self.output_path, 'batch_job_map.txt')
        
        # Load existing batch-job mappings if file exists
        self._load_batch_job_map()
    
    def _load_batch_job_map(self):
        """Load existing batch-job mappings from file if it exists"""
        if os.path.exists(self.batch_job_map_file):
            try:
                with open(self.batch_job_map_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            parts = line.strip().split()
                            if len(parts) >= 2:
                                try:
                                    batch_id, job_id = parts[0], parts[1]
                                    # Convert batch_id to integer
                                    self.batch_job_map[job_id] = int(batch_id)
                                except ValueError as e:
                                    print(f"Error loading batch-job mapping: {e}")
                                    # Exit on error since all batch IDs should be valid integers
                                    sys.exit(1)
            except Exception as e:
                print(f"Error loading batch-job mapping: {e}")
                sys.exit(1)
    
    def _save_batch_job_map(self):
        """Save batch-job mappings to file"""
        try:
            with open(self.batch_job_map_file, 'w') as f:
                for job_id, batch_id in self.batch_job_map.items():
                    f.write(f"{batch_id} {job_id}\n")
        except Exception as e:
            print(f"Error saving batch-job mapping: {e}")
    
    def is_batch_in_range(self, batch_id: int) -> bool:
        """
        Check if a batch ID is within the specified range
        
        Args:
            batch_id: Batch ID to check
            
        Returns:
            True if batch is in range or no range was specified, False otherwise
        """
        # If no range was specified, all batches are in range
        if self.min_batch_id is None and self.max_batch_id is None:
            return True
            
        # Check minimum bound if specified
        if self.min_batch_id is not None and batch_id < self.min_batch_id:
            return False
            
        # Check maximum bound if specified
        if self.max_batch_id is not None and batch_id > self.max_batch_id:
            return False
            
        return True
    
    def create_job_script(self, 
                          batch_id: int, 
                          batch_files: List[str]) -> Optional[str]:
        """
        Create a SLURM job script for processing a batch of CIF files.
        If parameter matrix is enabled, creates a script that launches multiple sub-jobs.
        
        Args:
            batch_id: ID of the batch
            batch_files: List of CIF file paths in the batch
            
        Returns:
            Path to the created job script or None if batch is out of range
        """
        # Ensure batch_id is an integer
        batch_id = int(batch_id)
        
        # Skip if batch is outside the specified range
        if not self.is_batch_in_range(batch_id):
            print(f"Skipping batch {batch_id}: outside specified batch range")
            return None
        
        # Check if parameter matrix is enabled
        if self.parameter_matrix.is_enabled():
            return self._create_parameter_matrix_job_script(batch_id, batch_files)
        else:
            return self._create_single_job_script(batch_id, batch_files)
    
    def _create_single_job_script(self, 
                                  batch_id: int, 
                                  batch_files: List[str]) -> str:
        """
        Create a single job script (original behavior)
        
        Args:
            batch_id: ID of the batch
            batch_files: List of CIF file paths in the batch
            
        Returns:
            Path to the created job script
        """
        script_path = os.path.join(self.scripts_dir, f'job_batch_{batch_id}.sh')
        
        # Ensure batch_files are all strings
        batch_files = [str(file_path) for file_path in batch_files if file_path]
        
        # Create batch-specific output directory under results_dir
        batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
        os.makedirs(batch_output_dir, exist_ok=True)
        
        # Start with custom template if provided, otherwise use default template
        if 'slurm_template' in self.templates and os.path.exists(self.templates['slurm_template']):
            with open(self.templates['slurm_template'], 'r') as f:
                script_content = f.read()
                
            # Replace template variables
            replacements = {
                '${BATCH_NUMBER}': str(batch_id),
                '${NUM_SAMPLES}': str(len(batch_files)),
                '${OUTPUT_DIR}': batch_output_dir,
            }
            
            for placeholder, value in replacements.items():
                script_content = script_content.replace(placeholder, value)
        else:
            # Create default script content
            script_content = self._create_default_job_script(batch_id, batch_files, batch_output_dir)
        
        # Write script to file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        return script_path
    
    def _create_parameter_matrix_job_script(self, 
                                           batch_id: int, 
                                           batch_files: List[str]) -> str:
        """
        Create individual job scripts for each parameter combination instead of one multi-node job
        
        Args:
            batch_id: ID of the batch
            batch_files: List of CIF file paths in the batch
            
        Returns:
            Path to the coordinator script (for backward compatibility)
        """
        # Get parameter combinations
        param_combinations = self.parameter_matrix.get_parameter_combinations()
        
        # Create individual job scripts for each parameter combination
        job_scripts = []
        for param_combo in param_combinations:
            param_id = param_combo['param_id']
            param_name = param_combo['name']
            
            # Create individual job script for this parameter combination
            script_path = self._create_individual_parameter_job_script(batch_id, batch_files, param_combo)
            job_scripts.append(script_path)
        
        # Create a coordinator script that submits all parameter jobs
        coordinator_script = self._create_parameter_coordinator_script(batch_id, job_scripts, param_combinations)
        
        return coordinator_script
    
    def _create_individual_parameter_job_script(self, 
                                              batch_id: int, 
                                              batch_files: List[str], 
                                              param_combo: Dict[str, Any]) -> str:
        """
        Create a job script for a single parameter combination
        
        Args:
            batch_id: ID of the batch
            batch_files: List of CIF file paths in the batch
            param_combo: Parameter combination dictionary
        Returns:
            Path to the created job script
        """
        param_id = param_combo['param_id']
        param_name = param_combo['name']
        
        # Create script path
        script_path = os.path.join(self.scripts_dir, f'job_batch_{batch_id}_param_{param_id}.sh')
        
        # Create job script content
        script_content = "#!/bin/bash\n\n"
        
        # Add SLURM directives for individual parameter job
        script_content += f"#SBATCH --job-name=g_{batch_id}_p{param_id}\n"
        script_content += f"#SBATCH -o {os.path.join(self.logs_dir, f'batch_{batch_id}_param_{param_id}_%j.out')}\n"
        script_content += f"#SBATCH -e {os.path.join(self.logs_dir, f'batch_{batch_id}_param_{param_id}_%j.err')}\n"
        script_content += f"#SBATCH --nodes=1\n"
        script_content += f"#SBATCH --ntasks-per-node=1\n"
        script_content += f"#SBATCH --cpus-per-task={len(batch_files)}\n"
        
        # Add custom SLURM configuration
        for key, value in self.slurm_config.items():
            if key == 'time' and isinstance(value, (int, float)) and not isinstance(value, str):
                hours, remainder = divmod(int(value), 3600)
                minutes, seconds = divmod(remainder, 60)
                formatted_time = f"{hours}:{minutes:02d}:{seconds:02d}"
                script_content += f"#SBATCH --{key}={formatted_time}\n"
            else:
                script_content += f"#SBATCH --{key}={value}\n"
        
        script_content += "\n"
        
        # Add environment setup if provided
        if 'environment_setup' in self.config:
            script_content += f"{self.config['environment_setup']}\n\n"
        
        # Export environment variables for forcefields
        script_content += "# Set environment variables for forcefield paths\n"
        forcefield_files = self.config.get('forcefield_files', {})
        for key, path in forcefield_files.items():
            script_content += f"export FF_{key.upper()}=\"{path}\"\n"
        
        # Export template files and variables
        script_content += "\n# Handle simulation template and variables\n"
        if 'run_file_templates' in self.config:
            for key, template_config in self.config['run_file_templates'].items():
                if isinstance(template_config, dict):
                    file_path = template_config.get('file_path', '')
                    if file_path and os.path.exists(file_path):
                        script_content += f"export TEMPLATE_{key.upper()}=\"{file_path}\"\n"
                    
                    if 'variables' in template_config:
                        for var_key, var_value in template_config['variables'].items():
                            script_content += f"export SIM_VAR_{var_key}=\"{var_value}\"\n"
        
        # Export parameter-specific environment variables (only SIM_VAR_...)
        script_content += f"\n# Parameter combination {param_id}: {param_name}\n"
        script_content += f"export PARAM_ID={param_id}\n"
        script_content += f"export PARAM_NAME='{param_name}'\n"
        # Export parameter values as PARAM_VAR_ environment variables (for template processing)
        parameters = param_combo['parameters']
        for param_key, param_value in parameters.items():
            script_content += f"export PARAM_VAR_{param_key}=\"{param_value}\"\n"
        script_content += "\n"
        # Organize parameter combination results inside batch directory
        batch_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
        param_dir = os.path.join(batch_dir, param_name)
        script_content += f"# Create output directory for parameter combination {param_id}\n"
        script_content += f"mkdir -p {param_dir}\n"
        script_content += f"cd {param_dir}\n\n"

        # Create parameter-specific batch list file
        param_batch_list = os.path.join(param_dir, "cif_file_list.txt")
        script_content += f"# Create list of CIF files for this parameter combination\n"
        script_content += f"cat > {param_batch_list} << 'EOF'\n"
        for cif_file in batch_files:
            script_content += f"{cif_file}\n"
        script_content += "EOF\n\n"
        
        # Generate workflow steps for this parameter combination
        workflow_steps = self._generate_parameter_workflow_steps(batch_id, param_id, param_dir, param_batch_list)
        
        # Add workflow steps
        script_content += f"# Execute workflow for parameter combination {param_id}\n"
        script_content += f"echo 'Starting parameter combination {param_id}: {param_name}'\n"
        script_content += f"echo 'Job started at: ' `date`\n\n"
        
        script_content += workflow_steps
        
        # Write final exit status
        script_content += f"\n# Write final exit status\n"
        script_content += f"echo $? > {os.path.join(param_dir, 'exit_status.log')}\n"
        script_content += f"echo 'Parameter combination {param_id} completed at: ' `date`\n"
        
        # Write script to file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        return script_path
    
    def _create_parameter_coordinator_script(self, 
                                           batch_id: int, 
                                           job_scripts: List[str], 
                                           param_combinations: List[Dict[str, Any]]) -> str:
        """
        Create a coordinator script that submits all parameter combination jobs
        
        Args:
            batch_id: ID of the batch
            job_scripts: List of job script paths for each parameter combination
            param_combinations: List of parameter combinations
            
        Returns:
            Path to the coordinator script
        """
        coordinator_script = os.path.join(self.scripts_dir, f'job_batch_{batch_id}_coordinator.sh')
        
        script_content = "#!/bin/bash\n\n"
        script_content += f"# Parameter Matrix Coordinator for Batch {batch_id}\n"
        script_content += f"# This script submits individual SLURM jobs for each parameter combination\n\n"
        
        script_content += f"echo 'Starting parameter matrix coordinator for batch {batch_id}'\n"
        script_content += f"echo 'Submitting {len(param_combinations)} parameter combination jobs'\n\n"
        
        # Array to store job IDs
        script_content += "declare -a PARAM_JOB_IDS\n\n"
        
        # Submit each parameter combination job
        for i, (script_path, param_combo) in enumerate(zip(job_scripts, param_combinations)):
            param_id = param_combo['param_id']
            param_name = param_combo['name']
            
            script_content += f"# Submit parameter combination {param_id}: {param_name}\n"
            script_content += f"echo 'Submitting parameter combination {param_id}: {param_name}'\n"
            script_content += f"JOB_ID=$(sbatch --parsable {script_path})\n"
            script_content += f"PARAM_JOB_IDS[{i}]=$JOB_ID\n"
            script_content += f"echo 'Parameter combination {param_id} submitted with job ID: $JOB_ID'\n\n"
        
        # Report submitted job IDs
        script_content += "echo 'All parameter combination jobs submitted:'\n"
        script_content += "for i in \"${!PARAM_JOB_IDS[@]}\"; do\n"
        script_content += "    echo \"  Parameter combination $i: Job ID ${PARAM_JOB_IDS[$i]}\"\n"
        script_content += "done\n\n"
        
        script_content += "echo 'Parameter matrix coordinator completed'\n"
        script_content += "echo 'Use squeue to monitor individual parameter jobs'\n"
        
        # Write script to file
        with open(coordinator_script, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(coordinator_script, 0o755)
        
        return coordinator_script
    
    def _create_parameter_matrix_script_content(self, 
                                               batch_id: int, 
                                               batch_files: List[str], 
                                               param_combinations: List[Dict[str, Any]]) -> str:
        """
        Create script content for parameter matrix job that launches multiple sub-jobs
        
        Args:
            batch_id: ID of the batch
            batch_files: List of CIF file paths in the batch
            param_combinations: List of parameter combinations
            
        Returns:
            Script content string
        """
        script_content = "#!/bin/bash\n\n"
        
        # Get the number of parameter combinations
        num_combinations = len(param_combinations)
        
        # Add SLURM directives for the main job
        script_content += f"#SBATCH --job-name=g_{batch_id}_matrix\n"
        script_content += f"#SBATCH -o {os.path.join(self.logs_dir, f'batch_{batch_id}_matrix_%j.out')}\n"
        script_content += f"#SBATCH -e {os.path.join(self.logs_dir, f'batch_{batch_id}_matrix_%j.err')}\n"
        script_content += f"#SBATCH --nodes={num_combinations}\n"
        script_content += f"#SBATCH --ntasks-per-node=1\n"
        script_content += f"#SBATCH --cpus-per-task={len(batch_files)}\n"
        script_content += f"#SBATCH --gpus-per-node=1\n"
        script_content += f"#SBATCH --gpu-bind=closest\n"
        script_content += f"#SBATCH --no-requeue\n"
        script_content += "\n"
        
        # Add custom SLURM configuration
        for key, value in self.slurm_config.items():
            if key == 'time' and isinstance(value, (int, float)) and not isinstance(value, str):
                hours, remainder = divmod(int(value), 3600)
                minutes, seconds = divmod(remainder, 60)
                formatted_time = f"{hours}:{minutes:02d}:{seconds:02d}"
                script_content += f"#SBATCH --{key}={formatted_time}\n"
            else:
                script_content += f"#SBATCH --{key}={value}\n"
        
        script_content += "\n"
        
        # Add environment setup if provided
        if 'environment_setup' in self.config:
            script_content += f"{self.config['environment_setup']}\n\n"
        
        # Export environment variables for forcefields
        script_content += "# Set environment variables for forcefield paths\n"
        forcefield_files = self.config.get('forcefield_files', {})
        for key, path in forcefield_files.items():
            script_content += f"export FF_{key.upper()}=\"{path}\"\n"
        
        # Export template files
        script_content += "\n# Handle simulation template and variables\n"
        if 'run_file_templates' in self.config:
            for key, template_config in self.config['run_file_templates'].items():
                if isinstance(template_config, dict):
                    file_path = template_config.get('file_path', '')
                    if file_path and os.path.exists(file_path):
                        script_content += f"export TEMPLATE_{key.upper()}=\"{file_path}\"\n"
                    
                    if 'variables' in template_config:
                        for var_key, var_value in template_config['variables'].items():
                            script_content += f"export SIM_VAR_{var_key}=\"{var_value}\"\n"
        
        script_content += "\n"
        
        # Create main batch list file
        main_batch_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
        os.makedirs(main_batch_dir, exist_ok=True)
        batch_list_file = os.path.join(main_batch_dir, "cif_file_list.txt")
        
        script_content += f"# Create list of CIF files for this batch\n"
        script_content += f"cat > {batch_list_file} << 'EOF'\n"
        for cif_file in batch_files:
            script_content += f"{cif_file}\n"
        script_content += "EOF\n\n"
        
        # Launch sub-jobs for each parameter combination
        script_content += f"echo 'Starting parameter matrix job for batch {batch_id} with {num_combinations} parameter combinations'\n"
        script_content += "echo 'Job started at: ' `date`\n\n"
        
        # Array to store background process IDs
        script_content += "declare -a SUB_PIDS\n\n"
        
        # Launch each parameter combination as a separate process
        for i, param_combo in enumerate(param_combinations):
            param_id = param_combo['param_id']
            param_name = param_combo['name']
            
            # Organize parameter combination results inside batch directory
            batch_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
            param_dir = os.path.join(batch_dir, param_name)
            script_content += f"# Launch parameter combination {i+1}/{num_combinations}: {param_name}\n"
            script_content += f"echo 'Starting parameter combination {param_id}: {param_name}'\n"
            script_content += f"mkdir -p {param_dir}\n"

            # Create parameter-specific batch list file
            param_batch_list = os.path.join(param_dir, "cif_file_list.txt")
            script_content += f"cp {batch_list_file} {param_batch_list}\n"
            
            # Launch sub-job in background
            script_content += f"(\n"
            script_content += f"    export PARAM_ID={param_id}\n"
            script_content += f"    export PARAM_NAME='{param_name}'\n"
            
            # Export parameter values as environment variables
            parameters = param_combo['parameters']
            for param_key, param_value in parameters.items():
                script_content += f"    export PARAM_{param_key.upper()}={param_value}\n"
            
            # Generate workflow steps for this parameter combination
            workflow_steps = self._generate_parameter_workflow_steps(batch_id, param_id, param_dir, param_batch_list)
            
            # Add workflow steps with proper indentation
            for line in workflow_steps.split('\n'):
                if line.strip():
                    script_content += f"    {line}\n"

            script_content += f"    echo $? > {os.path.join(param_dir, 'exit_status.log')}\n"
            script_content += f") &\n"
            script_content += f"SUB_PIDS[{i}]=$!\n"
            script_content += f"echo 'Parameter combination {param_id} started with PID ${SUB_PIDS[{i}]}'\n\n"
        
        # Wait for all sub-jobs to complete
        script_content += "echo 'Waiting for all parameter combinations to complete...'\n"
        script_content += "overall_exit_status=0\n\n"
        
        script_content += "for i in \"${!SUB_PIDS[@]}\"; do\n"
        script_content += "    wait ${SUB_PIDS[$i]}\n"
        script_content += "    sub_exit_status=$?\n"
        script_content += "    echo \"Parameter combination $i completed with exit status $sub_exit_status\"\n"
        script_content += "    if [ $sub_exit_status -ne 0 ]; then\n"
        script_content += "        overall_exit_status=$sub_exit_status\n"
        script_content += "    fi\n"
        script_content += "done\n\n"
        
        # Write final completion status
        script_content += f"echo $overall_exit_status > {os.path.join(main_batch_dir, 'exit_status.log')}\n"
        script_content += "echo 'Parameter matrix job completed at: ' `date`\n"
        script_content += "exit $overall_exit_status\n"
        
        return script_content
    
    def _generate_parameter_workflow_steps(self, 
                                          batch_id: int, 
                                          param_id: int, 
                                          output_dir: str, 
                                          file_list: str) -> str:
        """
        Generate workflow steps for a specific parameter combination
        
        Args:
            batch_id: Batch ID
            param_id: Parameter combination ID
            output_dir: Output directory for this parameter combination
            file_list: Path to file containing list of CIF files
            
        Returns:
            Workflow steps as string
        """
        steps_content = ""
        
        # Check if a workflow is defined in the config
        workflow = self.config.get('workflow', None)
        
        # If no explicit workflow is defined, use the scripts section for sequential processing
        if not workflow:
            workflow = []
            for step_name, script_path in self.scripts.items():
                if script_path:
                    workflow.append({
                        'name': step_name,
                        'script': script_path,
                        'output_subdir': step_name,
                        'required': True
                    })
        
        # Track the previous step's output directory
        prev_step_output_dir = None
        
        # Process each workflow step
        for i, step in enumerate(workflow):
            step_name = step.get('name', f'step_{i+1}')
            script_path = step.get('script', self.scripts.get(step_name, ''))
            output_subdir = step.get('output_subdir', step_name)
            required = step.get('required', True)
            
            # Skip if no script is defined
            if not script_path:
                continue
            
            step_output_dir = os.path.join(output_dir, output_subdir)
            step_input = file_list if prev_step_output_dir is None else prev_step_output_dir
            
            # Add step header
            steps_content += f"echo 'Step {i+1}: {step_name.replace('_', ' ').title()} (Parameter {param_id})'\n"
            steps_content += f"mkdir -p {step_output_dir}\n"
            
            # Check if step completed successfully
            exit_status_file = os.path.join(step_output_dir, 'exit_status.log')
            steps_content += f"if [ -f {exit_status_file} ] && [ \"$(cat {exit_status_file})\" = \"0\" ]; then\n"
            steps_content += f"    echo '✓ Step {step_name} already completed successfully, skipping...'\n"
            steps_content += f"else\n"
            steps_content += f"    echo '⚙️ Executing step {step_name}...'\n"
            
            # Generate step execution code
            from .utils import resolve_installed_script_and_type
            script_file, script_type = resolve_installed_script_and_type(script_path)
            
            if script_type == 'bash':
                steps_content += "    " + self._generate_parameter_bash_step(
                    script_file=script_file,
                    step_name=step_name,
                    batch_id=batch_id,
                    param_id=param_id,
                    input_file=step_input,
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)
                ).replace('\n', '\n    ')
            elif script_type == 'python':
                steps_content += "    " + self._generate_parameter_python_step(
                    script_path=script_path,
                    step_name=step_name,
                    batch_id=batch_id,
                    param_id=param_id,
                    input_file=step_input,
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)
                ).replace('\n', '\n    ')
            else:
                raise ValueError(f"Unsupported script type for {script_path}: {script_type}")
            
            # Add status check
            step_var_name = f"{step_name.lower().replace('-', '_')}_status"
            steps_content += f"    {step_var_name}=$?\n"
            steps_content += f"    if [ ${step_var_name} -ne 0 ]; then\n"
            steps_content += f"        echo '❌ {step_name} failed'\n"
            
            if required:
                steps_content += f"        echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}\n"
                steps_content += "        exit 1\n"
            else:
                steps_content += "        # Continue despite failure in this optional step\n"
            
            steps_content += "    fi\n"
            
            # Write exit status to file
            if step_name != 'simulation' and 'mps_run' not in script_path:
                steps_content += f"    echo $? > {exit_status_file}\n"
            
            steps_content += "fi\n"
            
            # Update previous step output directory
            prev_step_output_dir = step_output_dir
        
        return steps_content
    
    def _generate_parameter_bash_step(self, 
                                     script_file: str, 
                                     step_name: str, 
                                     batch_id: int, 
                                     param_id: int,
                                     input_file: str, 
                                     output_dir: str, 
                                     step: Dict[str, Any],
                                     is_first_step: bool = False) -> str:
        """
        Generate bash script execution for a parameter-specific step
        
        Args:
            script_file: Path to the script file
            step_name: Name of the step
            batch_id: Batch ID
            param_id: Parameter combination ID
            input_file: Input file or directory
            output_dir: Output directory for this step
            step: Step configuration dictionary
            is_first_step: Whether this is the first step in the workflow
            
        Returns:
            Bash script content for this step
        """
        content = ""
        
        # Change to output directory and copy script
        script_path = script_file
        script_basename = os.path.basename(script_path)
        local_script = f"{step_name}_{script_basename}"
        
        content += f"cd {output_dir}\n"
        content += f"cp {script_path} ./{local_script}\n"
        content += f"chmod +x ./{local_script}\n"
        
        # Handle template processing with parameter substitution
        template_env_var = ""
        if 'run_file_templates' in self.config and f'{step_name}_input' in self.config['run_file_templates']:
            template_config = self.config['run_file_templates'][f'{step_name}_input']
            if isinstance(template_config, dict) and 'file_path' in template_config:
                template_file_path = template_config['file_path']
                
                if template_file_path and os.path.exists(template_file_path):
                    local_template = f"{step_name}_template_param_{param_id}.input"
                    content += f"# Copy and modify template with parameter values\n"
                    content += f"cp {template_file_path} ./{local_template}\n"
                    
                    # Get template variables from config
                    template_vars = {}
                    if 'variables' in template_config:
                        for var_key, var_value in template_config['variables'].items():
                            template_vars[f"SIM_VAR_{var_key}"] = var_value
                    
                    # Convert parameter matrix parameters to SIM_VAR_ environment variables
                    if self.parameter_matrix.is_enabled():
                        parameters = self.parameter_matrix.get_parameters_for_combination(param_id)
                        for param_key, param_value in parameters.items():
                            # Convert parameter names to PARAM_VAR_ format
                            template_vars[f"PARAM_VAR_{param_key}"] = param_value
                    
                    # Apply all template variable substitutions using sed (same as standard script)
                    for var_name, var_value in template_vars.items():
                        if var_name.startswith('SIM_VAR_'):
                            var_key = var_name.replace('SIM_VAR_', '')
                            content += f"if grep -q \"^{var_key}\" ./{local_template}; then\n"
                            content += f"  sed -i \"s/^{var_key}.*/{var_key} {var_value}/\" ./{local_template}\n"
                            content += f"else\n"
                            content += f"  echo \"{var_key} {var_value}\" >> ./{local_template}\n"
                            content += f"fi\n"
                        elif var_name.startswith('PARAM_VAR_'):
                            # Handle PARAM_VAR_ variables similarly
                            var_key = var_name.replace('PARAM_VAR_', '')
                            # content += f"if grep -q \"^{var_key}\" ./{local_template}; then\n"
                            # content += f"  sed -i \"s/^{var_key}.*/{var_key} {var_value}/\" ./{local_template}\n"
                            # content += f"else\n"
                            # content += f"  echo \"{var_key} {var_value}\" >> ./{local_template}\n"
                            # content += f"fi\n"
                        
                    template_env_var = f"$(pwd)/{local_template}"
                    content += f"export TEMPLATE_{step_name.upper()}_INPUT=\"{template_env_var}\"\n"
        
        # Execute the script
        content += f"# Execute script with parameter-specific settings\n"
        if template_env_var:
            content += f"bash ./{local_script} {batch_id} {input_file} {output_dir} {template_env_var}\n"
        else:
            content += f"bash ./{local_script} {batch_id} {input_file} {output_dir}\n"
        
        content += f"script_status=$?\n"
        content += f"if [ $script_status -eq 0 ]; then\n"
        content += f"    rm -f ./{local_script}\n"
        content += f"fi\n"
        content += f"cd -\n"
        
        return content
    
    def _generate_parameter_python_step(self, 
                                       script_path: str, 
                                       step_name: str, 
                                       batch_id: int, 
                                       param_id: int,
                                       input_file: str, 
                                       output_dir: str, 
                                       step: Dict[str, Any],
                                       is_first_step: bool = False) -> str:
        """
        Generate python script execution for a parameter-specific step
        
        Args:
            script_path: Path to the script
            step_name: Name of the step
            batch_id: Batch ID
            param_id: Parameter combination ID
            input_file: Input file or directory
            output_dir: Output directory for this step
            step: Step configuration dictionary
            is_first_step: Whether this is the first step in the workflow
            
        Returns:
            Python script execution content for this step
        """
        content = ""
        
        # Handle template processing with parameter substitution
        if 'run_file_templates' in self.config and f'{step_name}_input' in self.config['run_file_templates']:
            template_config = self.config['run_file_templates'][f'{step_name}_input']
            if isinstance(template_config, dict) and 'file_path' in template_config:
                template_file_path = template_config['file_path']
                
                if template_file_path and os.path.exists(template_file_path):
                    local_template = f"{step_name}_template_param_{param_id}.input"
                    template_full_path = os.path.join(output_dir, local_template)
                    
                    content += f"# Create parameter-specific template\n"
                    content += f"mkdir -p {output_dir}\n"
                    content += f"cp {template_file_path} {template_full_path}\n"
                    
                    # Get template variables from config
                    template_vars = {}
                    if 'variables' in template_config:
                        for var_key, var_value in template_config['variables'].items():
                            template_vars[f"SIM_VAR_{var_key}"] = var_value
                    
                    # Convert parameter matrix parameters to SIM_VAR_ environment variables
                    if self.parameter_matrix.is_enabled():
                        parameters = self.parameter_matrix.get_parameters_for_combination(param_id)
                        for param_key, param_value in parameters.items():
                            # Convert parameter names to PARAM_VAR_ format
                            template_vars[f"PARAM_VAR_{param_key}"] = param_value
                    
                    # Apply all template variable substitutions using sed (same as bash approach)
                    for var_name, var_value in template_vars.items():
                        if var_name.startswith('SIM_VAR_'):
                            var_key = var_name.replace('SIM_VAR_', '')
                            content += f"if grep -q \"^{var_key}\" {template_full_path}; then\n"
                            content += f"  sed -i \"s/^{var_key}.*/{var_key} {var_value}/\" {template_full_path}\n"
                            content += f"else\n"
                            content += f"  echo \"{var_key} {var_value}\" >> {template_full_path}\n"
                            content += f"fi\n"
                        elif var_name.startswith('PARAM_VAR_'):
                            # Handle PARAM_VAR_ variables similarly
                            var_key = var_name.replace('PARAM_VAR_', '')
                            # content += f"if grep -q \"^{var_key}\" {template_full_path}; then\n"
                            # content += f"  sed -i \"s/^{var_key}.*/{var_key} {var_value}/\" {template_full_path}\n"
                            # content += f"else\n"
                            # content += f"  echo \"{var_key} {var_value}\" >> {template_full_path}\n"
                            # content += f"fi\n"
                    
                    content += f"export TEMPLATE_{step_name.upper()}_INPUT=\"{template_full_path}\"\n"
        
        # Get arguments
        args = step.get('args', [])
        if not args:
            args = [input_file, output_dir]
            
            # Add template path if applicable
            template_key = f"{step_name}_input"
            if 'run_file_templates' in self.config and template_key in self.config['run_file_templates']:
                template_env_var = f"$TEMPLATE_{step_name.upper()}_INPUT"
                args.append(template_env_var)
        
        # Add parameter ID as additional argument
        args_str = ' '.join([str(arg) for arg in [batch_id, param_id] + args])
        
        # Generate command based on script path format
        if '/' in script_path or script_path.endswith('.py'):
            content += f"python {script_path} {args_str}\n"
        else:
            content += f"python -m {script_path} {args_str}\n"
        
        return content
        
        # Write script to file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        return script_path
    
    def _create_default_job_script(self, 
                                   batch_id: int, 
                                   batch_files: List[str], 
                                   batch_output_dir: str) -> str:
        """Create default SLURM job script content with exit status logging."""
        script_content = "#!/bin/bash\n\n"
        
        #Get the number of structures in the batch
        num_structures = len(batch_files)
        
        # Add SLURM directives
        script_content += f"#SBATCH --job-name=g_{batch_id}\n"
        script_content += f"#SBATCH -o {os.path.join(self.logs_dir, f'batch_{batch_id}_%j.out')}\n"
        script_content += f"#SBATCH -e {os.path.join(self.logs_dir, f'batch_{batch_id}_%j.err')}\n"
        script_content += f"#SBATCH --nodes=1\n"
        script_content += f"#SBATCH --ntasks-per-node=1\n"
        script_content += f"#SBATCH --gpus-per-node=1\n"
        script_content += f"#SBATCH --cpus-per-task={num_structures}\n"
        script_content += f"#SBATCH --gpu-bind=closest\n"
        script_content += f"#SBATCH --no-requeue\n"
        script_content += "\n"
        
        # Add custom SLURM configuration, preserving the time format
        for key, value in self.slurm_config.items():
            # Make sure time is in HH:MM:SS format, not seconds
            if key == 'time' and isinstance(value, (int, float)) and not isinstance(value, str):
                # Convert seconds back to HH:MM:SS if needed
                hours, remainder = divmod(int(value), 3600)
                minutes, seconds = divmod(remainder, 60)
                formatted_time = f"{hours}:{minutes:02d}:{seconds:02d}"
                script_content += f"#SBATCH --{key}={formatted_time}\n"
            else:
                script_content += f"#SBATCH --{key}={value}\n"
        
        script_content += "\n"
        
        # Add environment setup if provided
        if 'environment_setup' in self.config:
            script_content += f"{self.config['environment_setup']}\n\n"
        
        # # Set GRASPA environment variables using project_root from config
        # script_content += f"# Set GRASPA environment variables\n"
        
        # # Use project_root directly from config without recalculation
        # project_root = self.config.get('project_root', '')
        
        # # Always set scripts dir to the standard location
        # graspa_scripts_dir = os.path.join(project_root, 'gRASPA_job_tracker', 'scripts')
        
        # # Export the environment variables
        # script_content += f"export GRASPA_SCRIPTS_DIR=\"{graspa_scripts_dir}\"\n"
        # script_content += f"export GRASPA_ROOT=\"{project_root}\"\n\n"
        
        # Extract forcefield paths and set them as environment variables
        script_content += "# Set environment variables for forcefield paths\n"
        forcefield_files = self.config.get('forcefield_files', {})
        for key, path in forcefield_files.items():
            # Set as environment variable - path substitution already done by config_parser
            script_content += f"export FF_{key.upper()}=\"{path}\"\n"
        
        # Extract template files and variables in one pass to avoid redundancy
        script_content += "\n# Handle simulation template and variables\n"
        if 'run_file_templates' in self.config:
            for key, template_config in self.config['run_file_templates'].items():
                if isinstance(template_config, dict):
                    # Process template path - path substitution already done by config_parser
                    file_path = template_config.get('file_path', '')
                    
                    if file_path and os.path.exists(file_path):
                        script_content += f"export TEMPLATE_{key.upper()}=\"{file_path}\"\n"
                    
                    # Process variables specific to this template
                    if 'variables' in template_config:
                        for var_key, var_value in template_config['variables'].items():
                            script_content += f"export SIM_VAR_{var_key}=\"{var_value}\"\n"

        # Create a file with the list of CIF files for this batch
        batch_list_file = os.path.join(batch_output_dir, "cif_file_list.txt")
        script_content += f"# Create list of CIF files for this batch\n"
        script_content += f"cat > {batch_list_file} << 'EOF'\n"
        for cif_file in batch_files:
            script_content += f"{cif_file}\n"
        script_content += "EOF\n\n"
        
        # Add commands to process the batch
        script_content += f"echo 'Starting job for batch {batch_id} with {len(batch_files)} CIF files'\n"
        script_content += "echo 'Job started at: ' `date`\n\n"
        
        # Add workflow steps based on available scripts
        script_content += self._generate_workflow_steps(batch_id, batch_output_dir, batch_list_file)
        
        # Write completion status
        script_content += f"\n# Write completion status\n"
        script_content += f"echo $? > {os.path.join(batch_output_dir, 'exit_status.log')}\n"
        script_content += "echo 'Job completed at: ' `date`\n"
        
        return script_content
    
    def _generate_workflow_steps(self, batch_id: int, output_dir: str, file_list: str) -> str:
        """Generate workflow steps with exit status logging."""
        steps_content = ""
        # Check if a workflow is defined in the config
        workflow = self.config.get('workflow', None)
        
        # If no explicit workflow is defined, use the scripts section for sequential processing
        if not workflow:
            # Create a default workflow from available scripts - keeping original order
            workflow = []
            for step_name, script_path in self.scripts.items():
                if script_path:  # Only include steps with configured scripts
                    workflow.append({
                        'name': step_name,
                        'script': script_path,
                        'output_subdir': step_name,
                        'required': True  # All steps are required by default
                    })
        
        # Track the previous step's output directory to use as input for the next step
        prev_step_output_dir = None
        # Process each workflow step in sequence
        for i, step in enumerate(workflow):
            step_name = step.get('name', f'step_{i+1}')
            script_path = step.get('script', self.scripts.get(step_name, ''))
            output_subdir = step.get('output_subdir', step_name)
            required = step.get('required', True)
            
            # Skip if no script is defined for this step
            if not script_path:
                print(f"Warning: No script defined for step '{step_name}', skipping...")
                continue
                
            step_output_dir = os.path.join(output_dir, output_subdir)
            
            # For the first step, use the original file_list as input
            # For subsequent steps, use the previous step's output directory
            step_input = file_list if prev_step_output_dir is None else prev_step_output_dir
            
            # Add step header
            steps_content += f"echo 'Step {i+1}: {step_name.replace('_', ' ').title()}'\n"
            steps_content += f"mkdir -p {step_output_dir}\n"
            
            # Check if the step has already been completed successfully
            exit_status_file = os.path.join(step_output_dir, 'exit_status.log')
            steps_content += f"# Check if this step has already completed successfully\n"
            steps_content += f"if [ -f {exit_status_file} ] && [ \"$(cat {exit_status_file})\" = \"0\" ]; then\n"
            steps_content += f"    echo '✓ Step {step_name} already completed successfully, skipping...'\n"
            steps_content += f"else\n"
            steps_content += f"    echo '⚙️ Executing step {step_name}...'\n"
            
            # Special case: Always treat mps_run as a bash script regardless of extension
            # is_bash_script = (script_path.endswith(('.sh', '.bash')) or 
            #                  'mps_run' in script_path or 
            #                  step_name == 'simulation')
            script_file, script_type = resolve_installed_script_and_type(script_path)
            
            #Determing if the script is a bash script or a Python module
            #First find the extension from the file path
            
            if script_type == 'bash':
                is_bash_script = True
            elif script_type == 'python':
                is_bash_script = False
            else:
                raise ValueError(f"Unsupported script type for {script_path}: {script_type}. Should be '.sh' or '.py'.")
            if is_bash_script:
                steps_content += "    " + self._generate_bash_step(
                    script_file=script_file,
                    step_name=step_name,
                    batch_id=batch_id,
                    input_file=step_input,  # Use the appropriate input
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)  # Indicate if this is the first step
                ).replace('\n', '\n    ')  # Indent all lines
            else:
                # Python module or script - use directly
                steps_content += "    " + self._generate_python_step(
                    script_path=script_path,
                    step_name=step_name,
                    batch_id=batch_id,
                    input_file=step_input,  # Use the appropriate input
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)  # Indicate if this is the first step
                ).replace('\n', '\n    ')  # Indent all lines
            
            # Add status check
            step_var_name = f"{step_name.lower().replace('-', '_')}_status"
            
            # For simulation scripts, use the stored simulation_status variable
            if 'mps_run' in script_path:
                steps_content += f"    # For mps_run script, use the existing simulation_status variable, else $?\n"
                steps_content += f"    {step_var_name}=${step_var_name}\n"
            else:
                # For regular scripts, capture exit status as usual
                steps_content += f"    {step_var_name}=$?\n"
                
            steps_content += f"    if [ ${step_var_name} -ne 0 ]; then\n"
            steps_content += f"        echo '❌ {step_name} failed'\n"
            
            if required:
                steps_content += f"        echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}\n"
                steps_content += "        exit 1\n"
            else:
                steps_content += "        # Continue despite failure in this optional step\n"
                    
            steps_content += "    fi\n"
            
            # Only write exit status to file for non-simulation steps as simulation exit status is already written
            if step_name != 'simulation' and 'mps_run' not in script_path:
                steps_content += f"    # Write exit status to file\n"
                steps_content += f"    echo $? > {exit_status_file}\n"
            
            # Close the "else" block from "if already completed successfully"
            steps_content += "fi\n"
            
            # Ensure the script does not exit prematurely after the simulation step
            if step_name == 'simulation':
                steps_content += f"# Ensure transition to the next step after simulation\n"
                steps_content += f"echo 'Simulation step completed. Proceeding to analysis...'\n\n"
            
            # Update the previous step output dir for the next iteration
            prev_step_output_dir = step_output_dir
        
        return steps_content
    
    def _generate_bash_step(self, script_file: str, step_name: str, batch_id: int, 
                           input_file: str, output_dir: str, step: Dict[str, Any],
                           is_first_step: bool = False) -> str:
        """Generate bash script execution commands for a workflow step"""
        content = ""
        
        # Copy template if one is specified and exists
        template_key = f"{step_name}_input_template"
        template_path = self.templates.get(template_key, '')
        if template_path and os.path.exists(template_path):
            content += f"cp {template_path} {output_dir}/{step_name}.input\n"
        
        # Handle special case for mps_run which may be a Python module path
        #is_module = not ('/' in script_path or script_path.endswith(('.sh', '.bash', '.py')))
        script_path = script_file
        
        
        script_basename = os.path.basename(script_path)
        local_script = f"{step_name}_{script_basename}"
            
        # Change to output directory
        content += f"# Change to output directory\n"
        content += f"cd {output_dir}\n"
        
        # Copy script locally
        content += f"cp {script_path} ./{local_script}\n"
        content += f"chmod +x ./{local_script}\n"
        script_to_run = f"./{local_script}"
        
        # Add template path if applicable
        template_env_var = ""
        template_file_path = None
        # Find the template path and set environment variable
        if 'run_file_templates' in self.config and f'{step_name}_input' in self.config['run_file_templates']:
            template_config = self.config['run_file_templates'][f'{step_name}_input']
            if isinstance(template_config, dict) and 'file_path' in template_config:
                template_file_path = template_config['file_path']
                step_name_upper = step_name.upper()
                template_env_var = f"$TEMPLATE_{step_name_upper}_INPUT"
                
                # Generate the template file with variable substitution
                if template_file_path and os.path.exists(template_file_path) and 'variables' in template_config:
                    local_template = f"{step_name}_template.input"
                    content += f"# Copy and modify template with variables\n"
                    content += f"cp {template_file_path} ./{local_template}\n"
                    
                    # Process each variable with a simpler approach
                    content += f"# Simple variable replacement for template\n"
                    for var_key, var_value in template_config['variables'].items():
                        content += f"if grep -q \"^{var_key}\" ./{local_template}; then\n"
                        # content += f"  # Replace existing variable\n"
                        # content += f"  sed -i \"s/^{var_key}.*/{var_key} {var_value}/\" ./{local_template}\n"
                        # content += f"else\n"
                        # content += f"  # Add variable if it doesn't exist\n"
                        # content += f"  echo \"{var_key} {var_value}\" >> ./{local_template}\n"
                        # content += f"fi\n"
                    
                    # Update the template environment variable to point to the modified local template
                    content += f"export TEMPLATE_{step_name_upper}_INPUT=\"$(pwd)/{local_template}\"\n"
                    # Also provide the path to scripts file since the shell script is copied to run dir
                    content += f"export {step_name_upper}_SCRIPTS_DIR=\"{os.path.dirname(script_path)}\"\n"
        
        # Execute with batch_id and appropriate arguments
        content += f"# Execute script locally\n"
        
        # # Special handling for mps_run - it expects batch_id, input_dir, output_dir, scripts_dir
        # if 'mps_run' in script_path:
        #     # For mps_run, input_file is a directory with CIF files if not first step
        #if not is_first_step:
        
        
        content += f"# Run simulation and IMMEDIATELY capture its exit status\n"
        if not template_env_var:
            content += f"bash {script_to_run} {batch_id} {input_file} {output_dir}\n"
        else:
            content += f"bash {script_to_run} {batch_id} {input_file} {output_dir} {template_env_var}\n"
        content += f"simulation_status=$?\n"
        
        # Store the status for later use before any other commands execute
        content += f"# Write exit status to log file immediately\n"
        content += f"echo $simulation_status > exit_status.log\n"
            
        # else:
        #     # For first step or when input_file is a file list
        #     content += f"# First step: setting up input/output directories\n"
        #     # We're already in the output directory, pass batch_id, input_dir, output_dir, scripts_dir
        #     content += f"bash {script_to_run} {batch_id} {input_file} {output_dir}\n"
        #     content += f"simulation_status=$?\n"
            
        #     # Store the status for later use before any other commands execute
        #     content += f"# Write exit status to log file immediately\n"
        #     content += f"echo $simulation_status > exit_status.log\n"
            
        #     # Export template as environment variable instead of argument
        #     if template_env_var and not template_file_path:  # Only if we didn't already set it above
        #         content += f"export TEMPLATE_SIMULATION_INPUT={template_env_var}\n"
            
        #     # Also provide input file as an environment variable for mps_run
        #     content += f"export MPS_INPUT_FILE=\"{input_file}\"\n"
        # # else:
        #     # Regular script execution
        #     if template_env_var:
        #         content += f"bash {script_to_run} {batch_id} {input_file} {template_env_var}\n"
        #     else:
        #         content += f"bash {script_to_run} {batch_id} {input_file}\n"
        
        # # Capture exit status and clean up on success
        # if 'mps_run' in script_path:
        #     # For mps_run scripts, use the already captured simulation_status
        #     content += f"script_status=$simulation_status\n"
        # else:
        #     # For regular scripts, capture the exit status now
        #    content += f"script_status=$?\n"
        
        content += f"script_status=$?\n"
            
        content += f"if [ $script_status -eq 0 ]; then\n"
        content += f"    # Clean up unnecessary files on success\n"
        content += f"    rm -f ./{local_script}\n"
        content += f"fi\n"
        
        # Return to original directory and pass through the exit status
        content += f"cd -\n"
        
        # Remove the explicit exit call for mps_run but keep for other scripts
        # This fixes the premature job termination issue
        if 'mps_run' in script_path:
            content += f"# Avoid exit for simulation scripts to prevent premature job termination\n"
            content += f"simulation_status=$script_status\n"
        else:
            content += f"exit $script_status\n"
        
        # else:
        #     # Regular bash script - run from original location
        #     # Get additional arguments if specified
        #     args = step.get('args', [input_file, output_dir])
            
        #     # Add template path if applicable
        #     template_key = f"{step_name}_input"
        #     template_env_var = None
        #     if 'run_file_templates' in self.config and template_key in self.config['run_file_templates']:
        #         template_env_var = f"$TEMPLATE_{step_name.upper()}_INPUT"
        #         if isinstance(args, list) and template_env_var not in args:
        #             args.append(template_env_var)
            
        #     if isinstance(args, list):
        #         args_str = ' '.join([str(arg) for arg in [batch_id] + args])
        #     else:
        #         args_str = f"{batch_id} {input_file} {output_dir}"
        #         if template_env_var:
        #             args_str += f" {template_env_var}"
                
        #     content += f"bash {script_path} {args_str}\n"
            
        return content
    
    def _generate_python_step(self, script_path: str, step_name: str, batch_id: int, 
                             input_file: str, output_dir: str, step: Dict[str, Any],
                             is_first_step: bool = False) -> str:
        """Generate python script execution commands for a workflow step"""
        content = ""
        
        # Special handling for simulation scripts or those needing to run in output directory
        if step.get('change_dir', False):
            # Get script basename for local copy if it's a file
            if '/' in script_path or script_path.endswith('.py'):
                script_basename = os.path.basename(script_path)
                local_script = f"{step_name}_{script_basename}"
                
                # Change to output directory and copy script locally
                content += f"# Change to output directory and copy script locally\n"
                content += f"cd {output_dir}\n"
                content += f"cp {script_path} ./{local_script}\n"
                
                # Get arguments
                args = step.get('args', [])
                if not args:
                    args = [input_file, output_dir]
                    
                    # Add template path if applicable
                    template_key = f"{step_name}_input"
                    if 'run_file_templates' in self.config and template_key in self.config['run_file_templates']:
                        template_env_var = f"$TEMPLATE_{step_name.upper()}_INPUT"
                        args.append(template_env_var)
                
                # Format arguments
                args_str = ' '.join([str(arg) for arg in [batch_id] + args])
                
                # Execute script
                content += f"# Execute script locally\n"
                content += f"python ./{local_script} {args_str}\n"
                
                # Capture exit status and clean up on success - fix potential escaping/string interpolation issues
                content += "script_status=$?\n"
                content += "if [ $script_status -eq 0 ]; then\n"
                content += "    # Clean up unnecessary files on success\n"
                content += f"    rm -f ./{local_script}\n"
                content += "fi\n"
                
                # Return to original directory
                content += f"cd -\n"
            else:
                # It's a module name - run with -m flag in output directory
                content += f"cd {output_dir}\n"
                
                # Get arguments
                args = step.get('args', [])
                if not args:
                    args = [input_file, output_dir]
                    
                    # Add template path if applicable
                    template_key = f"{step_name}_input"
                    if 'run_file_templates' in self.config and template_key in self.config['run_file_templates']:
                        template_env_var = f"$TEMPLATE_{step_name.upper()}_INPUT"
                        args.append(template_env_var)
                
                # Format arguments
                args_str = ' '.join([str(arg) for arg in [batch_id] + args])
                
                # Execute module
                content += f"python -m {script_path} {args_str}\n"
                content += "script_status=$?\n"
                content += f"cd -\n"
                
                # Prevent premature job termination for simulation scripts
                if step_name == 'simulation':
                    content += f"# Skip exit for simulation step to prevent premature job termination\n"
                else:
                    content += f"exit $script_status\n"
        else:
            # Regular Python script/module - run from original location
            # Get arguments
            args = step.get('args', [])
            if not args:
                args = [input_file, output_dir]
                
                # Add template path if applicable
                template_key = f"{step_name}_input"
                if 'run_file_templates' in self.config and template_key in self.config['run_file_templates']:
                    template_env_var = f"$TEMPLATE_{step_name.upper()}_INPUT"
                    args.append(template_env_var)
            
            # Format arguments
            args_str = ' '.join([str(arg) for arg in [batch_id] + args])
            
            # Generate command based on script path format
            if '/' in script_path or script_path.endswith('.py'):
                # It's a file path, run directly
                content += f"python {script_path} {args_str}\n"
            else:
                # It's a module name, use -m flag
                content += f"python -m {script_path} {args_str}\n"
        return content
    
    def print_job_script(self, script_path: str) -> None:
        """
        Print the content of a job script to stdout
        
        Args:
            script_path: Path to the job script
        """
        try:
            with open(script_path, 'r') as f:
                print("\n===== JOB SCRIPT CONTENT =====")
                print(f.read())
                print("==============================\n")
        except Exception as e:
            print(f"Error reading job script: {e}")
    
    def submit_job(self, script_path: str, dry_run: bool = False, batch_id: Optional[int] = None,
                   force_resubmission: bool = False) -> Optional[str]:
        """
        Submit a job to SLURM or perform a dry run
        
        Args:
            script_path: Path to the job script
            dry_run: If True, only print the job script and don't submit
            batch_id: The batch ID associated with this job
            force_resubmission: If True, clear any existing entries for this batch ID
            
        Returns:
            Job ID if submission was successful, "dry-run" for dry run, None on error or if out of range
        """
        # Handle None script_path (when batch is out of range)
        if script_path is None:
            return None
            
        # Ensure script_path is a string
        script_path = str(script_path)
        
        if not os.path.exists(script_path):
            print(f"Error: Job script does not exist: {script_path}")
            return None
            
        if dry_run:
            print(f"[DRY RUN] Job script generated at: {script_path}")
            #self.print_job_script(script_path)
            print(f"[DRY RUN] To submit manually: sbatch {script_path}")
            return "dry-run"
        
        import re
        try:
            print(f"Submitting job script: {script_path}")
            result = subprocess.run(['sbatch', script_path], 
                                   check=True, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
            # Extract job ID from sbatch output (robustly handle cluster name)
            output = result.stdout.strip()
            job_id = None
            if "Submitted batch job" in output:
                # Use regex to extract the first integer after 'Submitted batch job'
                match = re.search(r"Submitted batch job (\d+)", output)
                if match:
                    job_id = match.group(1)
                else:
                    # Fallback: try to find any integer in the output
                    match = re.search(r"(\d+)", output)
                    if match:
                        job_id = match.group(1)
                if job_id:
                    print(f"Job submitted successfully with ID: {job_id}")
                    # Store batch_id to job_id mapping if batch_id is provided
                    if batch_id is not None:
                        self.batch_job_map[job_id] = batch_id
                        self._save_batch_job_map()
                        # Update the job status CSV file with the new job
                        self.update_job_status_csv(job_id=job_id, batch_id=batch_id, 
                                                 force_resubmission=force_resubmission)
                    return job_id
                else:
                    print(f"❌ Could not parse job ID from sbatch output: {output}")
                    return None
            print(f"Job submission output: {output}")
            return None
        except subprocess.CalledProcessError as e:
            print(f"Failed to submit job: {e}")
            print(f"stderr: {e.stderr}")
            return None
    
    def get_job_status(self, job_id: str, batch_output_dir: Optional[str] = None) -> str:
        """
        Get the status of a SLURM job or check exit_status.log if available.

        Args:
            job_id: SLURM job ID
            batch_output_dir: Directory containing exit_status.log (optional)

        Returns:
            Job status (PENDING, RUNNING, COMPLETED, FAILED, etc.) or 'UNKNOWN' if job not found
        """
        # Check exit_status.log in the batch's subfolder
        if batch_output_dir:
            exit_status_file = os.path.join(batch_output_dir, "exit_status.log")
            if os.path.exists(exit_status_file):
                with open(exit_status_file, "r") as f:
                    status = f.read().strip()
                    if status == "0":
                        return "COMPLETED"
                    else:
                        return "FAILED"

        # Fallback to SLURM commands
        # Ensure job_id is a string
        job_id = str(job_id)
        
        # Handle special case for dry-run jobs
        if job_id == "dry-run":
            return "DRY-RUN"
            
        try:
            result = subprocess.run(['squeue', '--job', job_id, '--format=%T', '--noheader'],
                                   check=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
            
            output = result.stdout.strip()
            if output:
                return output
            else:
                # If no output, check if job completed or failed
                sacct_result = subprocess.run(
                    ['sacct', '-j', job_id, '--format=State', '--noheader', '--parsable2'],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                sacct_output = sacct_result.stdout.strip()
                if sacct_output:
                    # Get the first state that isn't for a step (like .batch or .extern)
                    for state in sacct_output.split('\n'):
                        if state and '.' not in state:
                            return state
                
            return 'UNKNOWN'
            
        except subprocess.CalledProcessError:
            return 'UNKNOWN'
    
    def get_batch_id_for_job(self, job_id: str) -> Optional[int]:
        """
        Get the batch ID associated with a job ID
        
        Args:
            job_id: SLURM job ID
            
        Returns:
            Batch ID if found, None otherwise
        """
        return self.batch_job_map.get(job_id)
    
    def _format_datetime(self, timestamp):
        """
        Format a timestamp as a human-readable date and time string.
        
        Args:
            timestamp: Unix timestamp (float) or timestamp string
            
        Returns:
            Formatted date and time string (YYYY-MM-DD HH:MM:SS)
        """
        import datetime
        
        # Convert string to float if needed
        if isinstance(timestamp, str):
            try:
                timestamp = float(timestamp)
            except ValueError:
                # If it's already a formatted string, return it
                if len(timestamp) > 10 and "-" in timestamp:
                    return timestamp
                return "Unknown"
        
        # Return empty string for None or zero timestamps
        if not timestamp:
            return ""
            
        try:
            dt = datetime.datetime.fromtimestamp(timestamp)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OverflowError):
            return "Invalid time"

    def _get_current_workflow_stage(self, batch_id: int, status: str) -> str:
        """
        Determine the current workflow stage of a job based on output directories and status.
        
        Args:
            batch_id: Batch ID of the job
            status: Current job status
            
        Returns:
            String representing the current workflow stage
        """
        # For simple statuses, return as-is
        if status in ["PENDING", "DRY-RUN"]:
            return status.lower()
        
        if status in ["COMPLETED", "CANCELLED", "TIMEOUT", "UNKNOWN"]:
            return status.lower()
            
        if status == "FAILED":
            return "failed"
        
        if status == "NEVER_SUBMITTED":
            return "never_submitted"
            
        if status == "PARTIALLY_COMPLETE":
            # For partially complete jobs, find the last completed step using exit status files
            batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
            if not os.path.exists(batch_output_dir):
                return "partially_complete"
                
            # Get workflow steps from config
            workflow_steps = []
            
            # Try to extract workflow stages in different ways depending on config structure
            if 'workflow' in self.config and self.config['workflow']:
                # Extract from explicit workflow definition
                workflow_steps = [step.get('name', f'step_{i+1}') for i, step in enumerate(self.config['workflow'])]
            elif 'scripts' in self.config and self.config['scripts']:
                # Extract from scripts section
                workflow_steps = list(self.config['scripts'].keys())
            
            # Use default steps if none found in config
            if not workflow_steps:
                workflow_steps = ['partial_charge', 'simulation', 'analysis']
            
            # Check for completed steps by looking for exit_status.log files with "0" value
            completed_steps = []
            last_successful_step = None
            
            # For each workflow step, check its exit status file
            for step in workflow_steps:
                step_dir = os.path.join(batch_output_dir, step)
                exit_status_file = os.path.join(step_dir, 'exit_status.log')
                
                # If the directory doesn't exist, this step wasn't reached
                if not os.path.exists(step_dir):
                    continue
                    
                # Check if exit status file exists and read its content
                if os.path.exists(exit_status_file):
                    try:
                        with open(exit_status_file, 'r') as f:
                            exit_status = f.read().strip()
                            if exit_status == '0':
                                # Step completed successfully
                                completed_steps.append(step)
                                last_successful_step = step
                    except:
                        # If we can't read the file, consider the step didn't complete
                        pass
            
            # If we found completed steps, report the last one
            if last_successful_step:
                return f"partially_complete (completed: {last_successful_step})"
            
            # If we couldn't determine which steps completed
            return "partially_complete"
        
        # For RUNNING jobs - determine which workflow stage they're in
        batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
        if not os.path.exists(batch_output_dir):
            return "initializing"
            
        # Get workflow steps from config
        workflow_steps = []
        
        # Try to extract workflow stages in different ways depending on config structure
        if 'workflow' in self.config and self.config['workflow']:
            # Extract from explicit workflow definition
            workflow_steps = [step.get('name', f'step_{i+1}') for i, step in enumerate(self.config['workflow'])]
        elif 'scripts' in self.config and self.config['scripts']:
            # Extract from scripts section
            workflow_steps = list(self.config['scripts'].keys())
        
        #Raise error if no workflow steps found
        assert workflow_steps, "No workflow steps defined in configuration"
        
        # Check for latest stage in reverse order (latest to earliest)
        for step in reversed(workflow_steps):
            step_dir = os.path.join(batch_output_dir, step)
            exit_status_file = os.path.join(step_dir, 'exit_status.log')
            
            # If directory exists, this stage has started
            if os.path.exists(step_dir):
                if os.path.exists(exit_status_file):
                    # Check exit status to see if step completed
                    try:
                        with open(exit_status_file, 'r') as f:
                            exit_code = f.read().strip()
                            if exit_code == '0':
                                # Step completed successfully, continuing to next one
                                continue
                            else:
                                # Step failed
                                return f"{step} (failed)"
                    except:
                        pass
                        
                # Stage directory exists but no exit status or non-zero status
                # Check for specific activity indicators within the stage
                if step == 'simulation':
                    # Check for RASPA log files to determine simulation progress
                    import glob
                    raspa_logs = glob.glob(os.path.join(step_dir, '**', 'Output', 'System_0', '*.data'), recursive=True)
                    if raspa_logs:
                        # Found RASPA output data - check for cycle information
                        try:
                            latest_log = sorted(raspa_logs, key=os.path.getmtime)[-1]
                            with open(latest_log, 'r') as f:
                                content = f.read()
                                if 'Production cycle:' in content:
                                    # Extract last production cycle
                                    import re
                                    cycles = re.findall(r'Production cycle:\s*(\d+)', content)
                                    if cycles:
                                        last_cycle = cycles[-1]
                                        return f"{step} (cycle {last_cycle})"
                        except:
                            pass
                    
                    # If we can't extract cycle info but the dir exists
                    return f"{step} (running)"
                
                # For other steps, just report the step name
                return step
        # If no stage directories found but job is running
        return "unknown_stage (running)"

    def update_job_status_csv(self, job_id: str = None, batch_id: int = None, force_resubmission: bool = False):
        """
        Update the job_status.csv file with current job statuses.
        If job_id and batch_id are provided, update only that job.
        Otherwise, update all jobs in the batch_job_map.

        Args:
            job_id: Specific job ID to update (optional)
            batch_id: Specific batch ID to update (optional)
            force_resubmission: If True, clear any existing entries for this batch ID (for resubmission)
        """
        import csv
        import time
        
        csv_file = os.path.join(self.output_path, 'job_status.csv')
        
        # Read existing data if file exists
        job_data = {}
        
        # If force_resubmission is True and we have a specific batch_id, skip loading existing data for that batch
        if force_resubmission and batch_id:
            batch_id_str = str(batch_id)
            if os.path.exists(csv_file):
                try:
                    with open(csv_file, 'r') as f:
                        reader = csv.reader(f)
                        header = next(reader, None)  # Read header
                        
                        for row in reader:
                            if len(row) >= 3:  # Ensure row has at least batch_id, job_id, status
                                if row[0] != batch_id_str:  # Skip the batch_id we're resubmitting
                                    # Ensure row has enough elements for all columns including workflow_stage
                                    while len(row) < 6:
                                        row.append('')
                                        
                                    job_data[row[0]] = row
                except Exception as e:
                    print(f"Warning: Error reading job status CSV: {e}")
        else:
            if os.path.exists(csv_file):
                try:
                    with open(csv_file, 'r') as f:
                        reader = csv.reader(f)
                        header = next(reader, None)  # Read header
                        
                        for row in reader:
                            if len(row) >= 3:  # Ensure row has at least batch_id, job_id, status
                                # Ensure row has enough elements for all columns including workflow_stage
                                while len(row) < 6:
                                    row.append('')
                                    
                                job_data[row[0]] = row
                except Exception as e:
                    print(f"Warning: Error reading job status CSV: {e}")
        
        # Update specific job if provided
        if job_id and batch_id:
            batch_id_str = str(batch_id)
            # Validate job_id: must be int or 'dry-run', never cluster name or other string
            valid_job_id = False
            if job_id == "dry-run":
                valid_job_id = True
            else:
                try:
                    _ = int(job_id)
                    valid_job_id = True
                except Exception:
                    print(f"❌ Invalid job_id in update_job_status_csv for batch {batch_id}: {job_id}. Skipping row.")
                    valid_job_id = False

            if not valid_job_id:
                return  # Do not write invalid job_id to CSV

            status = self.get_job_status(job_id)

            # Get batch output directory to check for exit_status.log
            batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
            if os.path.exists(batch_output_dir):
                status = self.get_job_status(job_id, batch_output_dir)

            # Get the workflow stage - always calculate this
            workflow_stage = self._get_current_workflow_stage(batch_id, status)

            # Update or add entry
            if batch_id_str in job_data:
                job_data[batch_id_str][1] = job_id
                job_data[batch_id_str][2] = status
                job_data[batch_id_str][5] = workflow_stage  # Always set workflow_stage

                # Update completion time if job is completed and no completion time is set
                if status in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'UNKNOWN'] and not job_data[batch_id_str][4]:
                    job_data[batch_id_str][4] = self._format_datetime(time.time())
            else:
                # Format the submission time as a readable date-time string
                current_time = time.time()
                formatted_time = self._format_datetime(current_time)
                job_data[batch_id_str] = [batch_id_str, job_id, status, formatted_time, '', workflow_stage]
        else:
            # Update all jobs in the batch_job_map
            for job_id, batch_id in self.batch_job_map.items():
                batch_id_str = str(batch_id)
                
                # Get batch output directory to check for exit_status.log
                batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
                status = self.get_job_status(job_id, batch_output_dir if os.path.exists(batch_output_dir) else None)
                
                # Get the workflow stage - always calculate this for every job
                workflow_stage = self._get_current_workflow_stage(batch_id, status)
                
                # Update or add entry
                if batch_id_str in job_data:
                    job_data[batch_id_str][1] = job_id
                    job_data[batch_id_str][2] = status
                    job_data[batch_id_str][5] = workflow_stage  # Always set workflow_stage
                    
                    # Update completion time if job is completed and no completion time is set
                    if status in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'UNKNOWN'] and not job_data[batch_id_str][4]:
                        job_data[batch_id_str][4] = self._format_datetime(time.time())
                else:
                    # Format the submission time as a readable date-time string
                    current_time = time.time()
                    formatted_time = self._format_datetime(current_time)
                    job_data[batch_id_str] = [batch_id_str, job_id, status, formatted_time, '', workflow_stage]
        
        # Write updated data back to CSV
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header with workflow_stage column
            writer.writerow(['batch_id', 'job_id', 'status', 'submission_time', 'completion_time', 'workflow_stage'])
            # Write data
            for row in job_data.values():
                writer.writerow(row)
        
        # Verify the workflow stage was properly written - useful for debugging
        if job_id and batch_id:
            print(f"Updated job {job_id} (batch {batch_id}) with status: {status}, workflow stage: {workflow_stage}")
    
    def refresh_all_job_statuses(self):
        """
        Refresh the status of all jobs in the batch_job_map and update the CSV file.
        This is useful for periodic status updates and fixing missing workflow stage entries.
        
        Returns:
            Dict of batch_id -> status for monitoring
        """
        # Load latest batch-job mappings
        self._load_batch_job_map()
        
        # First update the CSV with fresh status information from the job scheduler
        self.update_job_status_csv()
        
        # Now fix any entries with missing workflow stage directly
        csv_file = os.path.join(self.output_path, 'job_status.csv')
        
        if os.path.exists(csv_file):
            import pandas as pd
            
            try:
                # Read the CSV into a pandas DataFrame for easier manipulation
                df = pd.read_csv(csv_file)
                
                # Check if workflow_stage column exists, add it if not
                if 'workflow_stage' not in df.columns:
                    df['workflow_stage'] = ''
                    print("Added missing workflow_stage column to job status file")
                
                # Check for and fix missing workflow stage values
                updated_rows = False
                
                for index, row in df.iterrows():
                    # Check if workflow_stage is empty or NaN
                    if pd.isna(row['workflow_stage']) or row['workflow_stage'] == '':
                        batch_id = int(row['batch_id'])
                        status = row['status']
                        
                        # Set workflow stage based on status directly
                        if status == 'COMPLETED':
                            workflow_stage = 'completed'
                        elif status == 'PENDING':
                            workflow_stage = 'pending'
                        elif status == 'FAILED':
                            workflow_stage = 'failed'
                        elif status == 'CANCELLED':
                            workflow_stage = 'cancelled'
                        elif status == 'RUNNING':
                            # For running jobs, get detailed workflow stage
                            workflow_stage = self._get_current_workflow_stage(batch_id, status)
                        else:
                            workflow_stage = status.lower()
                        
                        # Update the workflow stage in the DataFrame
                        df.at[index, 'workflow_stage'] = workflow_stage
                        updated_rows = True
                        print(f"Fixed workflow stage for batch {batch_id}: {status} → {workflow_stage}")
                
                # Write back the updated DataFrame if any rows were changed
                if updated_rows:
                    df.to_csv(csv_file, index=False)
                    print(f"Updated job status file with workflow stage information")
            
            except Exception as e:
                print(f"Error processing CSV for workflow stage updates: {e}")
        
        # Return a dict of batch_id -> status for monitoring
        statuses = {}
        if os.path.exists(csv_file):
            try:
                import csv
                with open(csv_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        batch_id = row['batch_id']
                        status = row['status']
                        statuses[batch_id] = status
            except Exception as e:
                print(f"Error reading job statuses: {e}")
        
        return statuses

    def monitor_jobs(self, update_interval: int = 60, max_updates: int = -1):
        """
        Monitor jobs and update the job_status.csv file periodically.
        
        Args:
            update_interval: Number of seconds between updates (default: 60)
            max_updates: Maximum number of updates (-1 for unlimited)
        """
        import time
        import pandas as pd
        
        updates = 0
        try:
            while max_updates == -1 or updates < max_updates:
                print(f"Updating job statuses... (update #{updates+1})")
                statuses = self.refresh_all_job_statuses()
                
                # Print current statuses with improved formatting
                csv_file = os.path.join(self.output_path, 'job_status.csv')
                if os.path.exists(csv_file):
                    try:
                        df = pd.read_csv(csv_file)
                        if not df.empty:
                            print(f"\nCurrently tracked jobs: {len(df)}")
                            
                            # Get status counts
                            status_counts = df['status'].value_counts().to_dict()
                            pending = status_counts.get('PENDING', 0)
                            running = status_counts.get('RUNNING', 0)
                            completed = status_counts.get('COMPLETED', 0)
                            failed = status_counts.get('FAILED', 0)
                            
                            print(f"  PENDING: {pending}, RUNNING: {running}, COMPLETED: {completed}, FAILED: {failed}")
                            
                            # Print details of running and pending jobs
                            active_jobs = df[df['status'].isin(['RUNNING', 'PENDING'])]
                            if not active_jobs.empty:
                                print("\nActive jobs:")
                                for _, job in active_jobs.iterrows():
                                    submission_time = job['submission_time']
                                    # Include workflow stage if available
                                    if 'workflow_stage' in df.columns and not pd.isna(job['workflow_stage']):
                                        print(f"  - Batch {job['batch_id']}: Job ID {job['job_id']} ({job['status']} - {job['workflow_stage']}, submitted: {submission_time})")
                                    else:
                                        print(f"  - Batch {job['batch_id']}: Job ID {job['job_id']} ({job['status']}, submitted: {submission_time})")
                    except Exception as e:
                        print(f"Error reading job status CSV: {e}")
                
                updates += 1
                if max_updates == -1 or updates < max_updates:
                    print(f"\nNext update in {update_interval} seconds...")
                    time.sleep(update_interval)
        except KeyboardInterrupt:
            print("\nJob monitoring stopped by user.")
            return

    def get_queue_jobs(self) -> Set[str]:
        """
        Get a set of all job IDs currently in the scheduler queue

        Returns:
            A set of job ID strings currently in the queue
        """
        queue_jobs = set()
        
        # Logic depends on scheduler type
        scheduler_type = self.config.get("scheduler", {}).get("type", "slurm").lower()
        
        try:
            if scheduler_type == "slurm":
                # Get all jobs from squeue
                result = subprocess.run(['squeue', '-h', '-o', '%i'], 
                                      stdout=subprocess.PIPE, 
                                      stderr=subprocess.PIPE,
                                      universal_newlines=True)
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if line.strip():
                            queue_jobs.add(line.strip())
                
            elif scheduler_type == "pbs" or scheduler_type == "torque":
                # Get all jobs from qstat
                result = subprocess.run(['qstat', '-f'], 
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       universal_newlines=True)
                if result.returncode == 0:
                    # Parse PBS/Torque qstat output to extract job IDs
                    job_id_pattern = r'Job Id:\s*(\d+)'
                    matches = re.finditer(job_id_pattern, result.stdout)
                    for match in matches:
                        queue_jobs.add(match.group(1))
            
            elif scheduler_type == "lsf":
                # Get all jobs from bjobs
                result = subprocess.run(['bjobs', '-noheader', '-o', 'JOBID'],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      universal_newlines=True)
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if line.strip():
                            queue_jobs.add(line.strip())
            
            else:
                print(f"Warning: Unsupported scheduler type '{scheduler_type}' for queue check")
                
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            print(f"Error checking queue: {e}")
            
        return queue_jobs

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