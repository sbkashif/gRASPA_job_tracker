import os
import subprocess
import importlib
import sys
from typing import Dict, Any, List, Optional, Callable, Union, Tuple, Set
import tempfile
from pathlib import Path

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
        self.templates = config.get('file_templates', {})
        
        # Create directories for job scripts and logs
        os.makedirs(self.scripts_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Set batch range limits if provided
        self.min_batch_id = None
        self.max_batch_id = None
        if batch_range:
            self.min_batch_id, self.max_batch_id = batch_range
            
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
                                batch_id, job_id = parts[0], parts[1]
                                self.batch_job_map[job_id] = int(batch_id)
            except Exception as e:
                print(f"Error loading batch-job mapping: {e}")
    
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
    
    def create_job_script(self, batch_id: int, batch_files: List[str]) -> Optional[str]:
        """
        Create a SLURM job script for processing a batch of CIF files
        
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
    
    def _create_default_job_script(self, batch_id: int, batch_files: List[str], batch_output_dir: str) -> str:
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
        
        # Set GRASPA environment variables using project_root from config
        script_content += f"# Set GRASPA environment variables\n"
        
        # Use project_root directly from config without recalculation
        project_root = self.config.get('project_root', '')
        
        # Always set scripts dir to the standard location
        graspa_scripts_dir = os.path.join(project_root, 'gRASPA_job_tracker', 'scripts')
        
        # Export the environment variables
        script_content += f"export GRASPA_SCRIPTS_DIR=\"{graspa_scripts_dir}\"\n"
        script_content += f"export GRASPA_ROOT=\"{project_root}\"\n\n"
        
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
                            script_content += f"export SIM_VAR_{var_key.upper()}=\"{var_value}\"\n"

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
        """Generate workflow steps with exit status logging, completion checks, and rerun capabilities."""
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
        
        # Build dependency map for sequential workflow - each step depends on the previous one
        for i in range(1, len(workflow)):
            if 'depends_on' not in workflow[i]:
                workflow[i]['depends_on'] = []
            
            # If dependencies aren't explicitly defined, assume sequential dependency
            if not workflow[i]['depends_on']:
                workflow[i]['depends_on'].append(workflow[i-1]['name'])
        
        # Track the previous step's output directory to use as input for the next step
        prev_step_output_dir = None
        
        # Add step completion check function
        steps_content += """
# Function to check if a step is already completed
check_step_completion() {
    local step_name="$1"
    local step_dir="$2"
    
    # Check if exit_status.log exists and contains a successful exit code
    if [ -f "${step_dir}/exit_status.log" ]; then
        local exit_status=$(cat "${step_dir}/exit_status.log")
        if [ "$exit_status" = "0" ]; then
            echo "✅ Step '${step_name}' was previously completed successfully. Skipping."
            return 0  # Step completed successfully
        else
            echo "⚠️  Step '${step_name}' was previously attempted but failed (exit code: ${exit_status}). Will retry."
            return 1  # Step failed
        fi
    elif [ -d "${step_dir}" ] && [ "$(ls -A ${step_dir} 2>/dev/null)" ]; then
        # Directory exists with content but no status file - likely interrupted
        echo "🔄 Step '${step_name}' appears to have been interrupted. Will retry."
        return 2  # Step incomplete
    fi
    
    # No completion status found, directory empty or doesn't exist
    echo "🆕 Starting step '${step_name}' for the first time."
    return 3  # Step not started
}

# Function to check if dependencies are satisfied
check_dependencies() {
    local step_name="$1"
    local dependencies=("${@:2}")
    
    # If no dependencies, return success
    if [ ${#dependencies[@]} -eq 0 ]; then
        return 0
    fi
    
    echo "🔍 Checking dependencies for step '${step_name}': ${dependencies[*]}"
    
    # Check each dependency
    for dep in "${dependencies[@]}"; do
        local dep_dir="${output_dir}/${dep}"
        
        # Check if this dependency is explicitly defined in our workflow
        if ! [[ "${workflow_steps_str}" =~ "${dep}" ]]; then
            echo "⚠️  Dependency '${dep}' is not defined in the workflow - potential configuration issue"
            # We'll treat this as a warning, not an error, since it might be an optional external dependency
            continue
        fi
        
        # Check if directory exists
        if [ ! -d "${dep_dir}" ]; then
            echo "❌ Dependency directory '${dep}' does not exist - required workflow step is missing"
            return 1
        fi
        
        # Check if dependency status file exists and contains successful exit code
        local dep_status_file="${dep_dir}/exit_status.log"
        if [ ! -f "${dep_status_file}" ]; then
            echo "❌ Dependency '${dep}' for step '${step_name}' has not been completed"
            return 1
        fi
        
        local dep_status=$(cat "${dep_status_file}")
        if [ "${dep_status}" != "0" ]; then
            echo "❌ Dependency '${dep}' for step '${step_name}' failed with status ${dep_status}"
            return 1
        fi
    done
    
    echo "✅ All dependencies satisfied for step '${step_name}'"
    return 0
}

# Function to log step status with timestamp
log_step_status() {
    local step_name="$1"
    local status="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${timestamp}] ${status}: ${step_name}"
}

"""
        
        # Create an array of step names for dependency checking
        step_names = [step.get('name') for step in workflow]
        steps_content += f"# All workflow steps for dependency checking\n"
        steps_content += f"workflow_steps_str=\"{' '.join(step_names)}\"\n\n"
        
        # Create a dictionary for step name to directory lookup
        steps_content += "# Create a mapping of step names to directories\n"
        steps_content += "declare -A step_directories\n"
        for step in workflow:
            step_name = step.get('name')
            output_subdir = step.get('output_subdir', step_name)
            steps_content += f'step_directories["{step_name}"]="${output_dir}/{output_subdir}"\n'
        
        steps_content += "\n"
        
        # Process each workflow step in sequence
        for i, step in enumerate(workflow):
            step_name = step.get('name', f'step_{i+1}')
            script_path = step.get('script', self.scripts.get(step_name, ''))
            output_subdir = step.get('output_subdir', step_name)
            required = step.get('required', True)
            dependencies = step.get('depends_on', [])
            
            # Skip if no script is defined for this step
            if not script_path:
                continue
                
            step_output_dir = os.path.join(output_dir, output_subdir)
            
            # For the first step, use the original file_list as input
            # For subsequent steps, use the previous step's output directory
            step_input = file_list if prev_step_output_dir is None else prev_step_output_dir
            
            # Add step header
            steps_content += f"\necho '====== Step {i+1}: {step_name.replace('_', ' ').title()} ======'\n"
            steps_content += f"mkdir -p {step_output_dir}\n"
            
            # Convert dependencies to an array for bash
            deps_array = "("
            for dep in dependencies:
                deps_array += f'"{dep}" '
            deps_array += ")"
            
            # Add dependency and step completion checks
            steps_content += f"""
# Check dependencies for {step_name}
check_dependencies "{step_name}" {deps_array}
dependency_check=$?

if [ $dependency_check -ne 0 ]; then
    if [ "{str(required).lower()}" = "true" ]; then
        log_step_status "{step_name}" "SKIPPED_DEPENDENCY_FAILED"
        echo "⛔ Required dependencies for step '{step_name}' have failed. Skipping batch {batch_id}."
        echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}
        exit 1
    else
        log_step_status "{step_name}" "SKIPPED_OPTIONAL"
        echo "⏭️  Skipping optional step '{step_name}' due to failed dependencies."
        continue
    fi
fi

# Check if this step is already completed
check_step_completion "{step_name}" "{step_output_dir}"
step_check_status=$?

case $step_check_status in
    0)
        # Step already completed successfully, set status to 0 and continue to next step
        {step_name.lower().replace('-', '_')}_status=0
        ;;
    1)
        # Step previously failed, decide whether to retry
        if [ "{str(required).lower()}" = "true" ]; then
            log_step_status "{step_name}" "RETRYING_FAILED_STEP"
            echo "🔁 Rerunning previously failed step '{step_name}' (required for workflow)"
            
            # Backup previous output with timestamp
            if [ -d "{step_output_dir}" ] && [ "$(ls -A {step_output_dir} 2>/dev/null)" ]; then
                backup_dir="{step_output_dir}_failed_$(date '+%Y%m%d_%H%M%S')"
                echo "📦 Backing up previous attempt to $backup_dir"
                mv {step_output_dir} $backup_dir
                mkdir -p {step_output_dir}
            fi
"""
            
            # Special case: Always treat mps_run as a bash script regardless of extension
            is_bash_script = (script_path.endswith(('.sh', '.bash')) or 
                             'mps_run' in script_path or 
                             step_name == 'simulation')
            
            if is_bash_script:
                steps_content += self._generate_bash_step(
                    script_path=script_path,
                    step_name=step_name,
                    batch_id=batch_id,
                    input_file=step_input,  # Use the appropriate input
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)  # Indicate if this is the first step
                )
            else:
                # Python module or script - use directly
                steps_content += self._generate_python_step(
                    script_path=script_path,
                    step_name=step_name,
                    batch_id=batch_id,
                    input_file=step_input,  # Use the appropriate input
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)  # Indicate if this is the first step
                )
            
            # Add status check
            step_var_name = f"{step_name.lower().replace('-', '_')}_status"
            
            # Continue the case statement
            steps_content += f"""
                {step_var_name}=$?
                
                # Log exit status
                echo ${step_var_name} > {os.path.join(step_output_dir, 'exit_status.log')}
                
                if [ ${step_var_name} -ne 0 ]; then
                    log_step_status "{step_name}" "RETRY_FAILED"
                    echo "❌ Retry of step '{step_name}' failed again with exit code ${step_var_name}"
                    echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}
                    exit 1
                else
                    log_step_status "{step_name}" "RETRY_SUCCEEDED"
                    echo "✅ Retry of step '{step_name}' succeeded"
                fi
            else
                # Optional step that previously failed, skip it
                log_step_status "{step_name}" "SKIPPED_OPTIONAL_FAILED"
                echo "⏭️  Skipping optional step '{step_name}' that previously failed"
                {step_var_name}=0  # Consider it "passed" for workflow purposes
            fi
            ;;
        2|3)
            # Step was interrupted or not started, run it
            log_step_status "{step_name}" "STARTING"
            echo "🚀 Running step '{step_name}'"
"""
            
            # Add the execution for cases 2 and 3 (interrupted or not started)
            if is_bash_script:
                steps_content += self._generate_bash_step(
                    script_path=script_path,
                    step_name=step_name,
                    batch_id=batch_id,
                    input_file=step_input,
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)
                )
            else:
                steps_content += self._generate_python_step(
                    script_path=script_path,
                    step_name=step_name,
                    batch_id=batch_id,
                    input_file=step_input,
                    output_dir=step_output_dir,
                    step=step,
                    is_first_step=(prev_step_output_dir is None)
                )
                
            steps_content += f"""
            {step_var_name}=$?
            
            # Log exit status
            echo ${step_var_name} > {os.path.join(step_output_dir, 'exit_status.log')}
            
            if [ ${step_var_name} -ne 0 ]; then
                log_step_status "{step_name}" "FAILED"
                echo "❌ Step '{step_name}' failed with exit code ${step_var_name}"
                
                if [ "{str(required).lower()}" = "true" ]; then
                    echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}
                    exit 1
                else
                    log_step_status "{step_name}" "FAILED_BUT_OPTIONAL"
                    echo "⚠️  Step '{step_name}' failed but is optional, continuing workflow"
                fi
            else
                log_step_status "{step_name}" "SUCCESS"
                echo "✅ Step '{step_name}' completed successfully"
            fi
            ;;
    esac

"""
            
            # Ensure the script does not exit prematurely after the simulation step
            if step_name == 'simulation':
                steps_content += f"# Ensure transition to the next step after simulation\n"
                steps_content += f"echo '🔄 Simulation step handled. Proceeding to next step...'\n\n"
            
            # Update the previous step output dir for the next iteration
            prev_step_output_dir = step_output_dir

        # Add final success indication to exit_status.log in the main output directory
        steps_content += f"\n# Write final success status to main exit_status.log\n"
        steps_content += f"echo 0 > {os.path.join(output_dir, 'exit_status.log')}\n"
        steps_content += f"echo '🎉 All required steps completed successfully for batch {batch_id}'\n"
        
        return steps_content
    
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
    
    def submit_job(self, script_path: str, dry_run: bool = False, batch_id: Optional[int] = None) -> Optional[str]:
        """
        Submit a job to SLURM or perform a dry run
        
        Args:
            script_path: Path to the job script
            dry_run: If True, only print the job script and don't submit
            batch_id: The batch ID associated with this job
            
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
            self.print_job_script(script_path)
            print(f"[DRY RUN] To submit manually: sbatch {script_path}")
            return "dry-run"
        
        try:
            print(f"Submitting job script: {script_path}")
            result = subprocess.run(['sbatch', script_path], 
                                   check=True, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
            # Extract job ID from sbatch output (usually something like "Submitted batch job 123456")
            output = result.stdout.strip()
            if "Submitted batch job" in output:
                job_id = output.split()[-1]
                print(f"Job submitted successfully with ID: {job_id}")
                
                # Store batch_id to job_id mapping if batch_id is provided
                if batch_id is not None:
                    self.batch_job_map[job_id] = batch_id
                    self._save_batch_job_map()
                    
                    # Update the job status CSV file with the new job
                    self.update_job_status_csv(job_id=job_id, batch_id=batch_id)
                    
                return job_id        
            
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

    def update_job_status_csv(self, job_id: str = None, batch_id: int = None):
        """
        Update the job_status.csv file with current job statuses.
        If job_id and batch_id are provided, update only that job.
        Otherwise, update all jobs in the batch_job_map.

        Args:
            job_id: Specific job ID to update (optional)
            batch_id: Specific batch ID to update (optional)
        """
        import csv
        import time
        
        csv_file = os.path.join(self.output_path, 'job_status.csv')
        
        # Read existing data if file exists
        job_data = {}
        if os.path.exists(csv_file):
            try:
                with open(csv_file, 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader, None)  # Skip header if it exists
                    for row in reader:
                        if len(row) >= 3:
                            batch_id_csv = row[0]
                            job_data[batch_id_csv] = row
            except Exception as e:
                print(f"Warning: Error reading job status CSV: {e}")
        
        # Update specific job if provided
        if job_id and batch_id:
            batch_id_str = str(batch_id)
            status = self.get_job_status(job_id)
            
            # Get batch output directory to check for exit_status.log
            batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
            if os.path.exists(batch_output_dir):
                status = self.get_job_status(job_id, batch_output_dir)
            
            # Update or add entry
            if batch_id_str in job_data:
                job_data[batch_id_str][1] = job_id
                job_data[batch_id_str][2] = status
                # Update completion time if job is completed
                if status in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'UNKNOWN'] and not job_data[batch_id_str][4]:
                    job_data[batch_id_str][4] = self._format_datetime(time.time())
            else:
                # Format the submission time as a readable date-time string
                current_time = time.time()
                formatted_time = self._format_datetime(current_time)
                job_data[batch_id_str] = [batch_id_str, job_id, status, formatted_time, '']
        else:
            # Update all jobs in the batch_job_map
            for job_id, batch_id in self.batch_job_map.items():
                batch_id_str = str(batch_id)
                
                # Get batch output directory to check for exit_status.log
                batch_output_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
                status = self.get_job_status(job_id, batch_output_dir if os.path.exists(batch_output_dir) else None)
                
                # Update or add entry
                if batch_id_str in job_data:
                    job_data[batch_id_str][1] = job_id
                    job_data[batch_id_str][2] = status
                    # Update completion time if job is completed and no completion time is set
                    if status in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'UNKNOWN'] and not job_data[batch_id_str][4]:
                        job_data[batch_id_str][4] = self._format_datetime(time.time())
                else:
                    # Format the submission time as a readable date-time string
                    current_time = time.time()
                    formatted_time = self._format_datetime(current_time)
                    job_data[batch_id_str] = [batch_id_str, job_id, status, formatted_time, '']
        
        # Write updated data back to CSV
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['batch_id', 'job_id', 'status', 'submission_time', 'completion_time'])
            # Write data
            for row in job_data.values():
                writer.writerow(row)
    
    def refresh_all_job_statuses(self):
        """
        Refresh the status of all jobs in the batch_job_map and update the CSV file.
        This is useful for periodic status updates.
        """
        # Load latest batch-job mappings
        self._load_batch_job_map()
        
        # Update the CSV with fresh status information
        self.update_job_status_csv()
        
        # Return a dict of batch_id -> status for monitoring
        statuses = {}
        csv_file = os.path.join(self.output_path, 'job_status.csv')
        
        if os.path.exists(csv_file):
            import csv
            with open(csv_file, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 3:
                        batch_id = row[0]
                        status = row[2]
                        statuses[batch_id] = status
        
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
