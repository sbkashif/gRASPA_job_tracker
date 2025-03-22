import os
import subprocess
from typing import Dict, Any, List, Optional

class JobScheduler:
    """Handle SLURM job submission and management"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the job scheduler
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.slurm_config = config['slurm']
        self.output_path = config['output_path']
        self.partial_charge_script = config['partial_charge_script']
        self.simulation_script = config['simulation_script']
        self.simulation_input_file = config['simulation_input_file']
        
        # Create directories for job scripts and logs
        self.scripts_dir = os.path.join(self.output_path, 'job_scripts')
        self.logs_dir = os.path.join(self.output_path, 'job_logs')
        
        os.makedirs(self.scripts_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
    
    def create_job_script(self, batch_id: int, batch_files: List[str]) -> str:
        """
        Create a SLURM job script for processing a batch of CIF files
        
        Args:
            batch_id: ID of the batch
            batch_files: List of CIF file paths in the batch
            
        Returns:
            Path to the created job script
        """
        script_path = os.path.join(self.scripts_dir, f'job_batch_{batch_id}.sh')
        
        # Create batch-specific output directory
        batch_output_dir = os.path.join(self.output_path, f'batch_{batch_id}')
        os.makedirs(batch_output_dir, exist_ok=True)
        
        # Create script content
        script_content = "#!/bin/bash\n\n"
        
        # Add SLURM directives
        script_content += f"#SBATCH --job-name=graspa_batch_{batch_id}\n"
        script_content += f"#SBATCH --output={os.path.join(self.logs_dir, f'job_batch_{batch_id}_%j.out')}\n"
        script_content += f"#SBATCH --error={os.path.join(self.logs_dir, f'job_batch_{batch_id}_%j.err')}\n"
        
        for key, value in self.slurm_config.items():
            script_content += f"#SBATCH --{key}={value}\n"
        
        script_content += "\n"
        
        # Add environment setup if provided
        if 'environment_setup' in self.config:
            script_content += f"{self.config['environment_setup']}\n\n"
        
        # Create a file with the list of CIF files for this batch
        batch_list_file = os.path.join(batch_output_dir, "cif_file_list.txt")
        with open(batch_list_file, 'w') as f:
            for cif_file in batch_files:
                f.write(f"{cif_file}\n")
        
        # Add commands to process the batch
        script_content += f"echo 'Starting job for batch {batch_id}'\n\n"
        
        # Add command to generate partial charges using the provided Python script
        partial_charge_output_dir = os.path.join(batch_output_dir, "partial_charges")
        os.makedirs(partial_charge_output_dir, exist_ok=True)
        
        script_content += f"echo 'Step 1: Generating partial charges'\n"
        script_content += f"python {self.partial_charge_script} --input {batch_list_file} --output {partial_charge_output_dir}\n"
        script_content += f"partial_charge_status=$?\n\n"
        
        # Check if partial charge generation was successful
        script_content += "if [ $partial_charge_status -ne 0 ]; then\n"
        script_content += "    echo 'Partial charge generation failed with status $partial_charge_status'\n"
        script_content += f"    echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}\n"
        script_content += "    exit 1\n"
        script_content += "fi\n\n"
        
        # Add command to run GRASPA simulations using the bash script
        simulation_output_dir = os.path.join(batch_output_dir, "simulations")
        os.makedirs(simulation_output_dir, exist_ok=True)
        
        script_content += f"echo 'Step 2: Running GRASPA simulations'\n"
        script_content += f"bash {self.simulation_script} {partial_charge_output_dir} {self.simulation_input_file} {simulation_output_dir}\n"
        script_content += f"simulation_status=$?\n\n"
        
        # Check if simulation was successful
        script_content += "if [ $simulation_status -ne 0 ]; then\n"
        script_content += "    echo 'Simulation failed with status $simulation_status'\n"
        script_content += f"    echo '{batch_id}' >> {os.path.join(self.output_path, 'failed_batches.txt')}\n"
        script_content += "    exit 1\n"
        script_content += "fi\n\n"
        
        # Write completion status
        script_content += f"echo 0 > {os.path.join(batch_output_dir, 'exit_status.log')}\n"
        script_content += "echo 'Job completed successfully'\n"
        
        # Write script to file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        return script_path
    
    def submit_job(self, script_path: str) -> Optional[str]:
        """
        Submit a job to SLURM
        
        Args:
            script_path: Path to the job script
            
        Returns:
            Job ID if submission was successful, None otherwise
        """
        try:
            result = subprocess.run(['sbatch', script_path], 
                                   check=True, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
            
            # Extract job ID from sbatch output (usually something like "Submitted batch job 123456")
            output = result.stdout.strip()
            if "Submitted batch job" in output:
                job_id = output.split()[-1]
                return job_id
            
            return None
            
        except subprocess.CalledProcessError as e:
            print(f"Failed to submit job: {e}")
            print(f"stderr: {e.stderr}")
            return None
    
    def get_job_status(self, job_id: str) -> str:
        """
        Get the status of a SLURM job
        
        Args:
            job_id: SLURM job ID
            
        Returns:
            Job status (PENDING, RUNNING, COMPLETED, FAILED, etc.) or 'UNKNOWN' if job not found
        """
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
                    ['sacct', '--job', job_id, '--format=State', '--noheader', '--parsable2'],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                if sacct_result.stdout.strip():
                    return sacct_result.stdout.strip().split('\n')[0]
                
            return 'UNKNOWN'
            
        except subprocess.CalledProcessError:
            return 'UNKNOWN'
