import os
import time
import subprocess
import requests
import shutil
from urllib.parse import urlparse
from tqdm import tqdm
from typing import Dict, List, Any, Set, Optional, Union, Tuple
import pandas as pd

from .batch_manager import BatchManager
from .job_scheduler import JobScheduler

class JobTracker:
    """Track job progress and manage job submission"""
    
    def __init__(self, config: Dict[str, Any], batch_range: Optional[Tuple[int, int]] = None):
        """
        Initialize the job tracker
        
        Args:
            config: Configuration dictionary
            batch_range: Optional tuple of (min_batch_id, max_batch_id) to limit which batches are processed
        """
        self.config = config
        
        # Store batch range
        self.batch_range = batch_range
        
        # Use output structure from config
        self.output_path = config['output']['output_dir']
        self.results_dir = config['output']['results_dir']
        
        self.max_concurrent_jobs = config['batch'].get('max_concurrent_jobs', 1)
        
        # Control whether failed jobs should be resubmitted
        self.resubmit_failed = config['batch'].get('resubmit_failed', False)
        
        # Initialize batch manager and job scheduler
        self.batch_manager = BatchManager(config)
        self.job_scheduler = JobScheduler(config, batch_range=batch_range)
        
        # Create tracking files and directories
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)
        
        self.job_status_file = os.path.join(self.output_path, 'job_status.csv')
        self.failed_batches_file = os.path.join(self.output_path, 'failed_batches.txt')
        
        # Initialize or load job status tracking
        self._initialize_job_status()
    
    def _format_timestamp(self) -> pd.Timestamp:
        """
        Create a timestamp without fractional seconds
        
        Returns:
            A pandas Timestamp object with second precision
        """
        # Get current time
        current_time = pd.Timestamp(time.time(), unit='s')
        # Format to string without microseconds and convert back to timestamp
        time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        return pd.Timestamp(time_str)
    
    def _initialize_job_status(self):
        """Initialize or load job status tracking"""
        if os.path.exists(self.job_status_file):
            self.job_status = pd.read_csv(self.job_status_file,
                                          parse_dates=['submission_time', 'completion_time'])
        else:
            # Get base columns (workflow_stage will be added at the end)
            base_columns = ['batch_id', 'job_id']
            
            # Add parameter combination column if parameter matrix is enabled (as 3rd column)
            if self.job_scheduler.parameter_matrix.is_enabled():
                base_columns.append('param_combination_id')
            
            # Add remaining columns
            base_columns.extend(['status', 'submission_time', 'completion_time'])
            
            # Add workflow_stage at the end
            base_columns.append('workflow_stage')
            
            self.job_status = pd.DataFrame(columns=base_columns)
            self.job_status.to_csv(self.job_status_file, index=False)
            
            # Set basic dtypes
            dtype_dict = {
                'batch_id': int,
                'job_id': int,
                'status': str,
                'submission_time': 'datetime64[ns]',
                'completion_time': 'datetime64[ns]',
                'workflow_stage': str
            }
            
            # Add parameter combination column dtype if parameter matrix is enabled
            if self.job_scheduler.parameter_matrix.is_enabled():
                dtype_dict['param_combination_id'] = str
            
            self.job_status = self.job_status.astype(dtype_dict)
        # Initialize failed batches list
        if os.path.exists(self.failed_batches_file):
            with open(self.failed_batches_file, 'r') as f:
                self.failed_batches = set(int(line.strip()) for line in f if line.strip())
        else:
            self.failed_batches = set()
            with open(self.failed_batches_file, 'w') as f:
                pass
    
    def _save_job_status_basic(self):
        """Save current job status to file"""
        self.job_status.to_csv(self.job_status_file, index=False)
        
    def _save_job_status(self, retries=3):
        """Save current job status to file with file locking to prevent conflicts"""
        import fcntl
        
        # Create a lock file
        lock_file = f"{self.job_status_file}.lock"
        
        try:
            with open(lock_file, 'w') as lock:
                # Non-blocking exclusive lock
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Save the DataFrame to CSV
                self.job_status.to_csv(self.job_status_file, index=False)
                
                # Release the lock
                fcntl.flock(lock, fcntl.LOCK_UN)
        except IOError:
            # Another process is updating the file, wait a bit and try again
            if retries > 0:
                time.sleep(0.5)
                self._save_job_status(retries-1)  # Recursive retry with decremented counter
            else:
                print("Failed to acquire lock for job status file. Skipping save.")
        finally:
            # Remove the lock file if it exists
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
    
    def _save_failed_batches(self):
        """Save failed batches to file"""
        with open(self.failed_batches_file, 'w') as f:
            for batch_id in self.failed_batches:
                f.write(f"{batch_id}\n")
    
    # Add a method to set resubmission behavior
    def set_resubmit_failed(self, resubmit: bool):
        """
        Set whether failed jobs should be resubmitted
        
        Args:
            resubmit: If True, failed jobs will be resubmitted
        """
        self.resubmit_failed = resubmit
        print(f"Resubmit failed jobs set to: {'Yes' if self.resubmit_failed else 'No'}")
    
    def _get_running_jobs(self) -> Set[str]:
        """Get the set of currently running or pending job IDs and update status of completed jobs"""
        running_jobs = set()
        status_changes = False
        
        # Get all jobs currently in the scheduler queue (from system)
        queue_jobs = self.job_scheduler.get_queue_jobs()
        
        # Filter for jobs with status 'RUNNING' or 'PENDING'
        active_jobs = self.job_status[self.job_status['status'].isin(['RUNNING', 'PENDING'])]
        
        for _, job in active_jobs.iterrows():
            job_id = str(job['job_id'])  # Ensure job_id is a string
            batch_id = job['batch_id']   # Get batch_id directly from the current job row
            current_status = job['status']  # Get current status to detect changes
            
            # Skip "dry-run" job IDs
            if job_id == "dry-run":
                mask = self.job_status['job_id'] == "dry-run"
            else:
                mask = self.job_status['job_id'] == int(job_id)
                
            # Get the batch_output_dir for this job
            batch_output_dir = os.path.join(self.results_dir, f'batch_{batch_id}')
            
            # Check if job is still in the queue
            if job_id != "dry-run" and job_id not in queue_jobs:
                # Job is no longer in queue, check exit status file to determine if it succeeded
                exit_status_file = os.path.join(batch_output_dir, 'exit_status.log')
                
                if os.path.exists(exit_status_file):
                    with open(exit_status_file, 'r') as f:
                        exit_status = f.read().strip()
                        if exit_status == '0':
                            new_status = 'COMPLETED'
                            print(f"✅ Job {job_id} for batch {batch_id} completed successfully")
                        else:
                            # Check if partial completion - look for completed steps
                            new_status = self._check_partial_completion(batch_id, batch_output_dir)
                            if new_status == 'PARTIALLY_COMPLETE':
                                print(f"⚠️ Job {job_id} for batch {batch_id} partially completed but failed at some step")
                                # Don't add to failed_batches since it's partially complete
                            else:
                                new_status = 'FAILED'
                                self.failed_batches.add(int(batch_id))
                                print(f"❌ Job {job_id} for batch {batch_id} failed with exit status {exit_status}")
                else:
                    # No exit status file but job is not in queue - check for partial completion
                    new_status = self._check_partial_completion(batch_id, batch_output_dir)
                    if new_status == 'PARTIALLY_COMPLETE':
                        print(f"⚠️ Job {job_id} for batch {batch_id} partially completed but failed at some step")
                        # Don't add to failed_batches since it's partially complete
                    else:
                        new_status = 'FAILED'
                        self.failed_batches.add(int(batch_id))
                        print(f"❌ Job {job_id} for batch {batch_id} is not in queue and no exit status file found")
            else:
                # Job might still be in queue, use scheduler's status check
                new_status = self.job_scheduler.get_job_status(job_id, batch_output_dir)
            
            # Update status if changed
            if any(mask):  # Verify there are rows that match this job_id
                if current_status != new_status:  # Status has changed
                    self.job_status.loc[mask, 'status'] = new_status
                    
                    # Explicitly highlight status transitions with better messages
                    if current_status == 'PENDING' and new_status == 'RUNNING':
                        print(f"📊 Job {job_id} for batch {batch_id} is now running")
                    elif new_status == 'COMPLETED':
                        print(f"✅ Job {job_id} for batch {batch_id} completed successfully")
                    elif new_status == 'PARTIALLY_COMPLETE':
                        print(f"⚠️ Job {job_id} for batch {batch_id} partially completed")
                    elif new_status == 'FAILED':
                        print(f"❌ Job {job_id} for batch {batch_id} failed")
                    else:
                        print(f"Job {job_id} status changed: {current_status} → {new_status}")
                    
                    # Track that we had status changes to ensure we save the file
                    status_changes = True
                else:
                    # Status hasn't changed, just log current status (less verbose)
                    if current_status == 'RUNNING':
                        print(f"Job {job_id} for batch {batch_id}: {new_status}")
                    else:
                        print(f"Job {job_id} status: {new_status}")
                    
                # Update workflow stage information for all jobs
                if new_status in ['RUNNING', 'PENDING', 'COMPLETED', 'FAILED', 'PARTIALLY_COMPLETE']:
                    # Get the current workflow stage using the job scheduler's method
                    workflow_stage = self.job_scheduler._get_current_workflow_stage(batch_id, new_status)
                    
                    # Only update if it's different from current value
                    current_workflow_stage = self.job_status.loc[mask, 'workflow_stage'].iloc[0] if not pd.isna(self.job_status.loc[mask, 'workflow_stage'].iloc[0]) else ""
                    if workflow_stage != current_workflow_stage:
                        self.job_status.loc[mask, 'workflow_stage'] = workflow_stage
                        print(f"Updated workflow stage for batch {batch_id}: {current_workflow_stage} → {workflow_stage}")
                        status_changes = True
            
            # Add to running jobs set if still active
            if new_status in ['RUNNING', 'PENDING']:
                running_jobs.add(job_id)
            else:
                # Job is no longer running, update completion time    
                if new_status in ['COMPLETED', 'CANCELLED', 'FAILED', 'TIMEOUT', 'UNKNOWN', 'PARTIALLY_COMPLETE']:
                    # Use formatted timestamp without fractional seconds
                    completion_time = self._format_timestamp()
                    self.job_status.loc[mask, 'completion_time'] = completion_time
                    status_changes = True
                    
                    # Double-check exit status file to verify job completion status
                    exit_status_file = os.path.join(self.results_dir, f'batch_{batch_id}', 'exit_status.log')
                    
                    if os.path.exists(exit_status_file):
                        with open(exit_status_file, 'r') as f:
                            exit_status = f.read().strip()
                            if exit_status != '0':
                                # Only add to failed batches if not partially complete
                                if new_status != 'PARTIALLY_COMPLETE':
                                    self.failed_batches.add(int(batch_id))
                                    if new_status != 'FAILED':
                                        print(f"⚠️ Updating status: Batch {batch_id} failed with exit code {exit_status}")
                                        self.job_status.loc[mask, 'status'] = 'FAILED'
                    elif new_status not in ['CANCELLED', 'PARTIALLY_COMPLETE']:
                        # No exit status file and not cancelled means the job failed
                        # Only add to failed batches if not partially complete
                        if new_status != 'PARTIALLY_COMPLETE':
                            self.failed_batches.add(int(batch_id))
                            if new_status != 'FAILED':
                                print(f"⚠️ Updating status: Batch {batch_id} failed - no exit status file")
                                self.job_status.loc[mask, 'status'] = 'FAILED'
                else:
                    print(f"⚠️ Unknown status for job {job_id}: {new_status}")
                    # Use formatted timestamp without fractional seconds
                    completion_time = self._format_timestamp()
                    self.job_status.loc[mask, 'completion_time'] = completion_time
                    status_changes = True
                    
        # Final save of job status after processing all jobs
        if status_changes:
            print("Job status changes detected - updating CSV file")
            self._save_job_status()
            self._save_failed_batches()
        
        # Update parameter combination status if parameter matrix is enabled
        if self.job_scheduler.parameter_matrix.is_enabled():
            self._update_parameter_combination_status()
        
        return running_jobs
    
    def _check_partial_completion(self, batch_id: int, batch_output_dir: str) -> str:
        """
        Check if a job was partially completed by looking for exit status log files
        in each workflow step directory.
        
        Args:
            batch_id: The batch ID to check
            batch_output_dir: Path to the batch output directory
            
        Returns:
            'PARTIALLY_COMPLETE' if partial completion detected, 'FAILED' otherwise
        """
        # Get workflow steps from the config
        workflow_steps = []
        
        # Try to extract workflow stages from config structure
        if 'workflow' in self.config and self.config['workflow']:
            # Extract from explicit workflow definition
            workflow_steps = [step.get('name', f'step_{i+1}') for i, step in enumerate(self.config['workflow'])]
        elif 'scripts' in self.config and self.config['scripts']:
            # Extract from scripts section - this is the most common case
            workflow_steps = list(self.config['scripts'].keys())
        
        # If no workflow steps found, we can't check for partial completion
        if not workflow_steps:
            return 'FAILED'
        
        # Check for completed steps by looking for exit_status.log files with "0" value
        completed_steps = []
        failed_steps = []
        
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
                        else:
                            # Step failed
                            failed_steps.append(step)
                except:
                    # If we can't read the file, consider the step failed
                    failed_steps.append(step)
            else:
                # Directory exists but no exit status file - step likely started but didn't complete
                failed_steps.append(step)
        
        # If any steps completed successfully but not all, consider it partially complete
        if completed_steps and len(completed_steps) < len(workflow_steps):
            # Calculate completion percentage for better reporting
            completion_percentage = (len(completed_steps) / len(workflow_steps)) * 100
            print(f"Batch {batch_id} partially completed {len(completed_steps)}/{len(workflow_steps)} steps ({completion_percentage:.1f}%): {', '.join(completed_steps)}")
            return 'PARTIALLY_COMPLETE'
        # If all steps completed, this shouldn't happen as the main exit status would be successful
        elif len(completed_steps) == len(workflow_steps):
            print(f"Batch {batch_id} appears to have all steps completed - should be marked as COMPLETED")
            return 'COMPLETED'
        # If no steps completed successfully
        else:
            if failed_steps:
                print(f"Batch {batch_id} failed in steps: {', '.join(failed_steps)}")
            return 'FAILED'
    
    def _update_parameter_combination_status(self):
        """
        Update the status of individual parameter combinations by checking their output directories
        """
        status_changes = False
        
        # Get all parameter combination entries (those with param_combination_id)
        if not self.job_scheduler.parameter_matrix.is_enabled():
            return
            
        param_entries = self.job_status[self.job_status['param_combination_id'].notna()]
        
        if param_entries.empty:
            return
        
        for _, param_entry in param_entries.iterrows():
            batch_id = param_entry['batch_id']
            current_status = param_entry['status']
            param_combo_id = param_entry['param_combination_id']
            
            # Skip if already completed or failed
            if current_status in ['COMPLETED', 'FAILED']:
                continue
            
            # Extract parameter ID from param_combination_id (format: B{batch_id}_{param_name})
            # For example: B1_T298_P100000_CO20.15_N20.85
            param_name = param_combo_id.split('_', 1)[1]  # Remove B{batch_id}_ prefix
            
            # Find matching parameter combination by name
            param_combinations = self.job_scheduler.parameter_matrix.get_parameter_combinations()
            param_combo = None
            for combo in param_combinations:
                if combo['name'] == param_name:
                    param_combo = combo
                    break
            
            if not param_combo:
                continue
            
            param_id = param_combo['param_id']
            
            # Get the output directory for this parameter combination
            sub_job_output_dir = self.job_scheduler.parameter_matrix.get_sub_job_output_dir(batch_id, param_id)
            
            if not os.path.exists(sub_job_output_dir):
                continue
            
            # Check exit status file
            exit_status_file = os.path.join(sub_job_output_dir, 'exit_status.log')
            new_status = current_status
            workflow_stage = 'unknown'
            
            if os.path.exists(exit_status_file):
                try:
                    with open(exit_status_file, 'r') as f:
                        exit_status = f.read().strip()
                        if exit_status == '0':
                            new_status = 'COMPLETED'
                            workflow_stage = 'completed'
                        else:
                            # Check for partial completion
                            new_status = self._check_parameter_partial_completion(batch_id, param_id, sub_job_output_dir)
                            if new_status == 'PARTIALLY_COMPLETE':
                                workflow_stage = 'partially_complete'
                            else:
                                new_status = 'FAILED'
                                workflow_stage = 'failed'
                except:
                    new_status = 'FAILED'
                    workflow_stage = 'failed'
            else:
                # Check if job is still running by checking SLURM queue
                job_id = str(param_entry['job_id'])
                if job_id != "dry-run" and not job_id.startswith("dry-run-"):
                    queue_jobs = self.job_scheduler.get_queue_jobs()
                    if job_id in queue_jobs:
                        new_status = 'RUNNING'
                        workflow_stage = 'running'
                    else:
                        new_status = 'FAILED'
                        workflow_stage = 'failed'
            
            # Update status if changed
            if new_status != current_status:
                # Create mask to find this specific parameter combination
                mask = (self.job_status['batch_id'] == batch_id) & (self.job_status['job_id'] == param_entry['job_id'])
                
                self.job_status.loc[mask, 'status'] = new_status
                self.job_status.loc[mask, 'workflow_stage'] = workflow_stage
                
                if new_status in ['COMPLETED', 'FAILED', 'PARTIALLY_COMPLETE']:
                    completion_time = self._format_timestamp()
                    self.job_status.loc[mask, 'completion_time'] = completion_time
                
                param_name = param_combo['name']
                print(f"Parameter combination {param_name} (batch {batch_id}): {current_status} → {new_status}")
                status_changes = True
        
        # Save if any changes were made
        if status_changes:
            self._save_job_status()
    
    def _check_parameter_partial_completion(self, batch_id: int, param_id: int, sub_job_output_dir: str) -> str:
        """
        Check if a parameter combination job was partially completed
        
        Args:
            batch_id: The batch ID
            param_id: The parameter combination ID
            sub_job_output_dir: Path to the parameter combination output directory
            
        Returns:
            'PARTIALLY_COMPLETE' if partial completion detected, 'FAILED' otherwise
        """
        # Get workflow steps from the config
        workflow_steps = []
        
        if 'workflow' in self.config and self.config['workflow']:
            workflow_steps = [step.get('name', f'step_{i+1}') for i, step in enumerate(self.config['workflow'])]
        elif 'scripts' in self.config and self.config['scripts']:
            workflow_steps = list(self.config['scripts'].keys())
        
        if not workflow_steps:
            return 'FAILED'
        
        # Check for completed steps
        completed_steps = []
        failed_steps = []
        
        for step in workflow_steps:
            step_dir = os.path.join(sub_job_output_dir, step)
            exit_status_file = os.path.join(step_dir, 'exit_status.log')
            
            if not os.path.exists(step_dir):
                continue
                
            if os.path.exists(exit_status_file):
                try:
                    with open(exit_status_file, 'r') as f:
                        exit_status = f.read().strip()
                        if exit_status == '0':
                            completed_steps.append(step)
                        else:
                            failed_steps.append(step)
                except:
                    failed_steps.append(step)
            else:
                failed_steps.append(step)
        
        # If any steps completed successfully but not all, consider it partially complete
        if completed_steps and len(completed_steps) < len(workflow_steps):
            completion_percentage = (len(completed_steps) / len(workflow_steps)) * 100
            print(f"Parameter combination {param_id} partially completed {len(completed_steps)}/{len(workflow_steps)} steps ({completion_percentage:.1f}%)")
            return 'PARTIALLY_COMPLETE'
        elif len(completed_steps) == len(workflow_steps):
            return 'COMPLETED'
        else:
            return 'FAILED'
    
    def get_parameter_matrix_status_summary(self, batch_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get a summary of parameter matrix job status
        
        Args:
            batch_id: Optional batch ID to filter results. If None, returns summary for all batches
            
        Returns:
            Dictionary containing status summary
        """
        if not self.job_scheduler.parameter_matrix.is_enabled():
            return {"error": "Parameter matrix is not enabled"}
        
        # Get parameter entries (those with param_combination_id)
        param_entries = self.job_status[self.job_status['param_combination_id'].notna()]
        
        # Filter by batch_id if specified
        if batch_id is not None:
            param_entries = param_entries[param_entries['batch_id'] == batch_id]
        
        if param_entries.empty:
            return {"error": "No parameter matrix jobs found"}
        
        # Count statuses
        status_counts = param_entries['status'].value_counts().to_dict()
        
        # Get workflow stage counts
        workflow_stage_counts = param_entries['workflow_stage'].value_counts().to_dict()
        
        # Calculate completion percentage
        total_combinations = len(param_entries)
        completed_combinations = status_counts.get('COMPLETED', 0)
        failed_combinations = status_counts.get('FAILED', 0)
        partial_combinations = status_counts.get('PARTIALLY_COMPLETE', 0)
        running_combinations = status_counts.get('RUNNING', 0)
        pending_combinations = status_counts.get('PENDING', 0)
        
        completion_percentage = (completed_combinations / total_combinations * 100) if total_combinations > 0 else 0
        
        summary = {
            "total_parameter_combinations": total_combinations,
            "parameter_columns": list(self.job_scheduler.parameter_matrix.parameters.keys()),
            "status_breakdown": {
                "completed": completed_combinations,
                "failed": failed_combinations,
                "partially_complete": partial_combinations,
                "running": running_combinations,
                "pending": pending_combinations
            },
            "workflow_stage_breakdown": workflow_stage_counts,
            "completion_percentage": round(completion_percentage, 2),
            "success_rate": round((completed_combinations / total_combinations * 100), 2) if total_combinations > 0 else 0
        }
        
        if batch_id is not None:
            summary["batch_id"] = batch_id
            
            # Add parameter-specific details
            param_details = []
            for _, param_entry in param_entries.iterrows():
                param_detail = {
                    "job_id": param_entry['job_id'],
                    "status": param_entry['status'],
                    "workflow_stage": param_entry['workflow_stage'],
                    "completion_time": param_entry['completion_time'],
                    "param_combination_id": param_entry['param_combination_id'],
                    "parameters": {}
                }
                
                # Parse parameter values from param_combination_id
                # Format: B{batch_id}_{param_name} 
                param_name = param_entry['param_combination_id'].split('_', 1)[1]
                # Find the parameter combination by name
                param_combinations = self.job_scheduler.parameter_matrix.get_parameter_combinations()
                for combo in param_combinations:
                    if combo['name'] == param_name:
                        param_detail["parameters"] = combo['parameters']
                        break
                
                param_details.append(param_detail)
            
            summary["parameter_details"] = param_details
        
        return summary
    
    def _get_next_batch_id(self) -> int:
        """Get the next batch ID to process"""
        # Get all batch IDs that are currently being processed (any status except FAILED)
        # MODIFIED: Don't consider FAILED jobs as in progress when resubmitting
        if self.resubmit_failed:
            in_progress_batch_ids = set(self.job_status[~self.job_status['status'].isin(['FAILED'])]['batch_id'])
        else:
            in_progress_batch_ids = set(self.job_status['batch_id'])
        
        # First, try to process any failed batches if resubmission is enabled
        if self.resubmit_failed and self.failed_batches:
            # Filter failed batches by the specified range if applicable
            if self.batch_range:
                min_batch, max_batch = self.batch_range
                filtered_failed_batches = [b for b in self.failed_batches if 
                                          (min_batch is None or b >= min_batch) and 
                                          (max_batch is None or b <= max_batch)]
                
                # Only consider failed batches that aren't currently in progress
                available_failed_batches = [b for b in filtered_failed_batches if b not in in_progress_batch_ids]
                
                if available_failed_batches:
                    # ADDED: Improved logging
                    print(f"Found {len(available_failed_batches)} failed batches eligible for resubmission")
                    return min(available_failed_batches)
            else:
                # Only consider failed batches that aren't currently in progress
                available_failed_batches = [b for b in self.failed_batches if b not in in_progress_batch_ids]
                if available_failed_batches:
                    # ADDED: Improved logging
                    print(f"Found {len(available_failed_batches)} failed batches eligible for resubmission")
                    return min(available_failed_batches)
        
        # Then, find the next batch that hasn't been processed or isn't in progress
        total_batches = self.batch_manager.get_num_batches()
        for batch_id in range(1, total_batches + 1):
            # Skip if batch is outside the specified range
            if self.batch_range:
                min_batch, max_batch = self.batch_range
                if (min_batch is not None and batch_id < min_batch) or \
                   (max_batch is not None and batch_id > max_batch):
                    continue
                    
            # Check if this batch is already being processed
            if batch_id not in in_progress_batch_ids:
                return batch_id
        
        # If we've already submitted jobs for all batches in the range
        return -1
    
    def prepare_environment(self) -> bool:
        """
        Prepare the environment for job submission:
        - Check if database is available or download it
        - Create batches if they don't exist
        
        Returns:
            True if preparation was successful, False otherwise
        """
        print("\n=== Preparing Environment ===")
        
        # Step 1: Check database status
        if 'path' not in self.config['database']:
            print("No database path specified in config. Skipping database check.")
        else:
            db_path = self.config['database']['path']
            if os.path.exists(db_path):
                cif_files = self.batch_manager._find_cif_files()
                if cif_files:
                    print(f"✓ Database ready: Found {len(cif_files)} CIF files at {db_path}")
                else:
                    print(f"⚠️ WARNING: Database exists at {db_path} but contains no CIF files")
                    # Try to download if remote URL is available and path is empty
                    if os.path.isdir(db_path) and not os.listdir(db_path):
                        print(f"Database directory is empty. Will try to download.")
                        if not self._ensure_database():
                            print("⚠️ Database preparation failed")
                            return False
                    else:
                        print(f"Database directory is not empty but has no CIF files. Please check the content.")
            else:
                # Download database if not present
                if not self._ensure_database():
                    print("⚠️ Database preparation failed")
                    return False
        
        # Step 2: Check batch status
        if self.batch_manager.has_batches():
            num_batches = self.batch_manager.get_num_batches()
            print(f"✓ Batches ready: Found {num_batches} existing batches")
            return True
        else:
            print("Creating batches from database...")
            batches = self.batch_manager.create_batches()
            
            if not batches:
                print("⚠️ No batches were created. Please check database path and files.")
                return False
            
            print(f"✓ Created {len(batches)} batches")
            return True
    
    def _ensure_database(self) -> bool:
        """
        Ensure the database exists, download it if necessary
        
        Returns:
            True if database exists or was downloaded successfully, False otherwise
        """
        # Skip if no database path configured
        if 'path' not in self.config['database']:
            print("No database path configured. Skipping database download.")
            return True
        
        db_path = self.config['database']['path']
        
        # If database exists and has content, we're good
        if os.path.exists(db_path):
            # For directories, check if they have CIF files
            if os.path.isdir(db_path):
                cif_files = self.batch_manager._find_cif_files()
                if cif_files:
                    print(f"✓ Database ready: Found {len(cif_files)} CIF files")
                    return True
                # If directory exists but is empty, try to download
                elif not os.listdir(db_path) and 'remote_url' in self.config['database']:
                    print(f"Database directory exists but is empty. Will download files.")
                else:
                    print(f"⚠️ Database exists but contains no CIF files and no remote URL is configured.")
                    return False
            # For single-file databases
            else:
                print(f"✓ Database file exists at: {db_path}")
                return True
        
        # If no database and no URL, we can't proceed
        if 'remote_url' not in self.config['database'] or not self.config['database']['remote_url']:
            print("⚠️ Database not found and no remote URL configured for download")
            return False
        
        # Download the database
        remote_url = self.config['database']['remote_url']
        print(f"Database not found. Downloading from: {remote_url}")
        
        # Create directory if it doesn't exist
        os.makedirs(db_path, exist_ok=True)
        
        # Get the filename from the URL
        parsed_url = urlparse(remote_url)
        filename = os.path.basename(parsed_url.path) or "database.zip"
        download_path = os.path.join(db_path, filename)
        
        try:
            # Download the file
            self._download_file(remote_url, download_path)
            
            # Extract if needed
            if self.config['database'].get('extract', False):
                self._extract_archive(download_path, db_path)
                
            # Verify we have CIF files after download
            cif_files = self.batch_manager._find_cif_files()
            if not cif_files:
                print("⚠️ WARNING: Downloaded database contains no CIF files")
                return False
            
            print(f"✓ Database download and setup complete. Contains {len(cif_files)} CIF files.")
            return True
                
        except Exception as e:
            print(f"⚠️ Failed to download or extract database: {e}")
            return False
    
    def _download_file(self, url: str, dest_path: str):
        """Download a file with progress reporting, trying different methods"""
        print(f"Attempting to download {url} to {dest_path}")
        
        # Create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
        
        # Try wget first (which has built-in progress)
        try:
            print("Trying download with wget...")
            subprocess.run(['wget', url, '-O', dest_path, '--show-progress'], check=True)
            print(f"Download complete: {dest_path}")
            return
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            print(f"wget error: {e}")
            print("wget not available, falling back to Python requests...")
        
        # Fall back to requests if wget is not available
        try:
            print("Starting download with Python requests...")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                block_size = 8192
                with open(dest_path, 'wb') as f, tqdm(
                    total=total_size, unit='B', unit_scale=True, 
                    desc=f"Downloading {os.path.basename(dest_path)}"
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=block_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            print(f"Download complete: {dest_path}")
        except Exception as e:
            print(f"Download failed with error: {e}")
            raise RuntimeError(f"Failed to download file: {e}")
    
    def _extract_archive(self, archive_path: str, extract_path: str):
        """Extract an archive file with appropriate method based on extension"""
        print(f"Extracting {archive_path} to {extract_path}")
        
        if archive_path.endswith('.zip'):
            try:
                # Try using unzip command first
                subprocess.run(['unzip', archive_path, '-d', extract_path], check=True)
            except (subprocess.SubprocessError, FileNotFoundError):
                # Fall back to Python's zipfile module
                import zipfile
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
        elif archive_path.endswith(('.tar.gz', '.tgz')):
            try:
                # Try using tar command first
                subprocess.run(['tar', '-xzf', archive_path, '-C', extract_path], check=True)
            except (subprocess.SubprocessError, FileNotFoundError):
                # Fall back to Python's tarfile module
                import tarfile
                with tarfile.open(archive_path, 'r:gz') as tar_ref:
                    tar_ref.extractall(extract_path)
        elif archive_path.endswith('.tar'):
            try:
                subprocess.run(['tar', '-xf', archive_path, '-C', extract_path], check=True)
            except (subprocess.SubprocessError, FileNotFoundError):
                import tarfile
                with tarfile.open(archive_path, 'r') as tar_ref:
                    tar_ref.extractall(extract_path)
        else:
            print(f"Warning: Unknown archive format for {archive_path}, extraction skipped.")
        
        print(f"Extraction complete to: {extract_path}")
    
    def submit_next_job(self, dry_run: bool = False) -> bool:
        """
        Submit the next batch job if possible
        
        Returns:
            True if a job was submitted, False otherwise
        """
        running_jobs = self._get_running_jobs()
        
        # Check if we can submit more jobs
        if len(running_jobs) >= self.max_concurrent_jobs:
            return False
        
        # Get the next batch to process
        next_batch_id = self._get_next_batch_id()
        
        if next_batch_id == -1:
            # No more batches to process
            return False
            
        # Double check that this batch isn't already in progress (just to be safe)
        batch_jobs = self.job_status[self.job_status['batch_id'] == next_batch_id]
        
        # For resubmission of failed jobs, we need to clear the existing entries
        # MODIFIED: If job is failed and we're resubmitting, remove from job_status first
        if self.resubmit_failed and next_batch_id in self.failed_batches and not batch_jobs.empty:
            if all(status == 'FAILED' for status in batch_jobs['status']):
                print(f"Removing failed job entries for batch {next_batch_id} to enable resubmission")
                self.job_status = self.job_status[self.job_status['batch_id'] != next_batch_id]
                batch_jobs = self.job_status[self.job_status['batch_id'] == next_batch_id]  # Should now be empty
        
        if not batch_jobs.empty:
            active_jobs = batch_jobs[batch_jobs['status'].isin(['PENDING', 'RUNNING'])]
            if not active_jobs.empty:
                print(f"⚠️ Batch {next_batch_id} already has active jobs. Skipping to prevent duplicates.")
                return False
        
        # Get the files for the batch
        try:
            batch_files = self.batch_manager.get_batch_files(next_batch_id)
            # Ensure all file paths are strings
            batch_files = [str(file_path) for file_path in batch_files if file_path]
        except ValueError as e:
            print(f"Error getting batch files: {e}")
            # If the batch doesn't exist, try to create batches first
            self.batch_manager.create_batches()
            try:
                batch_files = self.batch_manager.get_batch_files(next_batch_id)
                # Ensure all file paths are strings
                batch_files = [str(file_path) for file_path in batch_files if file_path]
            except ValueError:
                print(f"⚠️ Could not create or find batch {next_batch_id}. Skipping.")
                return False
        
        # Skip empty batches
        if not batch_files:
            print(f"⚠️ Batch {next_batch_id} has no files. Skipping.")
            self.failed_batches.add(int(next_batch_id))
            self._save_failed_batches()
            return False
        
        # Create the job script
        script_path = self.job_scheduler.create_job_script(next_batch_id, batch_files)
        
        # Submit the job - pass batch_id to store the relationship
        print(f"Submitting job for batch {next_batch_id} with {len(batch_files)} CIF files...")
        
        # Check if parameter matrix is enabled
        if self.job_scheduler.parameter_matrix.is_enabled():
            # For parameter matrix, submit individual jobs for each parameter combination
            param_combinations = self.job_scheduler.parameter_matrix.get_parameter_combinations()
            
            print(f"Parameter matrix enabled: submitting {len(param_combinations)} individual jobs")
            
            # Create individual job scripts
            coordinator_script = self.job_scheduler.create_job_script(next_batch_id, batch_files)
            
            # Submit individual jobs for each parameter combination  
            submitted_jobs = []
            for param_combo in param_combinations:
                param_id = param_combo['param_id']
                param_name = param_combo['name']
                
                # Create individual job script path
                param_script_path = os.path.join(
                    self.job_scheduler.scripts_dir, 
                    f'job_batch_{next_batch_id}_param_{param_id}.sh'
                )
                
                # Submit individual parameter job
                print(f"Submitting parameter combination {param_id}: {param_name}")
                param_job_id = self.job_scheduler.submit_job(param_script_path, dry_run=dry_run, batch_id=next_batch_id)
                
                if param_job_id:
                    submitted_jobs.append((param_job_id, param_combo))
                    
                    # Create parameter combination ID with batch prefix
                    param_combo_id = self.job_scheduler.parameter_matrix.get_sub_job_name(next_batch_id, param_id)
                    
                    # Create base job status entry with parameter combination ID
                    param_row = {
                        'batch_id': int(next_batch_id),
                        'job_id': int(param_job_id) if param_job_id != "dry-run" else f"dry-run-{param_id}",
                        'param_combination_id': param_combo_id,
                        'status': 'PENDING' if param_job_id != "dry-run" else "DRY-RUN",
                        'submission_time': self._format_timestamp(),
                        'completion_time': None,
                        'workflow_stage': 'pending'
                    }
                    
                    # Ensure all columns exist and fill missing ones with None
                    for col in self.job_status.columns:
                        if col not in param_row:
                            param_row[col] = None
                    
                    param_df = pd.DataFrame([param_row], columns=self.job_status.columns)
                    self.job_status = pd.concat([self.job_status, param_df], ignore_index=True)
                else:
                    print(f"⚠️ Failed to submit parameter combination {param_id}")
            
            success = len(submitted_jobs) > 0
            if success:
                print(f"✓ Submitted {len(submitted_jobs)} parameter combination jobs for batch {next_batch_id}")
            else:
                print(f"⚠️ No parameter jobs were submitted for batch {next_batch_id}")
            
        else:
            # Standard single job submission
            job_id = self.job_scheduler.submit_job(script_path, dry_run=dry_run, batch_id=next_batch_id)
            if job_id:
                # Standard single job entry
                new_row = {
                    'batch_id': int(next_batch_id),
                    'job_id': int(job_id) if job_id != "dry-run" else "dry-run",
                    'status': 'PENDING' if job_id != "dry-run" else "DRY-RUN",
                    'submission_time': self._format_timestamp(),
                    'completion_time': None,
                    'workflow_stage': 'pending'
                }
                
                # Fill missing columns with None for single job
                for col in self.job_status.columns:
                    if col not in new_row:
                        new_row[col] = None
                
                new_df = pd.DataFrame([new_row], columns=self.job_status.columns)
                self.job_status = pd.concat([self.job_status, new_df], ignore_index=True)
                
                print(f"✓ Submitted job for batch {next_batch_id} with job ID {job_id}")
                success = True
            else:
                print(f"⚠️ Failed to submit job for batch {next_batch_id}")
                success = False
        
        if success:
            self._save_job_status()
            
            # Remove batch from failed batches if it was a retry
            if next_batch_id in self.failed_batches:
                print(f"✓ Batch {next_batch_id} resubmitted successfully, removing from failed batches")
                self.failed_batches.remove(next_batch_id)
                self._save_failed_batches()
            
            return True
        else:
            return False
    
    def clean_job_status(self):
        """
        Clean up job status file by removing duplicate submissions and fixing inconsistencies.
        Also cancels duplicate jobs in the SLURM queue to prevent wasted resources.
        
        Prioritization strategy for duplicate jobs:
        1. PENDING jobs are preferred over RUNNING jobs
        2. For multiple PENDING jobs, keep the oldest submitted
        3. For multiple RUNNING jobs, keep the latest (newest) submitted
        
        Returns:
            Number of issues fixed
        """
        print("\n=== Cleaning Job Status File ===")
        if self.job_status.empty:
            print("Job status file is empty. Nothing to clean.")
            return 0
            
        original_count = len(self.job_status)
        issues_fixed = 0
        
        # Make sure subprocess is imported
        import subprocess
        
        # Identify batches with multiple active jobs
        batch_groups = self.job_status.groupby('batch_id')
        problematic_batches = []
        
        for batch_id, group in batch_groups:
            active_jobs = group[group['status'].isin(['PENDING', 'RUNNING'])]
            if len(active_jobs) > 1:
                problematic_batches.append((batch_id, len(active_jobs)))
                
                # Logic for prioritization:
                # 1. If PENDING jobs exist, keep the oldest PENDING job
                # 2. If only RUNNING jobs exist, keep the newest RUNNING job
                
                pending_jobs = active_jobs[active_jobs['status'] == 'PENDING']
                if not pending_jobs.empty:
                    # PENDING jobs exist - keep oldest PENDING job
                    sorted_pending = pending_jobs.sort_values('submission_time', ascending=True)
                    job_to_keep_idx = sorted_pending.index[0]
                    job_to_keep_id = self.job_status.loc[job_to_keep_idx, 'job_id']
                    print(f"Batch {batch_id}: Keeping oldest PENDING job {job_to_keep_id}")
                else:
                    # Only RUNNING jobs - keep newest RUNNING job
                    sorted_running = active_jobs.sort_values('submission_time', ascending=False)
                    job_to_keep_idx = sorted_running.index[0]
                    job_to_keep_id = self.job_status.loc[job_to_keep_idx, 'job_id']
                    print(f"Batch {batch_id}: Keeping newest RUNNING job {job_to_keep_id}")
                
                # Mark all other jobs as 'CANCELLED' and actually cancel them in SLURM
                for idx in active_jobs.index:
                    if idx != job_to_keep_idx:
                        job_id = str(self.job_status.loc[idx, 'job_id'])
                        job_status = self.job_status.loc[idx, 'status']
                        
                        # Skip "dry-run" job IDs
                        if job_id != "dry-run":
                            try:
                                # Issue scancel command to cancel job
                                print(f"  Cancelling {job_status} job {job_id} for batch {batch_id}")
                                subprocess.run(['scancel', job_id], check=False)
                                issues_fixed += 1
                            except Exception as e:
                                print(f"  Failed to cancel job {job_id}: {e}")
                        
                        # Update job status in tracking file
                        self.job_status.loc[idx, 'status'] = 'CANCELLED'
                        # Use formatted timestamp without fractional seconds
                        self.job_status.loc[idx, 'completion_time'] = self._format_timestamp()
        
        if problematic_batches:
            print(f"\nFound {len(problematic_batches)} batches with multiple active jobs")
            
            # Save the cleaned job status
            self._save_job_status()
            print(f"Fixed {issues_fixed} duplicate job submissions and cancelled them in the SLURM queue")
        else:
            print("✓ No batches with duplicate active jobs found")
            
        return issues_fixed
    
    def run(self, polling_interval: int = 60, dry_run: bool = False, resubmit_failed: Optional[bool] = None):
        """
        Run the job tracking and submission process
        
        Args:
            polling_interval: Time (in seconds) to wait between status checks
            dry_run: If True, only generate job scripts but don't submit them
            resubmit_failed: If provided, overrides the current resubmit_failed setting
        """
        # ADDED: More explicit resubmit_failed handling
        if resubmit_failed is not None:
            self.resubmit_failed = resubmit_failed
            if self.resubmit_failed:
                print("🔄 Resubmission of failed jobs is ENABLED")
            else:
                print("ℹ️ Resubmission of failed jobs is DISABLED")
        
        print("=== Starting gRASPA Job Tracker ===")
        print(f"Output path: {self.output_path}")
        print(f"Maximum concurrent jobs: {self.max_concurrent_jobs}")
        print(f"Polling interval: {polling_interval} seconds")
        print(f"Resubmit failed jobs: {'Yes' if self.resubmit_failed else 'No'}")
        
        # Clean up any inconsistencies in the job status file
        self.clean_job_status()
        
        # Display batch range if specified
        if self.batch_range:
            min_batch, max_batch = self.batch_range
            print(f"Batch range: {min_batch or 'START'} to {max_batch or 'END'}")
        
        # ADDED: Log any failed batches at the start
        if self.failed_batches:
            print(f"Found {len(self.failed_batches)} failed batches: {sorted(self.failed_batches)}")
            if self.resubmit_failed:
                print("These failed batches will be resubmitted")
        
        # Prepare environment first
        if not self.prepare_environment():
            print("⚠️ Environment preparation failed. Exiting.")
            return
        
        # Check if there are batches to process
        total_batches = self.batch_manager.get_num_batches()
        if total_batches == 0:
            print("⚠️ No batches found to process. Exiting.")
            return
        
        # Get a set of processed batch IDs for more accurate counting
        processed_batch_ids = set(self.job_status['batch_id'])
        processed_batches = len(processed_batch_ids)
        
        if processed_batches > 0:
            print(f"ℹ️ Found {processed_batches} previously processed batches")
        
        # Count failed batches that aren't already in the processed list
        new_failed_batches = self.failed_batches - processed_batch_ids
        
        # Calculate remaining batches correctly
        unprocessed_batches = total_batches - processed_batches
        retry_batches = len(new_failed_batches)
        remaining_batches = unprocessed_batches + retry_batches
        
        if remaining_batches == 0:
            print("✅ All batches have already been processed. Nothing to do.")
            if self.failed_batches:
                print(f"⚠️ There are {len(self.failed_batches)} failed batches that can be retried.")
            return
        
        # Provide more detailed information about batches to process
        if retry_batches > 0:
            print(f"ℹ️ Will process {unprocessed_batches} new batches and retry {retry_batches} failed batches")
        else:
            print(f"ℹ️ Will process {remaining_batches} remaining batches")
        
        # Main loop
        print("\n=== Starting Job Submission Loop ===")
        try:
            while True:
                # Force a refresh of job status from any external changes
                try:
                    if os.path.exists(self.job_status_file):
                        previous_status = self.job_status.copy()
                        self.job_status = pd.read_csv(self.job_status_file)
                        
                        # Check if any status changed from external updates
                        if not previous_status.equals(self.job_status):
                            print("Job status file was updated externally. Refreshed job status.")
                except Exception as e:
                    print(f"Warning: Could not refresh job status from file: {e}")

                # Then get current running jobs
                running_jobs = self._get_running_jobs()
                if running_jobs:
                    print(f"Currently running jobs: {len(running_jobs)}")
                    for job_id in running_jobs:
                        # Find the batch ID for this job ID safely
                        if job_id == "dry-run":
                            job_rows = self.job_status[self.job_status['job_id'] == "dry-run"]
                        else:
                            job_rows = self.job_status[self.job_status['job_id'] == int(job_id)]
                        if not job_rows.empty:
                            batch_id = job_rows['batch_id'].iloc[0]
                            print(f"  - Batch {batch_id}: Job ID {job_id}")
                        else:
                            # Try to get batch ID from the job scheduler's mapping
                            batch_id = self.job_scheduler.get_batch_id_for_job(job_id)
                            if batch_id is not None:
                                print(f"  - Batch {batch_id}: Job ID {job_id}")
                            else:
                                print(f"  - Unknown batch: Job ID {job_id}")
                
                # Try to submit new jobs if needed
                jobs_submitted = 0
                while len(running_jobs) + jobs_submitted < self.max_concurrent_jobs:
                    submitted = self.submit_next_job(dry_run=dry_run)
                    if submitted:
                        jobs_submitted += 1
                    else:
                        break
                
                # If no running jobs and no jobs were submitted, we're done
                if len(running_jobs) == 0 and jobs_submitted == 0:
                    if self.failed_batches:
                        print(f"\n⚠️ All jobs completed but there are {len(self.failed_batches)} failed batches.")
                        print(f"Failed batches: {sorted(self.failed_batches)}")
                    else:
                        print("\n✅ All jobs completed successfully!")
                    break
                
                # Wait for the polling interval
                print(f"Waiting {polling_interval} seconds before next check...")
                time.sleep(polling_interval)
                
        except KeyboardInterrupt:
            print("\n\nJob tracker interrupted by user. Currently running jobs will continue.")
            
        print("\n=== Job Tracker Finished ===")
        print(f"Job status saved to: {self.job_status_file}")
        if self.failed_batches:
            print(f"Failed batches saved to: {self.failed_batches_file}")
    
    def run_single_cif(self, cif_path, dry_run=False):
        """
        Run a simulation for a single CIF file
        
        Args:
            cif_path: Path to the CIF file
            dry_run: If True, generate job script but don't submit
            
        Returns:
            True if successful, False otherwise
        """
        cif_name = os.path.basename(cif_path)
        structure_name = os.path.splitext(cif_name)[0]
        
        print(f"Processing structure: {structure_name}")
        
        # Create a singles directory under the output directory structure
        singles_dir = os.path.join(self.config['output']['base_dir'], 'singles')
        os.makedirs(singles_dir, exist_ok=True)
        
        # Create structure-specific directory for this single job
        structure_dir = os.path.join(singles_dir, structure_name)
        os.makedirs(structure_dir, exist_ok=True)
        
        # Create job scripts directory
        scripts_dir = os.path.join(structure_dir, 'scripts')
        os.makedirs(scripts_dir, exist_ok=True)
        
        # Create results directory
        results_dir = os.path.join(structure_dir, 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        # Copy CIF file to expected location if needed
        target_cif_path = cif_path
        if not os.path.exists(cif_path):
            print(f"❌ CIF file not found: {cif_path}")
            return False
        
        if not cif_path.startswith(self.config['database']['path']):
            target_cif_path = os.path.join(self.config['database']['path'], cif_name)
            shutil.copy(cif_path, target_cif_path)
            print(f"Copied CIF file to database directory: {target_cif_path}")
        
        # Generate job script
        job_script_path = os.path.join(scripts_dir, f"job_{structure_name}.sh")
        print(f"Generating job script at: {job_script_path}")
        
        # Determine the next available batch ID by checking existing batch files
        batch_dir = self.config['output']['batches_dir']
        batch_files = [f for f in os.listdir(batch_dir) 
                      if f.startswith('batch_') and f.endswith('.csv')]
        
        # Extract numbers from batch_X.csv filenames and find max
        batch_numbers = []
        for f in batch_files:
            try:
                num_part = f.replace('batch_', '').replace('.csv', '')
                batch_numbers.append(int(num_part))
            except ValueError:
                continue
        
        # Use max batch number + 1, or start from 1 if none exists
        batch_id = max(batch_numbers) + 1 if batch_numbers else 1
        
        print(f"Assigned batch ID: {batch_id}")
        
        # Create a batch file for this single CIF
        batch_file = os.path.join(batch_dir, f'batch_{batch_id}.csv')
        
        # Create a batch CSV file with this single CIF file
        batch_df = pd.DataFrame({'file_path': [target_cif_path]})
        batch_df.to_csv(batch_file, index=False)
        print(f"Created batch file: {batch_file}")
        
        # Create batch results directory
        batch_results_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_id}')
        os.makedirs(batch_results_dir, exist_ok=True)
        
        # Create a modified config for the job scheduler with the correct output directory
        modified_config = self.config.copy()
        modified_config['output'] = self.config['output'].copy()
        
        # Create a temporary directory for script generation
        temp_scripts_dir = os.path.join(singles_dir, 'temp_scripts')
        os.makedirs(temp_scripts_dir, exist_ok=True)
        modified_config['output']['scripts_dir'] = temp_scripts_dir
        
        # Use the job scheduler with modified config
        job_scheduler = JobScheduler(modified_config)
        
        # Create the job script - this will return a path to the generated script
        try:
            script_path = job_scheduler.create_job_script(batch_id, [target_cif_path])
            
            if script_path and os.path.exists(script_path):
                # Copy the generated script to our desired location
                shutil.copy(script_path, job_script_path)
                print(f"Created job script: {job_script_path}")
            else:
                print("❌ Failed to generate job script")
                return False
        except Exception as e:
            print(f"❌ Error generating job script: {e}")
            return False
        
        # Submit the job if not dry run
        if not dry_run:
            job_id = job_scheduler.submit_job(job_script_path, batch_id=batch_id)
            if job_id:
                print(f"Job submitted with ID: {job_id}")
                print(f"Results will be stored in: {batch_results_dir}")
                
                # Update job status
                timestamp = self._format_timestamp()
                new_row = {
                    'batch_id': batch_id,  # Now using an integer batch ID
                    'job_id': job_id if job_id != "dry-run" else "dry-run",
                    'status': 'PENDING',
                    'submission_time': timestamp,
                    'completion_time': None
                }
                
                # Create a new DataFrame with correct types and append
                new_df = pd.DataFrame([new_row], columns=self.job_status.columns).astype(self.job_status.dtypes)
                self.job_status = pd.concat([self.job_status, new_df], ignore_index=True)
                self._save_job_status()
                
                return True
            else:
                print("❌ Job submission failed")
                return False
        else:
            print(f"[DRY RUN] Would submit job script: {job_script_path}")
            print(f"Results would be stored in: {batch_results_dir}")
            return True
