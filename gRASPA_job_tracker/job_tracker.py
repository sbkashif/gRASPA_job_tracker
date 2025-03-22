import os
import time
from typing import Dict, List, Any, Set
import pandas as pd

from .batch_manager import BatchManager
from .job_scheduler import JobScheduler

class JobTracker:
    """Track job progress and manage job submission"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the job tracker
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.output_path = config['output_path']
        self.max_concurrent_jobs = config['max_concurrent_jobs']
        
        # Initialize batch manager and job scheduler
        self.batch_manager = BatchManager(config)
        self.job_scheduler = JobScheduler(config)
        
        # Create tracking files
        self.job_status_file = os.path.join(self.output_path, 'job_status.csv')
        self.failed_batches_file = os.path.join(self.output_path, 'failed_batches.txt')
        
        # Initialize or load job status tracking
        self._initialize_job_status()
    
    def _initialize_job_status(self):
        """Initialize or load job status tracking"""
        if os.path.exists(self.job_status_file):
            self.job_status = pd.read_csv(self.job_status_file)
        else:
            self.job_status = pd.DataFrame(columns=[
                'batch_id', 'job_id', 'status', 'submission_time', 'completion_time'
            ])
            self.job_status.to_csv(self.job_status_file, index=False)
        
        # Initialize failed batches list
        if os.path.exists(self.failed_batches_file):
            with open(self.failed_batches_file, 'r') as f:
                self.failed_batches = set(int(line.strip()) for line in f if line.strip())
        else:
            self.failed_batches = set()
            with open(self.failed_batches_file, 'w') as f:
                pass
    
    def _save_job_status(self):
        """Save current job status to file"""
        self.job_status.to_csv(self.job_status_file, index=False)
    
    def _save_failed_batches(self):
        """Save failed batches to file"""
        with open(self.failed_batches_file, 'w') as f:
            for batch_id in self.failed_batches:
                f.write(f"{batch_id}\n")
    
    def _get_running_jobs(self) -> Set[str]:
        """Get the set of currently running or pending job IDs"""
        running_jobs = set()
        
        # Filter for jobs with status 'RUNNING' or 'PENDING'
        active_jobs = self.job_status[self.job_status['status'].isin(['RUNNING', 'PENDING'])]
        
        for _, job in active_jobs.iterrows():
            job_id = job['job_id']
            status = self.job_scheduler.get_job_status(job_id)
            
            if status in ['RUNNING', 'PENDING']:
                running_jobs.add(job_id)
            else:
                # Update job status if it's no longer running or pending
                self.job_status.loc[self.job_status['job_id'] == job_id, 'status'] = status
                
                if status in ['COMPLETED', 'CANCELLED', 'FAILED', 'TIMEOUT', 'UNKNOWN']:
                    self.job_status.loc[self.job_status['job_id'] == job_id, 'completion_time'] = time.time()
                    
                    # Check exit status file to determine if job succeeded
                    batch_id = self.job_status.loc[self.job_status['job_id'] == job_id, 'batch_id'].iloc[0]
                    exit_status_file = os.path.join(self.output_path, f'batch_{batch_id}', 'exit_status.log')
                    
                    if os.path.exists(exit_status_file):
                        with open(exit_status_file, 'r') as f:
                            exit_status = f.read().strip()
                            if exit_status != '0':
                                self.failed_batches.add(batch_id)
                    else:
                        # No exit status file means the job failed
                        self.failed_batches.add(batch_id)
        
        self._save_job_status()
        self._save_failed_batches()
        
        return running_jobs
    
    def _get_next_batch_id(self) -> int:
        """Get the next batch ID to process"""
        # First, try to process any failed batches
        if self.failed_batches:
            return min(self.failed_batches)
        
        # Then, find the next batch that hasn't been processed
        processed_batch_ids = set(self.job_status['batch_id'])
        
        total_batches = self.batch_manager.get_num_batches()
        for batch_id in range(1, total_batches + 1):
            if batch_id not in processed_batch_ids:
                return batch_id
        
        # If all batches have been processed, return -1
        return -1
    
    def submit_next_job(self) -> bool:
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
        
        # Get the files for the batch
        try:
            batch_files = self.batch_manager.get_batch_files(next_batch_id)
        except ValueError:
            # If the batch doesn't exist, create batches first
            self.batch_manager.create_batches()
            batch_files = self.batch_manager.get_batch_files(next_batch_id)
        
        # Create the job script
        script_path = self.job_scheduler.create_job_script(next_batch_id, batch_files)
        
        # Submit the job
        job_id = self.job_scheduler.submit_job(script_path)
        
        if job_id:
            # Update job status
            new_row = {
                'batch_id': next_batch_id,
                'job_id': job_id,
                'status': 'PENDING',
                'submission_time': time.time(),
                'completion_time': None
            }
            
            self.job_status = pd.concat([self.job_status, pd.DataFrame([new_row])], ignore_index=True)
            self._save_job_status()
            
            # Remove batch from failed batches if it was a retry
            if next_batch_id in self.failed_batches:
                self.failed_batches.remove(next_batch_id)
                self._save_failed_batches()
                
            print(f"Submitted job for batch {next_batch_id} with job ID {job_id}")
            return True
        else:
            print(f"Failed to submit job for batch {next_batch_id}")
            return False
    
    def run(self, polling_interval: int = 60):
        """
        Run the job tracking and submission process
        
        Args:
            polling_interval: Time (in seconds) to wait between status checks
        """
        print("Starting GRASPA job tracker")
        
        # Create batches if needed
        if not os.path.exists(os.path.join(self.output_path, 'batches')):
            print("Creating batches of CIF files...")
            self.batch_manager.create_batches()
            print(f"Created {self.batch_manager.get_num_batches()} batches")
        
        # Main loop
        while True:
            # Get current running jobs
            running_jobs = self._get_running_jobs()
            
            print(f"Currently running jobs: {len(running_jobs)}")
            
            # Try to submit new jobs if needed
            jobs_submitted = 0
            while len(running_jobs) + jobs_submitted < self.max_concurrent_jobs:
                submitted = self.submit_next_job()
                if submitted:
                    jobs_submitted += 1
                else:
                    break
            
            # If no running jobs and no jobs were submitted, we're done
            if len(running_jobs) == 0 and jobs_submitted == 0:
                if self.failed_batches:
                    print(f"All jobs completed but there are {len(self.failed_batches)} failed batches.")
                    print(f"Failed batches: {sorted(self.failed_batches)}")
                else:
                    print("All jobs completed successfully!")
                break
            
            # Wait for the polling interval
            time.sleep(polling_interval)
