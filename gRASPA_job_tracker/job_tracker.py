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
        
        self.max_concurrent_jobs = config['batch'].get('max_concurrent_jobs', 5)
        
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
        """Get the set of currently running or pending job IDs and update status of completed jobs"""
        running_jobs = set()
        
        # Filter for jobs with status 'RUNNING' or 'PENDING'
        active_jobs = self.job_status[self.job_status['status'].isin(['RUNNING', 'PENDING'])]
        
        for _, job in active_jobs.iterrows():
            job_id = str(job['job_id'])  # Ensure job_id is a string
            batch_id = job['batch_id']   # Get batch_id directly from the current job row
            
            # Skip "dry-run" job IDs
            if job_id == "dry-run":
                continue
                
            # Get the batch_output_dir for this job
            batch_output_dir = os.path.join(self.results_dir, f'batch_{batch_id}')
            
            # Pass batch_output_dir for more accurate status checking
            status = self.job_scheduler.get_job_status(job_id, batch_output_dir)
            
            if status in ['RUNNING', 'PENDING']:
                running_jobs.add(job_id)
            else:
                # Update job status if it's no longer running or pending
                mask = self.job_status['job_id'] == job_id
                if any(mask):  # Verify there are rows that match this job_id
                    self.job_status.loc[mask, 'status'] = status
                    
                    if status in ['COMPLETED', 'CANCELLED', 'FAILED', 'TIMEOUT', 'UNKNOWN']:
                        # Format timestamp as readable date-time
                        completion_time = self.job_scheduler._format_datetime(time.time())
                        self.job_status.loc[mask, 'completion_time'] = completion_time
                        
                        # Check exit status file to determine if job succeeded
                        exit_status_file = os.path.join(self.results_dir, f'batch_{batch_id}', 'exit_status.log')
                        
                        if os.path.exists(exit_status_file):
                            with open(exit_status_file, 'r') as f:
                                exit_status = f.read().strip()
                                if exit_status != '0':
                                    self.failed_batches.add(int(batch_id))
                                    print(f"⚠️ Batch {batch_id} failed with exit status {exit_status}")
                        else:
                            # No exit status file means the job failed
                            self.failed_batches.add(int(batch_id))
                            print(f"⚠️ Batch {batch_id} failed - no exit status file found")
        
        self._save_job_status()
        self._save_failed_batches()
        
        return running_jobs
    
    def _get_next_batch_id(self) -> int:
        """Get the next batch ID to process"""
        # First, try to process any failed batches
        if self.failed_batches:
            # Filter failed batches by the specified range if applicable
            if self.batch_range:
                min_batch, max_batch = self.batch_range
                filtered_failed_batches = [b for b in self.failed_batches if 
                                          (min_batch is None or b >= min_batch) and 
                                          (max_batch is None or b <= max_batch)]
                if filtered_failed_batches:
                    return min(filtered_failed_batches)
            else:
                return min(self.failed_batches)
        
        # Then, find the next batch that hasn't been processed
        processed_batch_ids = set(self.job_status['batch_id'])
        
        total_batches = self.batch_manager.get_num_batches()
        for batch_id in range(1, total_batches + 1):
            # Skip if batch is outside the specified range
            if self.batch_range:
                min_batch, max_batch = self.batch_range
                if (min_batch is not None and batch_id < min_batch) or \
                   (max_batch is not None and batch_id > max_batch):
                    continue
                    
            if batch_id not in processed_batch_ids:
                return batch_id
        
        # If all batches have been processed, return -1
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
                # Add to failed batches to avoid retrying
                self.failed_batches.add(int(next_batch_id))
                self._save_failed_batches()
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
        job_id = self.job_scheduler.submit_job(script_path, dry_run=dry_run, batch_id=next_batch_id)
        
        if job_id:
            # Update job status
            new_row = {
                'batch_id': int(next_batch_id),
                'job_id': str(job_id),
                'status': 'PENDING' if job_id != "dry-run" else "DRY-RUN",
                'submission_time': self.job_scheduler._format_datetime(time.time()),
                'completion_time': None
            }
            # Create a new DataFrame with the same dtypes as self.job_status
            new_df = pd.DataFrame([new_row], columns=self.job_status.columns).astype(self.job_status.dtypes)
            self.job_status = pd.concat([self.job_status, new_df], ignore_index=True)
            self._save_job_status()
            
            # Remove batch from failed batches if it was a retry
            if next_batch_id in self.failed_batches:
                self.failed_batches.remove(next_batch_id)
                self._save_failed_batches()
                
            print(f"✓ Submitted job for batch {next_batch_id} with job ID {job_id}")
            return True
        else:
            print(f"⚠️ Failed to submit job for batch {next_batch_id}")
            return False
    
    def run(self, polling_interval: int = 60, dry_run: bool = False):
        """
        Run the job tracking and submission process
        
        Args:
            polling_interval: Time (in seconds) to wait between status checks
            dry_run: If True, only generate job scripts but don't submit them
        """
        print("=== Starting GRASPA Job Tracker ===")
        print(f"Output path: {self.output_path}")
        print(f"Maximum concurrent jobs: {self.max_concurrent_jobs}")
        print(f"Polling interval: {polling_interval} seconds")
        
        # Display batch range if specified
        if self.batch_range:
            min_batch, max_batch = self.batch_range
            print(f"Batch range: {min_batch or 'START'} to {max_batch or 'END'}")
        
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
                # Get current running jobs
                running_jobs = self._get_running_jobs()
                if running_jobs:
                    print(f"Currently running jobs: {len(running_jobs)}")
                    for job_id in running_jobs:
                        # Find the batch ID for this job ID safely
                        job_rows = self.job_status[self.job_status['job_id'] == job_id]
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
