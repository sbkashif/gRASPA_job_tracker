import argparse
import os
import sys
import time
from typing import Dict, Any
import traceback

from .config_parser import ConfigParser
from .job_tracker import JobTracker
from .batch_manager import BatchManager
from .utils import create_default_config, check_slurm_available

def display_config_summary(config: Dict[str, Any]):
    """Display a summary of the loaded configuration"""
    print("\n=== Configuration Summary ===")
    print(f"Project: {config['project'].get('name', 'Unnamed')}")
    print(f"Database path: {config['database']['path']}")
    
    # Display output directories
    if 'output' in config:
        print("Output directories:")
        for name, path in config['output'].items():
            if path:
                print(f"  - {name}: {path}")
    
    # Display batch settings
    print("\nBatch settings:")
    if 'batch' in config:
        batch_size = config['batch'].get('size', 100)
        max_jobs = config['batch'].get('max_concurrent_jobs', 5)
        strategy = config['batch'].get('strategy', 'alphabetical')
        print(f"  - Size: {batch_size} structures per batch")
        print(f"  - Max concurrent jobs: {max_jobs}")
        print(f"  - Batching strategy: {strategy}")
    
    # Display SLURM settings
    print("\nSLURM settings:")
    if 'slurm_config' in config:
        for key, value in config['slurm_config'].items():
            print(f"  - {key}: {value}")
    
    print("==========================\n")

def verify_environment(config: Dict[str, Any], batch_manager: BatchManager) -> bool:
    """
    Verify that the environment is properly set up
    
    Args:
        config: Configuration dictionary
        batch_manager: Initialized BatchManager instance
        
    Returns:
        True if verification passed, False otherwise
    """
    print("Performing pre-run verification...")
    all_ok = True
    needs_database_download = False
    
    # Verify database exists
    db_path = config['database']['path']
    if not os.path.exists(db_path):
        print(f"⚠️ WARNING: Database path does not exist: {db_path}")
        if 'remote_url' in config['database'] and config['database']['remote_url']:
            print(f"  - Database will be downloaded from: {config['database']['remote_url']}")
            needs_database_download = True
        else:
            print(f"  - No remote URL configured for database download")
            all_ok = False
    else:
        print(f"✓ Database found at: {db_path}")
        # Count CIF files
        cif_files = batch_manager._find_cif_files()
        if cif_files:
            print(f"  - Found {len(cif_files)} CIF files")
        else:
            print(f"⚠️ WARNING: No CIF files found in database directory")
            all_ok = False
    
    # Verify output directories exist
    if 'output' in config:
        for dir_name, dir_path in config['output'].items():
            if dir_path:
                if os.path.exists(dir_path):
                    print(f"✓ Output directory '{dir_name}' found: {dir_path}")
                else:
                    print(f"⚠️ Output directory will be created: {dir_path}")
    
    # Verify script paths
    if 'scripts' in config:
        for script_name, script_path in config['scripts'].items():
            if not script_path:
                continue
                
            if os.path.exists(script_path):
                if script_path.endswith(('.sh', '.bash')) and not os.access(script_path, os.X_OK):
                    print(f"⚠️ WARNING: Script '{script_name}' is not executable: {script_path}")
                    all_ok = False
                else:
                    print(f"✓ Script '{script_name}' found: {script_path}")
            elif '.' in script_path and not os.path.sep in script_path:
                # Could be a Python module
                try:
                    module_name = '.'.join(script_path.split('.')[:-1])
                    __import__(module_name)
                    print(f"✓ Python module '{script_name}' found: {script_path}")
                except (ImportError, ModuleNotFoundError):
                    print(f"⚠️ WARNING: Python module '{script_name}' not found: {script_path}")
                    all_ok = False
            else:
                print(f"⚠️ WARNING: Script '{script_name}' not found: {script_path}")
                all_ok = False
    
    # Verify template files exist
    if 'file_templates' in config:
        for template_name, template_path in config['file_templates'].items():
            if template_path and os.path.exists(template_path):
                print(f"✓ Template '{template_name}' found: {template_path}")
            else:
                print(f"⚠️ WARNING: Template '{template_name}' not found: {template_path}")
                all_ok = False
    
    # Check if batches are already created
    if batch_manager.has_batches():
        num_batches = batch_manager.get_num_batches()
        print(f"✓ Batches already created: {num_batches} batches")
    else:
        print("ℹ️ No batches found - will be created during execution")
    
    # Check for SLURM availability
    if check_slurm_available():
        print("✓ SLURM commands available")
    else:
        print("⚠️ WARNING: SLURM commands not found. Job submission may fail.")
        all_ok = False
    
    print("Pre-run verification complete.\n")
    
    if needs_database_download:
        print("\nNOTE: Database download will be performed during the preparation stage.")
        print("      Make sure to run with normal mode or --prepare-only flag to download the database.")
    
    return all_ok

def main():
    parser = argparse.ArgumentParser(description='GRASPA Job Tracker - Automate simulation batches')
    parser.add_argument('--config', '-c', type=str, required=True,
                        help='Path to configuration file')
    parser.add_argument('--polling-interval', '-p', type=int, default=60,
                        help='Polling interval in seconds (default: 60)')
    parser.add_argument('--create-default-config', action='store_true',
                        help='Create a default configuration file')
    parser.add_argument('--prepare-only', action='store_true',
                        help='Only prepare environment (download database and create batches)')
    parser.add_argument('--download-db-only', action='store_true',
                        help='Only download the database without further processing')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Force continuing even if verification fails')
    parser.add_argument('--no-confirm', action='store_true',
                        help='Skip confirmation prompt')
    parser.add_argument('--version', '-v', action='store_true',
                        help='Show version information')
    parser.add_argument('--dry-run', action='store_true', help='Generate job scripts but don\'t submit them')
    parser.add_argument('--min-batch', '-min', type=int, help='Minimum batch ID to process')
    parser.add_argument('--max-batch', '-max', type=int, help='Maximum batch ID to process')
    parser.add_argument('--resubmit-failed', action='store_true', help='Resubmit failed jobs (default: do not resubmit)')
    parser.add_argument('--update-status', action='store_true',
                        help='Just scan and update status of all batches without submitting new jobs')
    args = parser.parse_args()
    
    # Show version information
    if args.version:
        from . import __version__
        print(f"gRASPA Job Tracker version {__version__}")
        return
    
    # Create default config if requested
    if args.create_default_config:
        create_default_config(args.config)
        print(f"✅ Created default configuration at {args.config}")
        print("Please edit this file with your specific settings before running again.")
        return
    
    # Load and validate configuration
    try:
        print(f"Loading configuration from {args.config}...")
        config_parser = ConfigParser(args.config)
        config = config_parser.get_config()
        
        # Display configuration summary
        display_config_summary(config)
        
        # Set batch range if specified
        batch_range = None
        if args.min_batch is not None or args.max_batch is not None:
            batch_range = (args.min_batch, args.max_batch)
            print(f"Processing batches in range: {args.min_batch or 'START'} to {args.max_batch or 'END'}")
        
        # Check for update-status mode
        if args.update_status:
            print("=== Status Update Mode - Scanning batch status without submitting jobs ===")
            tracker = JobTracker(config, batch_range=batch_range)
            
            try:
                tracker.clean_job_status()
                running_jobs = tracker._get_running_jobs()
                
                print("\n=== Job Status Update Summary ===")
                if tracker.job_status.empty:
                    print("No jobs found in tracking file.")
                else:
                    status_counts = tracker.job_status['status'].value_counts().to_dict()
                    print(f"PENDING:   {status_counts.get('PENDING', 0)}")
                    print(f"RUNNING:   {status_counts.get('RUNNING', 0)}")
                    print(f"COMPLETED: {status_counts.get('COMPLETED', 0)}")
                    print(f"FAILED:    {status_counts.get('FAILED', 0)}")
                    other = sum(count for status, count in status_counts.items() 
                              if status not in ['PENDING', 'RUNNING', 'COMPLETED', 'FAILED'])
                    if other > 0:
                        print(f"OTHER:     {other}")
                
                print(f"\nJob status saved to: {tracker.job_status_file}")
                if tracker.failed_batches:
                    print(f"Failed batches saved to: {tracker.failed_batches_file}")
                    print(f"Total failed batches: {len(tracker.failed_batches)}")
                    
                print("✅ Status update completed successfully")
                
            except Exception as e:
                print(f"⚠️ Error updating status: {e}")
                if os.environ.get("DEBUG"):
                    traceback.print_exc()
            return
        
        # Create and run job tracker early so we can use it for download-only mode
        tracker = JobTracker(config, batch_range=batch_range)
        
        # Check for download-only mode early, before verification
        if args.download_db_only:
            print("=== Download-only mode - Downloading database ===")
            # Force download if database doesn't exist
            if not os.path.exists(config['database']['path']) or args.force:
                if os.path.exists(config['database']['path']) and args.force:
                    print("Force flag set, re-downloading database...")
                    # Rename existing database
                    backup_path = f"{config['database']['path']}_backup_{int(time.time())}"
                    os.rename(config['database']['path'], backup_path)
                    print(f"Existing database backed up to: {backup_path}")
                
                try:
                    success = tracker._ensure_database()
                    if success:
                        print("✅ Database download completed successfully")
                        return
                    else:
                        print("⚠️ Database download failed")
                        sys.exit(1)
                except Exception as e:
                    print(f"⚠️ Database download failed with error: {e}")
                    if os.environ.get("DEBUG"):
                        traceback.print_exc()
                    sys.exit(1)
            else:
                print(f"Database already exists at: {config['database']['path']}")
                print("Use --force to re-download the database")
                return
        
        # Similarly, check prepare-only mode early
        if args.prepare_only:
            print("=== Prepare-only mode - Creating batches without submitting jobs ===")
            print("This will download the database if needed and create batches.")
            
            # Ask for confirmation before continuing with prepare-only
            if not args.no_confirm:
                prompt = "Proceed with environment preparation (download database and create batches)? [Y/n] "
                if input(prompt).lower() in ['n', 'no']:
                    print("Operation cancelled by user.")
                    return
            
            success = tracker.prepare_environment()
            if success:
                print("✅ Environment preparation completed successfully")
                return
            else:
                print("⚠️ Environment preparation failed")
                sys.exit(1)
        
        # Initialize BatchManager for verification
        batch_manager = BatchManager(config)
        
        # Verify environment before running normal mode
        verification_passed = verify_environment(config, batch_manager)
        
        if not verification_passed and not args.force:
            print("\n⚠️ Verification failed. You can:")
            print("  1. Fix the reported issues and try again")
            print("  2. Run with --force to continue anyway: gRASPA_job_tracker -c config.yaml --force")
            print("  3. Run with --download-db-only to just download the database:")
            print("     gRASPA_job_tracker -c config.yaml --download-db-only")
            print("  4. Run with --prepare-only to download database and create batches:")
            print("     gRASPA_job_tracker -c config.yaml --prepare-only\n")
            return
        
        # Ask for confirmation before continuing with normal mode
        if not args.no_confirm:
            prompt = "Continue with job tracker (may include download database if not already done, create batches if not already done, and submit jobs)? [Y/n] "
            if input(prompt).lower() in ['n', 'no']:
                print("Operation cancelled by user.")
                return
                
        resubmit_failed = None
        if args.resubmit_failed:
            resubmit_failed = True
        # Normal mode - run the job tracker
        print("=== Starting job tracker ===")
        # The prepare_environment() will be called inside run()
        tracker.run(polling_interval=args.polling_interval, \
            dry_run=args.dry_run,
            resubmit_failed=resubmit_failed)
        
    except Exception as e:
        print(f"⚠️ ERROR: Configuration error: {e}")
        if os.environ.get("DEBUG"):
            traceback.print_exc()
        return

if __name__ == '__main__':
    main()
