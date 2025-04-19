import argparse
import os
import sys
import time
from typing import Dict, Any
import traceback
import pandas as pd

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
    parser.add_argument('--run-single-cif', type=str,
                        help='Run simulation for a single CIF file (provide path to CIF file')
    
    # Add submit-batch option
    parser.add_argument('--submit-batch', type=int,
                        help='Submit a specific batch ID for processing')
    
    # Add concatenate-results option
    parser.add_argument('--concatenate-results', action='store_true',
                        help='Concatenate CSV results from multiple batches into a single file')
    parser.add_argument('--result-type', 
                        choices=['analysis'],
                        default='analysis',
                        help='Type of results to concatenate (currently only analysis is supported)')
    parser.add_argument('--output-file', type=str,
                        help='Path to the output concatenated CSV file')
    parser.add_argument('--all-batches', action='store_true',
                        help='Concatenate all available batches (if not specified, --min-batch and --max-batch are required)')
    
    # Add analyze-batch option
    parser.add_argument('--analyze-batch', type=int,
                        help='Analyze output for a specific batch ID')
    
    # Add test option
    parser.add_argument('--test', '-t', action='store_true', 
                        help='Run tests for batch results')
    parser.add_argument('--test-batch', type=int,
                        help='Specify a batch ID to test (requires --test)')
    parser.add_argument('--test-json', type=str, default='tests/expected_values.json',
                        help='JSON file with test data (requires --test)')
    parser.add_argument('--test-unittest', action='store_true',
                        help='Use Python unittest framework for tests (requires --test)')
    parser.add_argument('--test-csv', type=str,
                        help='Test against a specific CSV file instead of batch results (requires --test)')
    
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
    
    # Handle testing option
    if args.test:
        # Import test module
        test_module_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tests")
        sys.path.insert(0, test_module_path)
        
        try:
            # Try to import test module
            from tests.test_batch_results import run_unittest_suite, test_all_structures_from_json
            
            # Load and validate configuration
            config_parser = ConfigParser(args.config)
            config = config_parser.get_config()
            
            # Check that JSON file exists
            if not os.path.exists(args.test_json):
                # Try relative to project root
                project_root = os.path.dirname(os.path.dirname(__file__))
                test_json = os.path.join(project_root, args.test_json)
                if not os.path.exists(test_json):
                    # Try other common locations
                    potential_paths = [
                        args.test_json,  # Original path
                        test_json,  # Project root path
                        os.path.join(os.getcwd(), args.test_json),  # Current working directory
                        os.path.join(project_root, "tests", os.path.basename(args.test_json))  # Tests directory
                    ]
                    
                    for path in potential_paths:
                        if os.path.exists(path):
                            args.test_json = path
                            break
                    else:
                        print(f"⚠️ ERROR: Test JSON file not found: {args.test_json}")
                        print(f"Please create it first using the test_batch_results.py script.")
                        sys.exit(1)
                else:
                    args.test_json = test_json
            
            # If batch ID is specified, run only that batch
            if args.test_batch:
                # Import directly from test module
                from tests.test_batch_results import get_batch_results_path, list_structures, create_json_template
                
                batch_results_file = get_batch_results_path(config, args.test_batch)
                if not batch_results_file:
                    sys.exit(1)
                
                print(f"Using batch results file: {batch_results_file}")
                
                # Check if structures exist
                structures = list_structures(batch_results_file)
                if not structures:
                    sys.exit(1)
                
                # Create template if requested
                if not os.path.exists(args.test_json):
                    print(f"Creating test template: {args.test_json}")
                    df = pd.read_csv(batch_results_file)
                    create_json_template(args.test_json, args.test_batch, structures, df.columns)
                    print(f"Please edit {args.test_json} with expected values and run again.")
                    sys.exit(0)
            
                
            # After loading the test module but before running tests
            if args.test_csv:
                
                # complete the path to the csv file
                if not os.path.isabs(args.test_csv):
                    # Try to complete the path
                    args.test_csv = os.path.join(os.path.dirname(os.path.dirname(__file__)), args.test_csv)
                
                print(f"Using direct CSV file for testing: {args.test_csv}")
                
                # Import the necessary function from test_batch_results
                from tests.test_batch_results import test_against_csv_file
                
                    
                print(f"Using CSV file for testing: {args.test_csv}")
                
                # Run tests against the CSV file
                success = test_against_csv_file(args.test_csv, args.test_json)
                sys.exit(0 if success else 1)
                
            # Run tests based on mode
            if args.test_unittest:
                print(f"Running tests with unittest framework using: {args.test_json}")
                success = run_unittest_suite(config, args.test_json)
            else:
                print(f"Running tests manually using: {args.test_json}")
                success = test_all_structures_from_json(config, args.test_json)
            
            sys.exit(0 if success else 1)
            
        except ImportError as e:
            print(f"⚠️ ERROR: Could not import test module: {e}")
            print("Make sure the test_batch_results.py script exists in the tests directory.")
            sys.exit(1)
        except Exception as e:
            print(f"⚠️ ERROR: Test execution failed: {e}")
            if os.environ.get("DEBUG"):
                traceback.print_exc()
            sys.exit(1)
    
    # Handle submit-batch option
    if args.submit_batch is not None:
        print(f"=== Submitting Specific Batch {args.submit_batch} ===")
        
        # Load and validate configuration
        config_parser = ConfigParser(args.config)
        config = config_parser.get_config()
        
        # Set the batch range to target only this specific batch
        batch_range = (args.submit_batch, args.submit_batch)
        print(f"Processing batch: {args.submit_batch}")
        
        # Verify if batch exists
        batch_manager = BatchManager(config)
        if not batch_manager.has_batches() or args.submit_batch > batch_manager.get_num_batches():
            print(f"⚠️ ERROR: Batch {args.submit_batch} does not exist.")
            print("Make sure the batch has been prepared or run with --prepare-only first.")
            return 1
            
        # Ask for confirmation
        if not args.no_confirm:
            prompt = f"Submit batch {args.submit_batch} for processing? [Y/n] "
            if input(prompt).lower() in ['n', 'no']:
                print("Operation cancelled by user.")
                return
        
        # Submit the specific batch - using the normal job tracker approach
        try:
            # Create JobTracker with specific batch range
            tracker = JobTracker(config, batch_range=batch_range)
            
            resubmit_failed = args.resubmit_failed if args.resubmit_failed else False
            
            print(f"Submitting batch {args.submit_batch}...")
                
            tracker.run(polling_interval=args.polling_interval,
                        dry_run=args.dry_run,
                        resubmit_failed=resubmit_failed)
            if not tracker.job_status.empty:
                print(f"Batch {args.submit_batch} submitted successfully")
                return 0
        except Exception as e:
            print(f"⚠️ Error submitting batch: {e}")
            if os.environ.get("DEBUG"):
                traceback.print_exc()
            return 1
    
    # Load and validate configuration
    try:
        print(f"Loading configuration from {args.config}...")
        config_parser = ConfigParser(args.config)
        config = config_parser.get_config()
        
        # Display configuration summary
        display_config_summary(config)
        
        # Handle concatenate results option
        if args.concatenate_results:
            # Import here to avoid circular imports
            from .scripts.concatentate_batch_files import concatenate_csv_files
            
            if not args.output_file:
                print("⚠️ ERROR: --output-file is required with --concatenate-results")
                sys.exit(1)
            print("=== Concatenating Batch Results ===")
            
            # Determine which directory to use based on result type
            if args.result_type == 'analysis':
                if 'output' not in config or 'results_dir' not in config['output'] or not config['output']['results_dir']:
                    print("⚠️ ERROR: Results directory not specified in configuration")
                    sys.exit(1)
                    
                target_dir = config['output']['results_dir']
                if not os.path.isdir(target_dir):
                    print(f"⚠️ ERROR: Results directory not found: {target_dir}")
                    print(f"Make sure simulations have been run and the directory exists")
                    sys.exit(1)
            else:
                print(f"⚠️ ERROR: Result type '{args.result_type}' is not supported")
                sys.exit(1)
            
            print(f"Target directory: {target_dir}")
            
            # Determine batch range
            batch_range = None
            if not args.all_batches:
                if args.min_batch is None or args.max_batch is None:
                    print("⚠️ ERROR: Both --min-batch and --max-batch are required when not using --all-batches")
                    print("          Alternatively, use --all-batches to concatenate all batch files")
                    sys.exit(1)
                
                batch_range = (args.min_batch, args.max_batch)
                print(f"Concatenating batches in range: {batch_range[0]} to {batch_range[1]}")
            else:
                print("Concatenating all available batch files")
                
            # Get batch size for completeness checking
            expected_batch_size = config['batch'].get('size', None) if 'batch' in config else None
                                    
            # Call concatenate function
            success = concatenate_csv_files(
                target_dir,
                batch_range=batch_range,
                output_dir=config['output']['base_dir'],
                output_file_name=args.output_file,
                all_files=args.all_batches,
                result_type=args.result_type,
                expected_batch_size=expected_batch_size,
                verbose=True
            )
            
            return 0 if success else 1
        
        # Handle analyze-batch option
        if args.analyze_batch is not None:
            # Import here to avoid circular imports
            from .scripts.analyze_batch_output import process_batch
            
            print(f"=== Analyzing Batch {args.analyze_batch} ===")
            
            # Determine directories
            if 'output' not in config or 'results_dir' not in config['output'] or not config['output']['results_dir']:
                print("⚠️ ERROR: Results directory not specified in configuration")
                sys.exit(1)
                
            results_dir = config['output']['results_dir']
            batch_dir = f"batch_{args.analyze_batch}"
            input_dir = os.path.join(results_dir, batch_dir, "simulation")
            output_dir = os.path.join(results_dir, batch_dir, "analysis")
            
            # Check if input directory exists
            if not os.path.isdir(input_dir):
                print(f"⚠️ ERROR: Simulation directory not found: {input_dir}")
                print(f"Make sure the batch has been simulated and the directory exists")
                sys.exit(1)
            
            print(f"Simulation directory: {input_dir}")
            print(f"Output directory: {output_dir}")
            
            # Ask for confirmation
            if not args.no_confirm:
                prompt = f"Process analysis for batch {args.analyze_batch}? [Y/n] "
                if input(prompt).lower() in ['n', 'no']:
                    print("Operation cancelled by user.")
                    return
                    
            # Run the analysis
            try:
                success = process_batch(args.analyze_batch, input_dir, output_dir, write_json=True)
                if success:
                    print(f"✅ Analysis for batch {args.analyze_batch} completed successfully")
                    
                    # Report about potential issues detected by the safe_extract_averages function
                    failed_file = os.path.join(output_dir, f"batch_{args.analyze_batch}_failed_files.json")
                    if os.path.exists(failed_file):
                        import json
                        with open(failed_file, 'r') as f:
                            failed_data = json.load(f)
                            print(f"⚠️ {len(failed_data)} structures had issues during analysis.")
                            print(f"   Details saved to: {failed_file}")
                else:
                    print(f"⚠️ Analysis for batch {args.analyze_batch} failed")
                    sys.exit(1)
            except Exception as e:
                print(f"⚠️ Error processing batch {args.analyze_batch}: {e}")
                if os.environ.get("DEBUG"):
                    traceback.print_exc()
                sys.exit(1)
            
            return
        
        # Handle single CIF file run
        if args.run_single_cif:
            # Path auto-completion logic
            cif_path = args.run_single_cif
            
            # If the path doesn't exist as provided, try auto-completing it
            if not os.path.exists(cif_path):
                # Try appending to project directory structure if relative path
                if not os.path.isabs(cif_path):
                    # Check in standard locations
                    potential_paths = [
                        # Direct in output base dir
                        os.path.join(config['output']['base_dir'], cif_path),
                        # In the raw database dir
                        os.path.join(config['database']['path'], os.path.basename(cif_path)),
                        # In examples/data/raw
                        os.path.join(os.path.dirname(os.path.dirname(config['output']['base_dir'])), 
                                    "data/raw", os.path.basename(cif_path)),
                        # In current directory
                        os.path.join(os.getcwd(), cif_path),
                    ]
                    
                    for path in potential_paths:
                        if os.path.exists(path):
                            cif_path = path
                            print(f"Path auto-completed to: {cif_path}")
                            break
            
            if not os.path.exists(cif_path):
                print(f"⚠️ ERROR: CIF file not found: {args.run_single_cif}")
                print("Attempted to find the file in:")
                for path in potential_paths:
                    print(f"  - {path}")
                return
            
            print(f"=== Running simulation for single CIF file: {cif_path} ===")
            
            # Create and initialize JobTracker
            tracker = JobTracker(config)
            
            # Ask for confirmation
            if not args.no_confirm:
                prompt = f"Submit job for CIF file: {os.path.basename(cif_path)}? [Y/n] "
                if input(prompt).lower() in ['n', 'no']:
                    print("Operation cancelled by user.")
                    return
            
            # Run simulation for single CIF file
            try:
                success = tracker.run_single_cif(cif_path, dry_run=args.dry_run)
                if success:
                    print("✅ Job for single CIF file submitted successfully")
                else:
                    print("⚠️ Failed to submit job for single CIF file")
            except Exception as e:
                print(f"⚠️ Error submitting job: {e}")
                if os.environ.get("DEBUG"):
                    traceback.print_exc()
            return
        
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
                
                print("\n=== Updating Workflow Stages ===")
                fixed_count = 0
                
                # We'll use the job scheduler's method for workflow stage detection
                # This avoids hardcoding and handles all cases appropriately
                for idx, row in tracker.job_status.iterrows():
                    batch_id = row['batch_id']
                    job_id = row['job_id']
                    status = row['status']
                    current_workflow_stage = row['workflow_stage'] if not pd.isna(row['workflow_stage']) else ""
                    
                    # Get batch output directory to use for detecting workflow stage
                    batch_output_dir = os.path.join(tracker.results_dir, f'batch_{batch_id}')
                    
                    # Use job scheduler's method to determine current workflow stage
                    # This method already handles all statuses correctly including COMPLETED, FAILED, etc.
                    new_workflow_stage = tracker.job_scheduler._get_current_workflow_stage(batch_id, status)
                    
                    # Only update if different from current value
                    if new_workflow_stage != current_workflow_stage:
                        tracker.job_status.loc[idx, 'workflow_stage'] = new_workflow_stage
                        print(f"Updated workflow stage for batch {batch_id}: {current_workflow_stage or '(empty)'} → {new_workflow_stage}")
                        fixed_count += 1
                
                # Save the job status if we made any changes
                if fixed_count > 0:
                    print(f"Updated workflow stage for {fixed_count} jobs")
                    tracker._save_job_status()
                else:
                    print("No workflow stage updates needed")
                
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
