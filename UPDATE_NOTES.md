# gRASPA Job Tracker - Update Notes

## March 31, 2025: Improved Batch Analysis and Structure ID Handling

### Key Improvements

#### Robust Output File Processing
- Fixed critical issue in `analyze_batch_output.py` where "-nan" values in RASPA output files caused structure processing to fail
- Added intelligent file content sanitization to convert problematic values like "-nan" to properly handled numeric values
- Implemented multi-layer fallback system that ensures analysis can be completed even with problematic output files

#### Enhanced Structure ID Normalization
- Fixed issue in `concatentate_batch_files.py` where structures with "_pacmof" suffix weren't properly matched with their original IDs
- Added path normalization to handle cases where full file paths are compared with bare structure names
- Improved basename extraction to ensure consistent structure identification across workflow steps

#### New Command Line Features
- Added new `--analyze-batch` CLI option to reprocess analysis for specific batches from command line
- Enhanced error reporting for failed files with detailed JSON error logs
- Added support for displaying meaningful values (like "Inf" and "NaN") for special cases in selectivity calculations

### Implementation Details

#### In analyze_batch_output.py:
- Added `safe_extract_averages()` wrapper function that sanitizes problematic RASPA output before processing
- Implemented automatic replacement of "-nan", "nan" and "-" values with numeric zeros
- Added comprehensive error handling with detailed logging of problematic structures
- Fixed selectivity calculations to properly handle division by zero cases

#### In concatentate_batch_files.py:
- Enhanced `normalize_structure_id()` function to handle path information and file extensions
- Added proper handling of "_pacmof" suffix to ensure consistent structure identification 
- Improved output reporting to show only basenames in console output for better readability

#### In cli.py:
- Added new `--analyze-batch` argument for rerunning analysis on specific batches
- Implemented proper directory resolution and error handling for batch analysis

### Usage Examples

Run analysis for a specific batch:
```bash
gRASPA_job_tracker -c config.yaml --analyze-batch 469
```

This update significantly improves the reliability of the batch analysis process, especially for structures that produce non-standard output values. It ensures that all structures with valid simulation results are included in the analysis, even when they contain problematic numeric representations.

## March 30, 2025: Workflow Stage Tracking

### Overview

This update enhances job status tracking by adding a `workflow_stage` column to the job status CSV file. This allows you to see which specific stage of the workflow pipeline each job is currently in or where it failed.

### Changes

#### 1. Enhanced Job Status CSV Format

The job status CSV now includes a new column:

```
batch_id,job_id,status,submission_time,completion_time,workflow_stage
```

The `workflow_stage` column indicates:
- For running jobs: The specific workflow step currently executing (e.g., "partial_charge", "simulation", "analysis")
- For completed jobs: Simply "completed"
- For failed jobs: Which stage it failed at (e.g., "simulation (failed)")
- For pending jobs: "pending"

#### 2. Added Workflow Stage Tracking Logic

- Added the `_get_current_workflow_stage()` method that examines output directories to determine the current stage
- Updated the `update_job_status_csv()` method to include workflow stage information
- Added special handling for single CIF file runs vs. batch runs

#### 3. Migration Script

Added `migrate_job_status.py` to update existing job status CSV files to the new format:
- Creates a backup of the original file
- Adds the workflow_stage column
- Intelligently sets appropriate workflow stage values based on job status

### How to Migrate Existing Job Status Files

Run the migration script:

```bash
cd /projects/bcvz/sbinkashif/gRASPA_job_tracker
python -m gRASPA_job_tracker.migrate_job_status
```

Or with a specific path:

```bash
python -m gRASPA_job_tracker.migrate_job_status /path/to/job_status.csv
```

### Benefits

1. **Better Debugging**: Quickly identify which stage of the workflow pipeline failed
2. **Progress Tracking**: See which stage each running job is currently in
3. **Completion Status**: Clear indication of jobs that have completed the entire workflow
4. **Unified Tracking**: Works for both batch processing and single-file processing

### Example

Before:
```
batch_id,job_id,status,submission_time,completion_time
101,12345,FAILED,2025-03-27 17:11:03,2025-03-27 19:30:30.113957167
```

After:
```
batch_id,job_id,status,submission_time,completion_time,workflow_stage
101,12345,FAILED,2025-03-27 17:11:03,2025-03-27 19:30:30.113957167,simulation (failed)
```

This shows that job 101 failed specifically during the simulation stage.

## March 28, 2025: Step Completion Check and Workflow Dependency System

### Added Features

- **Step Completion Detection**: The scheduler now intelligently detects if a step has already been completed, is in progress, or failed, allowing for efficient reruns and recovery.
  
- **Dependency Management System**: Added a robust dependency tracking system that enables the workflow to understand relationships between steps.
  
- **Smart Workflow Execution**: When rerunning workflows:
  - Successfully completed steps are skipped
  - Failed required steps are retried with previous output backed up
  - Failed optional steps are skipped
  - Interrupted steps are properly recovered

- **Status Tracking and Visualization**:
  - Added timestamp and emoji indicators for clear visual status
  - Improved logging with detailed status messages
  - Clear indications when steps are retried or skipped
  
- **Backup System for Failed Attempts**: Previous failed attempts are automatically backed up with timestamps before rerunning, preserving historical data for debugging.

### Technical Implementation Details

- Added `check_step_completion()` function to examine if a step's `exit_status.log` exists with success code
- Added `check_dependencies()` function to validate if required predecessor steps completed successfully
- Enhanced workflow tracking with different handling for new, interrupted, failed or completed steps
- Added backup functionality for preserving data from failed runs
- Implemented smarter status reporting with timestamps and visual indicators

### Usage Notes

This system allows for more efficient resource utilization by:

1. Only running steps that need to be executed
2. Automatically recovering from failures in multi-step workflows
3. Preserving data from previous attempts for debugging purposes
4. Providing clear visual indicators of workflow progress

No configuration changes are needed to use these features - they are automatically applied to all workflows.

## March 28, 2025: Enhanced Job Duplicate Detection and Cancellation

### Added Features

- **Intelligent Duplicate Job Management**:
  - Added capability to detect and cancel duplicate SLURM jobs for the same batch
  - Smart prioritization of jobs based on status and submission time
  - Automated cleanup of the job queue to prevent resource waste

- **Prioritization Logic**:
  - PENDING jobs are prioritized over RUNNING jobs
  - Among multiple PENDING jobs, the oldest submission is kept
  - Among multiple RUNNING jobs, the newest submission is kept
  - All other duplicate jobs are automatically cancelled in SLURM

- **Job Status Update Utility**:
  - Added new command-line option `--update-status` to scan all batches and update job status without submitting new jobs
  - Provides a comprehensive status summary of all tracked batches

### Technical Implementation Details

- Enhanced `clean_job_status()` function to actively cancel duplicate jobs using SLURM's `scancel` command
- Implemented intelligence-based job selection to minimize workflow disruption
- Added detailed logging of duplicate detection and resolution actions

## March 28, 2025: Added Single CIF File Functionality

### New Features

#### Run Single CIF File
- Added a new command line option `--run-single-cif` that allows running a simulation for a single CIF file
- The option takes a file path to a CIF file as its argument
- Results are stored in a structured directory format within the configured output directory

#### Directory Structure for Single CIF Files
- Single CIF file results are stored in: `<base_output_dir>/singles/<structure_name>/`
- Within each structure directory:
  - `scripts/`: Contains the job submission script
  - `results/`: Contains the simulation results

#### Job Tracking
- Single CIF jobs are tracked in the same job status file as batch jobs
- Single CIF jobs are assigned batch IDs in the format: `single_<structure_name>`

### Usage Examples

Run a simulation for a specific CIF file:
```bash
gRASPA_job_tracker -c config.yaml --run-single-cif path/to/structure.cif
```