# gRASPA Job Tracker Update Notes

## 2025-03-28: Enhanced Job Duplicate Detection and Cancellation

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

## 2025-03-28: Step Completion Check and Workflow Dependency System

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