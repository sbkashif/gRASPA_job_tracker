# gRASPA Job Tracker - Update Notes

## July 17, 2025: Parameter Matrix Support for Multi-Dimensional Parameter Sweeps

### New Features: Parameter Matrix System

- **Parameter Matrix Configuration**: Added comprehensive support for multi-dimensional parameter sweeps through the new `parameter_matrix` configuration section
  - Define parameter ranges for temperature, pressure, mole fractions, and any custom parameters
  - Support for "all combinations" mode generating full factorial designs (e.g., 3×3×3×3 = 81 combinations)
  - Future support planned for custom parameter combinations

- **Individual Job Submission Architecture**: 
  - Each parameter combination now gets its own dedicated SLURM job for optimal resource utilization
  - Eliminates the previous multi-node job submission approach that was problematic for SLURM schedulers
  - Each job handles a single parameter combination for a single CIF file, providing maximum parallelization

- **Flexible Parameter Tracking System**:
  - Dynamic CSV tracking with `param_combination_id` column for easy identification
  - Consistent parameter naming convention: `T298_P100000_CO20.15_N20.85`
  - Batch-specific combination IDs: `B{batch_id}_{param_combination}`
  - Flexible column structure that adapts to different parameter sets

- **Enhanced Directory Structure**:
  - Parameter-specific output directories: `results/B1_T298_P100000_CO20.15_N20.85/`
  - Individual job scripts per parameter combination: `job_scripts/job_batch_1_param_0.sh`
  - Separate log files for each parameter job: `job_logs/batch_1_param_0_%j.out`

### Technical Implementation

- **Parameter Matrix Module**: New `parameter_matrix.py` module handles:
  - Parameter combination generation using `itertools.product`
  - Consistent parameter naming with intelligent abbreviations
  - Sub-job output directory management
  - Parameter-specific template file generation

- **Job Scheduler Enhancements**:
  - Updated `job_scheduler.py` to handle parameter matrix job creation
  - Individual job script generation for each parameter combination
  - Template substitution with parameter-specific values
  - Environment variable exports for all parameter values

- **Job Tracker Updates**:
  - Modified `job_tracker.py` to support parameter combination tracking
  - Updated status tracking methods to use `param_combination_id`
  - Enhanced job submission loop for parameter matrix jobs
  - Improved error handling for parameter-specific failures

### Configuration Example

```yaml
parameter_matrix:
  parameters:
    temperature: [298, 313, 333]  # K
    pressure: [100000, 200000, 500000]  # Pa
    co2_molfraction: [0.15, 0.25, 0.35]  # CO2 mole fraction
    n2_molfraction: [0.85, 0.75, 0.65]   # N2 mole fraction
  
  combinations: 'all'  # Creates 3×3×3×3 = 81 parameter combinations per batch
```

### Usage Examples

Run parameter matrix simulation:
```bash
gRASPA_job_tracker --config config-parameter-matrix.yaml
```

Test parameter matrix with dry run:
```bash
gRASPA_job_tracker --config config-parameter-matrix.yaml --dry-run
```

### Benefits

1. **Scalability**: Handles thousands of parameter combinations efficiently (24 batches × 81 combinations = 1,944 jobs)
2. **Resource Optimization**: Each job uses exactly the resources needed for one parameter combination
3. **Fault Tolerance**: Individual parameter combination failures don't affect other combinations
4. **Flexibility**: Easy to add new parameters or modify parameter ranges
5. **Tracking**: Comprehensive tracking of individual parameter combination progress

### Compatibility

- Fully backward compatible with existing single-parameter job configurations
- Existing batch processing workflows continue to work unchanged
- Parameter matrix is an optional feature that can be enabled/disabled per configuration

## April 2, 2025: Comprehensive Testing Framework for Simulation Results

### Added Testing Framework

- **Complete Testing System**: Implemented a comprehensive testing framework for validating simulation results
  - Tests can be run interactively, via JSON templates, or through automated unittest integration
  - Multiple interfaces available: direct script, CLI integration, and Python unittest framework
  - Support for both batch-specific testing and consolidated CSV file testing

- **Interactive Testing Mode**:
  - Added ability to test results interactively with immediate feedback
  - Supports listing available batches and selecting structures through the CLI
  - Color-coded output for clear pass/fail identification

- **JSON-Based Testing**:
  - Support for creating and using JSON templates of expected values
  - Enables reproducible testing across different environments and runs
  - JSON templates can span multiple batches and structures for comprehensive testing

- **Unittest Integration**:
  - Full Python unittest framework integration for CI/CD pipelines
  - Automatically generates test cases from JSON templates
  - Supports standard unittest discovery protocols

- **Combined CSV Testing Support**: 
  - Added ability to test against consolidated CSV files instead of batch-specific result files
  - New `--test-csv` parameter for specifying any CSV file containing simulation results
  - Tests work identically regardless of whether structures are in batch directories or consolidated files

- **CLI Integration**:
  - Added `--test` command to main CLI tool for running tests directly
  - Supports multiple testing modes with options for specific batches and custom JSON files
  - Integrates all testing capabilities into the main tool workflow

### Technical Implementation

- **Core Testing Module**:
  - Created `test_batch_results.py` with core testing functionality
  - Implements configurable tolerance for numeric comparisons (default: 1%)
  - Supports exclusion of calculated columns (like selectivity) from testing

- **Unittest Integration**:
  - Added `test_batch_results_cli.py` for unittest framework integration
  - Dynamic test method generation based on expected values JSON
  - Compatible with standard unittest discovery protocols (`python -m unittest discover`)

- **Flexible Structure Finding**:
  - Enhanced structure lookup to find test candidates across different result formats
  - Works with standard batch result directories, consolidated CSV files, and varying naming conventions
  - Intelligent path normalization to handle file paths, bare structure names, and suffixes

- **Comprehensive Results Reporting**:
  - Added detailed test summary showing pass/fail status for each structure
  - Shows percentage of successful tests and identifies specific failed comparisons
  - Color-coded output makes successful and failed tests easy to identify

### Usage Examples

Create a test JSON template:
```bash
gRASPA_job_tracker -c config.yaml --test --test-batch 524 --test-json tests/expected_values.json
```

## March 31, 2025: Fixed Exit Status Handling in MPS Run Scripts

### Issue Fixed

Fixed a critical issue where job scripts weren't correctly capturing and preserving the exit status of simulation runs. This occurred specifically with the `mps_run.sh` script, which correctly detected and reported simulation failures but the status wasn't being properly captured by the job scheduler.

### Key Improvements

- **Immediate Exit Status Capture**: Modified the job script generator to immediately capture the simulation exit status before any other commands can modify it
- **Status Preservation**: Added dedicated storage of the simulation status in a variable that persists through the entire script
- **Accurate Failure Detection**: The job scheduler now correctly captures cases where simulations fail due to missing exit_status.log files or other errors
- **Proper Error Propagation**: The exit status from the simulation is now properly propagated to the overall job status

### Technical Implementation

The following changes were made to improve exit status handling:

1. In `_generate_bash_step()`, immediately capture the exit status after running mps_run.sh:
   ```bash
   content += f"bash {script_to_run} {batch_id} {input_file} {scripts_dir} .\n"
   content += f"simulation_status=$?\n"
   content += f"# Write exit status to log file immediately\n"
   content += f"echo $simulation_status > exit_status.log\n"
   ```

2. In `_generate_workflow_steps()`, use the stored simulation_status variable:
   ```bash
   if step_name == 'simulation' or 'mps_run' in script_path:
       steps_content += f"    # For simulation steps, use the existing simulation_status variable\n"
       steps_content += f"    {step_var_name}=$simulation_status\n"
   ```

3. Avoid overwriting the simulation exit status when writing the job's overall exit status

These changes ensure that when simulations fail due to missing logs or other errors, the job is properly marked as failed in the job tracker system.

## March 31, 2025: Fixed Missing "fi" Statements in Generated Job Scripts

### Issue Fixed

Fixed a critical issue in the job script generator where the generated shell scripts had missing `fi` statements to close `if` blocks. This syntax error caused job scripts to fail with:

```
/var/spool/slurmd/job8759973/slurm_script: line 166: syntax error: unexpected end of file
```

### Technical Implementation

Added proper closure of `if` blocks in the `_generate_workflow_steps()` method:

```python
# Added missing closing statement at the end of each workflow step
steps_content += "fi\n"
```

This ensures each workflow step's `if/else` block is properly closed with a matching `fi` statement, preventing shell syntax errors.

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

