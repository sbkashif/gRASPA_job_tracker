# gRASPA Job Tracker

A Python package for generating large datasets of results from gRASPA simulations. This package is designed to facilitate the automated submission and tracking of multiple gRASPA jobs, and designing customized workflows from preprocessing to simulation to analysis.

## Features

- Configurable runs via a YAML configuration file
- Divide the database into batches for parallel processing using various strategies
- Customaziable workflow by specifying paths to scripts. The workflow has been tested on a three-step sequential process: generate partial charges, run gRASPA simulations, and obtain adsorbate loadings.
- Automated job submission to SLURM, tracking job status, and, optionally, resubmission of failed jobs.

## Directory Structure

```
gRASPA_job_tracker/
├── gRaspa_job_tracker/     # Python package
|   ├── batch_manager.py
|   ├── cli.py
|   ├── config_parser.py
|   ├── generate_batches.sh
|   ├── __init__.py
|   ├── job_scheduler.py
|   ├── job_tracker.py
|   ├── scripts
|   │   ├── 1-copy_cif_files.sh
|   │   ├── 2-update_unit_cells.sh
|   │   ├── analyze_batch_output.py
|   │   ├── cleanup_results.sh
|   │   ├── generate_partial_charge.py
|   │   ├── mincell.py
|   │   ├── mps_run.sh
|   │   ├── parse_graspa_output.py
|   │   ├── start_as_root.sh
|   │   └── stop_as_root.sh
|   └── utils.py
├── examples/               # Example files
│   ├── config.yaml         # Example configuration
│   ├── {PROJECT_NAME}/     # Example data
│       ├── data/           # Original database files
│           ├── raw/        # Original database files
│           ├── batches/    # Processed results
│           │   ├── batch_1.csv
│           │   ├── batch_2.csv
│           │   └── ...
│           ├── job_logs/
│           ├── job_status.csv # See the [job tracking](#job-tracking) section
│           ├── job_scripts/
│           └── results/
│               ├── batch_1/
│               │   ├── partial_charges/
│               │   ├── simulations/
│               │   └── analysis/
│               ├── batch_2/
│               ├── batch_3/
│               └── ...
├── forcefields/            # forcefield files
│   └── forcefiled_1_dir/
│   ├── forcefiled_2_dir/
├── templates/              # templates for the job submission
│   ├── simulation.input    # grapsa simulation input file
│   └── slurm_template.sh   # slurm job submission template
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
└── setup.py
```

## Installation

### Setting up the environment

The repository includes an `environment.yml` file with all the necessary Python dependencies. Follow these steps to set up your environment:

```bash
# Clone the repository
git clone https://github.com/sbkashif/gRASPA_job_tracker.git
cd gRASPA_job_tracker

# Create and activate the conda environment from the YAML file
conda env create -f environment.yml
conda activate graspa

# Install the package in editable mode
pip install -e .

```
### Required External Dependencies
   - gRASPA (https://github.com/snurr-group/gRASPA)
   - PACMOF2 (https://github.com/snurr-group/pacmof2/) -- Might already be installed via environment.yml. If not, install from source.


## Usage

1. Create a configuration file:
```bash
graspa_job_tracker --create-default-config my_config.yaml
```

2. Edit the configuration file with your specific settings:
   - Configure database source (local path or URL for download)
   - Set batch splitting strategy (alphabetical, size-based, etc.)
   - Configure your SLURM account settings
   - Specify paths to your partial charge, simulation, analysis or any intermediate scripts.
   - Configure forcefield variables (see [Forcefield Configuration](#forcefield-configuration))

3. Run the job tracker:

The recommended way to run the job tracker is in two steps. The first step prepares the batches and checks for any issues before submitting jobs. The second step will submit the jobs. The first step is a one-time operation and you can skip it if you are sure that the batches are already prepared.

```bash
graspa_job_tracker --config my_config.yaml --prepare-only
graspa_job_tracker --config my_config.yaml
```

You can also constrain the batches to be considered for submission:

```bash
graspa_job_tracker --config my_config.yaml --min-batch <BATCH_NUMBER> --max-batch <BATCH_NUMBER>
```

## Batch Splitting Strategies

The package supports multiple strategies for splitting your database into batches:

- **Alphabetical**: Split files based on alphabetical ordering
- **Size-based**: Group files based on their size using configurable thresholds
- **Random**: Randomly assign files to batches
- **custom_alphabetical**: One-time batch splitting based on alphabetical ordering done gRASPA_job_tracker.script.generate_batches earlier. This was done so that remaining batches can be run in this version of the code without needing to re-run all the batches.

## Configuration Options

See the example configuration file in `examples/config-coremof-clean.yaml` for a reference. The configuration file allows you to specify:
- **Database Source**: Local path or URL to download the database
- **Batch Splitting Strategy**: Choose from alphabetical, size-based, random, or custom_alphabetical
- **SLURM Settings**: Account, partition, time limits, and other SLURM parameters
- **Load Dependencies**: Paths to required software dependencies like gRASPA and PACMOF2
- **Script Paths**: Paths to your custom scripts
- **Force field and simulation parameters**: Paths to forcefield files and parameters for gRASPA simulations
- **Job Tracking**: Options for tracking job status and resubmitting failed jobs

## Force field Configuration

The simulation script (`mps_run.sh`) requires specific environment variables to locate and use forcefield files:

### Mandatory Forcefield Files
These files must be provided in your configuration:
```yaml
forcefield_files:
  FORCE_FIELD_MIXING_RULES: "/path/to/forcefields/force_field_mixing_rules.def"
  FORCE_FIELD: "/path/to/forcefields/force_field.def"
  PSEUDO_ATOMS: "/path/to/forcefields/pseudo_atoms.def"
```

### Molecule-Specific Files
You can add any number of additional molecule-specific files as needed:
```yaml
forcefield_files:
  # Mandatory files
  FORCE_FIELD_MIXING_RULES: "/path/to/forcefields/force_field_mixing_rules.def"
  FORCE_FIELD: "/path/to/forcefields/force_field.def"
  PSEUDO_ATOMS: "/path/to/forcefields/pseudo_atoms.def"
  
  # Molecule-specific files (can add as many as needed)
  CO2: "/path/to/forcefields/CO2.def"
  N2: "/path/to/forcefields/N2.def"
  CH4: "/path/to/forcefields/CH4.def"
  H2O: "/path/to/forcefields/H2O.def"
```

All files will be prefixed with `FF_` in the environment variables. The script checks for the mandatory files and will fail if they're missing, while additional molecule-specific files are copied if present.


## Job tracking
```csv
(graspa) [sbinkashif@dt-login01 coremof_clean]$ cat job_status.csv 
batch_id,job_id,status,submission_time,completion_time
401,8605663,COMPLETED,2025-03-27 12:11:03,2025-03-27 19:30:30
402,8605664,COMPLETED,2025-03-27 12:11:03,2025-03-27 19:30:30
..
421,8609242,RUNNING,2025-03-27 19:30:30,
422,8609243,RUNNING,2025-03-27 19:30:30,
423,8609244,RUNNING,2025-03-27 19:30:30,
...
448,8609270,PENDING,2025-03-27 19:30:36,
449,8609271,PENDING,2025-03-27 19:30:37,
450,8609272,PENDING,2025-03-27 19:30:37,
```

## Command Line Options

...

### Job Status Management

```bash
# Update job status without submitting new jobs
python -m gRASPA_job_tracker --update-status

# Update job status for a specific batch range
python -m gRASPA_job_tracker --update-status --batch-range 100-200
```

The `--update-status` option scans all batch directories to update the job status tracking file without submitting any new jobs. This is useful for:

- Recovering tracking information after modifying files manually
- Getting an overview of current job status
- Updating the status of completed jobs in the background

## Creating Custom Scripts

The gRASPA job tracker is designed to be extensible with custom scripts for various stages of the workflow. This section provides guidelines on how to create custom scripts that can work with the configuration system.

### General Principles

Custom scripts specified in the `scripts` section of your configuration file can be either Python modules or shell scripts. The system provides multiple ways to access configuration values:

1. **Command Line Arguments**: Values are passed as positional arguments
2. **Environment Variables**: Values are exported as environment variables 
3. **Template Files**: Some configuration values are processed into template files. For example, temperature and pressure values need to find a way into the `simulation.input` file for gRASPA.

### Custom Python Scripts

When writing a custom Python script for the job tracker:

1. **Script Placement**: Your Python script should be in a module accessible to the system. You can either:
   - Put it directly in the `gRASPA_job_tracker/scripts/` directory
   - Place it anywhere in your Python path and use the full module path in the config
   
2. **Configuration Access Methods**:

   a) **Command Line Arguments**: The system passes these standard arguments to your script:
   ```python
   # Standard argument pattern
   batch_id = sys.argv[1]       # First argument is always the batch ID
   input_file = sys.argv[2]     # Second argument is the input file/directory
   output_dir = sys.argv[3]     # Third argument is the output directory
   template_path = sys.argv[4]  # Fourth argument (optional) is the template path
   ```

   b) **Environment Variables**: Access configuration values through environment variables:
   ```python
   import os
   
   # Access a simulation parameter set in config
   cycles = os.environ.get("SIM_VAR_NumberOfInitializationCycles")
   
   # Access a forcefield file path
   force_field_path = os.environ.get("FF_FORCE_FIELD")
   ```
   
3. **Execution Context**: Your script might be executed with the current working directory set to:
   - The simulation directory (if `change_dir: true` is set in workflow)
   - The original directory (default)

4. **Example Custom Python Script** (supports both argument and environment variable access):
   ```python
   #!/usr/bin/env python
   import os
   import sys
   
   def main():
       # Get values from command line arguments
       batch_id = sys.argv[1]
       input_file = sys.argv[2]
       output_dir = sys.argv[3]
       
       # Alternatively, get values from environment variables
       # batch_id = os.environ.get("BATCH_ID")
       # input_dir = os.environ.get("INPUT_DIR")
       
       # Access simulation parameters from environment variables
       num_init_cycles = os.environ.get("SIM_VAR_NumberOfInitializationCycles")
       
       # Access forcefield files from environment variables
       force_field = os.environ.get("FF_FORCE_FIELD")
       co2_forcefield = os.environ.get("FF_CO2")
       
       print(f"Processing batch {batch_id}")
       print(f"Input file/directory: {input_file}")
       print(f"Output directory: {output_dir}")
       print(f"Using {num_init_cycles} initialization cycles")
       print(f"Force field: {force_field}")
       # Your script logic here
   
   if __name__ == "__main__":
       main()
   ```

### Custom Shell Scripts

For shell scripts:

1. **Script Placement**: Shell scripts should be executable files. They can be:
   - Located in the `gRASPA_job_tracker/scripts/` directory
   - Located anywhere on the system with the full path specified in the config
   
2. **Configuration Access Methods**:

   a) **Command Line Arguments**: The system passes standard arguments to your script:
   ```bash
   #!/bin/bash
   
   # Standard argument pattern
   batch_id=$1       # First argument is always the batch ID
   input_file=$2     # Second argument is the input file/directory
   output_dir=$3     # Third argument is the output directory (optional)
   scripts_dir=$4    # Fourth argument might be scripts_dir (for mps_run.sh)
   ```

   b) **Environment Variables**: Access configuration values directly as environment variables:
   ```bash
   #!/bin/bash
   
   # Access a simulation parameter set in config
   echo "Using ${SIM_VAR_NumberOfInitializationCycles} initialization cycles"
   
   # Access a forcefield file path
   echo "Force field: ${FF_FORCE_FIELD}"
   ```
   
3. **Special Case - Simulation Scripts**: For scripts like `mps_run.sh`, a special execution pattern is used:
   ```bash
   # For simulation scripts (e.g., mps_run.sh)
   bash mps_run.sh ${batch_id} ${input_dir} ${scripts_dir} ${output_dir}
   ```

4. **Example Custom Shell Script**:
   ```bash
   #!/bin/bash
   set -e  # Exit on error
   
   # Access command line arguments
   batch_id=$1
   input_file=$2
   output_dir=${3:-.}  # Use current directory if not specified
   
   echo "Processing batch ${batch_id}"
   echo "Input file/directory: ${input_file}"
   echo "Output directory: ${output_dir}"
   
   # Access simulation parameters from environment variables
   NUM_CYCLES=${SIM_VAR_NumberOfProductionCycles}
   echo "Using ${NUM_CYCLES} production cycles"
   
   # Access forcefield paths from environment variables
   echo "Force field mixing rules: ${FF_FORCE_FIELD_MIXING_RULES}"
   echo "Molecule forcefield: ${FF_CO2}"
   
   # Your script logic here
   # ...
   ```

### Configuration to Script Value Mapping

| Config Section | Access Method | Python Example | Bash Example |
|----------------|---------------|----------------|--------------|
| `forcefield_files` | Environment variables with `FF_` prefix | `os.environ.get("FF_FORCE_FIELD")` | `${FF_FORCE_FIELD}` |
| `run_file_templates.*.variables` | Environment variables with `SIM_VAR_` prefix | `os.environ.get("SIM_VAR_NumberOfInitializationCycles")` | `${SIM_VAR_NumberOfInitializationCycles}` |
| Script arguments | Command line arguments | `batch_id = sys.argv[1]` | `batch_id=$1` |
| Template files | Path in command line args or env variable | `template_path = sys.argv[4]` | `$TEMPLATE_SIMULATION_INPUT` |

Remember that all environment variables are strings. For numerical values, you'll need to convert them to the appropriate type in your scripts.

## Requirements

- Python 3.6+
- SLURM workload manager
- PyYAML
- pandas
- wget (for downloading databases)
