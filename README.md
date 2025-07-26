# gRASPA Job Tracker

A Python package for generating large datasets of results from gRASPA simulations. This package is designed to facilitate the automated submission and tracking of multiple gRASPA jobs, and designing customized workflows from preprocessing to simulation to analysis.

## Features

- Configurable runs via a YAML configuration file
- Divide the database into batches for parallel processing using various strategies
- Customizable workflow by specifying paths to scripts. The workflow has been tested on a three-step sequential process: generate partial charges, run gRASPA simulations, and obtain adsorbate loadings.
- Automated job submission to SLURM, tracking job status, and, optionally, resubmission of failed jobs.
- [NEW] Conduct multi-dimensional parameter sweeps with automated job generation for each parameter combination


## 🚀 NEW RELEASE: Parameter Matrix Support (July 2025)

### Multi-Dimensional Parameter Sweeps
The latest release introduces comprehensive **parameter matrix support** for conducting multi-dimensional parameter sweeps. This powerful feature allows you to:

- **Define parameter ranges** for temperature, pressure, mole fractions, and any custom parameters
- **Generate full factorial designs** automatically (e.g., 3×3×3×3 = 81 parameter combinations) or any custom combination
- **Track progress** of thousands of parameter combinations with [enhanced monitoring](#parameter-matrix-job-tracking)

### Parameter matrix definition in config file
```yaml
parameter_matrix:
  parameters:
    temperature: [298, 313, 333] #K
    pressure: "!eval np.logspace(-6,7,34).tolist()"  # Pa
    co2_molfraction: [0.15, 0.25, 0.35]
    n2_molfraction: [0.85, 0.75, 0.65]
  combinations: 'all'  # Creates 81 parameter combinations per batch
```
---
## Directory Structure

The package uses a structured directory layout to organize scripts, results, and logs. The main directories are:


```
{PROJECT_NAME}/
├── data/
│   ├── raw/                # Original database files
│   ├── batches/            # Batch CSV files (same as standard)
│   │   ├── batch_1.csv     # Contains single CIF file per batch
│   │   ├── batch_2.csv
│   │   └── ...
│   ├── job_logs/           # Individual parameter combination logs
│   │   ├── batch_1_param_0_%j.out
│   │   ├── batch_1_param_1_%j.out
│   │   ├── batch_1_param_2_%j.out
│   │   └── ...
│   ├── job_status.csv      # Enhanced tracking with param_combination_id
│   ├── job_scripts/        # Individual job scripts per parameter combination
│   │   ├── job_batch_1_param_0.sh
│   │   ├── job_batch_1_param_1.sh
│   │   ├── job_batch_1_param_2.sh
│   │   └── ...
│   ├── parameter_matrix.json # Generated parameter combinations
│   └── results/            # Parameter-specific result directories
│       ├── B1_T298_P100000_CO20.15_N20.85/    # Batch 1, Parameter combination 0
│       │   ├── cif_file_list.txt
│       │   ├── simulation/
│       │   │   ├── exit_status.log
│       │   │   └── [simulation results]
│       │   ├── analysis/
│       │   │   ├── exit_status.log
│       │   │   └── [analysis results]
│       │   └── exit_status.log
│       ├── B1_T298_P100000_CO20.15_N20.75/    # Batch 1, Parameter combination 1
│       │   ├── cif_file_list.txt
│       │   ├── simulation/
│       │   └── analysis/
│       ├── B1_T298_P100000_CO20.15_N20.65/    # Batch 1, Parameter combination 2
│       │   └── ...
│       ├── B2_T298_P100000_CO20.15_N20.85/    # Batch 2, Parameter combination 0
│       │   └── ...
│       └── ...                                # Up to thousands of parameter combinations
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

Intial template to work upon can be generated from the following command:

```bash
graspa_job_tracker --create-default-config my_config.yaml
```

2. Edit the configuration file with your specific settings:
   - Configure database source (local path or URL for download)
   - Set batch splitting strategy (alphabetical, size-based, etc.)
   - Configure your SLURM account settings
   - Specify paths to your custom scripts. For example, calculating partial charge, running graspa simulation, and analyzing results.
   - Specify forecfield files and template input files required by custom script. For example, `simulation.input` file template required by gRASPA.

3. Run the job tracker:

The recommended way to run the job tracker is in two steps. The first step is a one-time operation to create batches.Once the first step concludes, make sure that the batches are prepared correctly like achieving target size, sorting, and paths specified to the CIF files. If things look correct, run the second step to submit the jobs. The jobs will run the custom scripts defined in the configuration file for each batch.

```bash
#Step 1
graspa_job_tracker --config my_config.yaml --prepare-only
#Step 2
graspa_job_tracker --config my_config.yaml
```

### Advanced Usage Options

There are many different ways in which Step 2 can be run. You can also constrain the batches to be considered for submission:

```bash
graspa_job_tracker --config my_config.yaml --min-batch <BATCH_NUMBER> --max-batch <BATCH_NUMBER>
```
Or, run a specific batch:

```bash
graspa_job_tracker --config my_config.yaml --submit-batch <BATCH_NUMBER>
```

Or, if you have only a few files which doesn't need preparing batches you can run individual files:

```bash
graspa_job_tracker --config my_config.yaml --run-single-cif <PATH_TO_CIF_FILE>
#This functionality is not yet tested
```   

## Parameter Matrix Configuration

The parameter matrix feature allows you to conduct comprehensive parameter sweeps by defining ranges for multiple parameters. Each combination of parameters will be run as a separate SLURM job, providing optimal parallelization and resource utilization.

### Basic Configuration

Add a `parameter_matrix` section to your configuration file:

```yaml
parameter_matrix:
  # Define parameter ranges
  parameters:
    temperature: [298, 313, 333]  # K
    pressure: [100000, 200000, 500000]  # Pa
    co2_molfraction: [0.15, 0.25, 0.35]  # CO2 mole fraction
    n2_molfraction: [0.85, 0.75, 0.65]   # N2 mole fraction
  
  # How to combine parameters
  combinations: 'all'  # Creates 3×3×3×3 = 81 parameter combinations per batch
```

### Parameter Naming Convention

The system uses a consistent naming convention for parameter combinations:
- `T298_P100000_CO20.15_N20.85` for temperature=298K, pressure=100000Pa, CO2=0.15, N2=0.85
- Parameters are automatically abbreviated: `T` (temperature), `P` (pressure), `CO2` (CO2 mole fraction), `N2` (N2 mole fraction)

### Directory Structure

Each parameter combination gets its own directory structure:
```
results/
├── B1_T298_P100000_CO20.15_N20.85/
│   ├── simulation/
│   └── analysis/
├── B1_T298_P100000_CO20.15_N20.75/
│   ├── simulation/
│   └── analysis/
└── ...
```

### Job Script Generation

- Individual SLURM job scripts are generated for each parameter combination
- Each job script includes parameter-specific environment variables
- Template files are automatically updated with parameter values
- Job scripts are named: `job_batch_{batch_id}_param_{param_id}.sh`

### Scaling Example

For a configuration with:
- 24 batches (MOF structures)
- 81 parameter combinations (3×3×3×3)
- Total jobs: 24 × 81 = **1,944 individual SLURM jobs**

Each job processes one MOF structure with one parameter combination, providing maximum parallelization.

## Batch Splitting Strategies

The package supports multiple strategies for splitting your database into batches:

- **Alphabetical**: Split files based on alphabetical ordering
- **Size-based**: Group files based on their size using configurable thresholds
- **Random**: Randomly assign files to batches
- **custom_alphabetical**: Following a specific alphabetical ordering implemented in gRASPA\_job\_tracker.script.generate\_batches earlier befor creating this package. We are keeping this as an option since half of the batches reported in the first publication were created using this `generate_batches.sh` script. This is useful for reproducibility and consistency with previous results. For a new simulation, users can just pick `Alphabetical` keyword for batch splitting the CIF files in alphabetical order.

## Configuration Options

See the example configuration file in `examples/config-coremof-clean.yaml` for a standard reference, or `examples/config-parameter-matrix.yaml` for parameter matrix configuration. The configuration file allows you to specify:
- **Database Source**: Local path or URL to download the database
- **Batch Splitting Strategy**: custom\_alphabetical, alphabetical, size-based, or random
- **Parameter Matrix**: Multi-dimensional parameter sweep configuration (optional)
- **SLURM Settings**: Account, partition, time limits, and other SLURM parameters as per NCSA cluster
- **Load Dependencies**: Paths to required software dependencies like gRASPA and PACMOF2
- **Script Paths**: Paths to your custom scripts: partial charge calculation, gRASPA simulation, analysis to parse adsorbate loadings
- **Force field and simulation parameters**: Paths to forcefield files and template `simulation.input` file for gRASPA simulations, alongside modifications made to simulation parameters in the current run.

## Force field and simulation paramters

The simulation script for gRASPA simulations (`mps_run.sh`) requires specific environment variables to locate and use forcefield files:

### Mandatory Forcefield Files
These files and variables must be provided in your configuration as it is a gRASPA compulsion:
```yaml
forcefield_files:
  FORCE_FIELD_MIXING_RULES: "/path/to/forcefields/force_field_mixing_rules.def"
  FORCE_FIELD: "/path/to/forcefields/force_field.def"
  PSEUDO_ATOMS: "/path/to/forcefields/pseudo_atoms.def"
```

### Molecule-Specific Files
You can add any number of additional molecule-specific files as needed:
```yaml
  # Molecule-specific files (can add as many as needed)
  CO2: "/path/to/forcefields/CO2.def"
  N2: "/path/to/forcefields/N2.def"
  CH4: "/path/to/forcefields/CH4.def"
  H2O: "/path/to/forcefields/H2O.def"
```

All files will be prefixed with `FF_` in the environment variables during processing. The script checks for the mandatory files and will fail if they're missing, while additional molecule-specific files are copied if present.


## Job tracking

The status on jobs running for individual batches can be tracked in `jobs_status.csv` file which is generated in the ${PROJECT\_ROOT} directory defined in the config file.

### Parameter Matrix Job Tracking

For parameter matrix runs, each parameter combination is tracked individually with a unique `param_combination_id`:

```csv
batch_id,job_id,param_combination_id,status,submission_time,completion_time,workflow_stage
1,12345,B1_T298_P100000_CO20.15_N20.85,RUNNING,2025-07-17 10:00:00,,simulation
1,12346,B1_T298_P100000_CO20.15_N20.75,COMPLETED,2025-07-17 10:00:00,2025-07-17 12:00:00,completed
1,12347,B1_T298_P100000_CO20.15_N20.65,PENDING,2025-07-17 10:00:00,,pending
```

### Standard Job Tracking

For standard (non-parameter matrix) runs, the tracking format remains the same:

```csv
batch_id,job_id,status,submission_time,completion_time,workflow_stage
99,10766406,RUNNING,2025-06-23 03:58:09,,simulation (running)
..
401,8605663,COMPLETED,2025-03-27 17:11:03,2025-03-27 19:30:30,completed
402,8605664,COMPLETED,2025-03-27 17:11:03,2025-03-27 19:30:30,completed
..
568,10766205,PARTIALLY_COMPLETE,2025-06-23 03:06:01,2025-06-23 03:07:01,partially_complete (completed: simulation)
```


### Runtime updates in tracker file

```bash
# Update job status without submitting new jobs
python -m gRASPA_job_tracker --update-status

# Update job status for a specific batch range
python -m gRASPA_job_tracker --update-status --batch-range 100-200
```

The `--update-status` option scans all batch directories to update the job status tracking file without submitting any new jobs. This is useful for checking the status of existing jobs if you have exited from the base command: `graspa_job_tracker --config my_config.yaml` or related command used for job submission. If you don't exit the base command after all the batches have been successfully submitted,it will update the `job_status.csv` file every 60 seconds.

## Creating Custom Scripts

The gRASPA job tracker is designed to be extensible with custom scripts for various stages of the workflow. The job scheduler will generate the SLURM job script and call your script with a standard set of arguments and environment variables. 

An example simulation output is discussed [here](examples/data/coremof_clean/README.md).

### General Principles

Custom scripts specified in the `scripts` section of your configuration file can be either Python modules or shell scripts. The system provides multiple ways to access configuration values defined in the [config](examples/config-coremof-clean.yaml) file:

1. **Environment Variables**: Values are exported as environment variables (available to all steps).
2. **Template Files**: Some configuration values are processed into template files if those values are to be controlled by a tool-specific file. For example, gRASPA software reads simulation paramter via a `simulation.input` file.

### Script Argument Convention

The argument convention is enforced from the source code for all custom scripts and the users must follow the convention. The order and meaning of the arguments is described below:

- **First Argument** `batch_id` (string or integer identifying the batch)
- **Second Argument:** Input directory or file list
  - **First Step in `scripts` section:** File list of CIF files in the batch. This is just an extra precaution to avoid processing unnecessary file present in the original database directory. The package will automatically create this file which will essentially be a list of CIF files of a batch. The package will also take care of passing the file path as the second argument of the first script. The users would just need to ensure that that they are not trying to access anything else in the second argument.
  - **Subsequent Steps:** Output directory of the previous step which contains all the output files from the previous step. For example, output directory of partial charge calculation can be used in the gRASPA simulation step.
- **Third Argument:** Output directory (where your script should write results)
- **Fourth Argument (Optional):** Template path (if defined in `run_file_templates` corresponding to a given step in `scripts`). For example, if `<stepname>_input` is defined in `run_file_templates`, then a fourth argument will be passed to your script. The argument with be the path to the template file. The script can either use this path directly by accessing fourth argument or access it via the environment variable `TEMPLATE_<STEP_NAME>_INPUT`.

A future update will cover source code modification to allow additional argumnets to the custom `scripts`.

### Accessing Configuration Values

- **Environment Variables**: All configuration values (forcefields, simulation parameters, etc.) are available as environment variables for all steps.
  - Forcefield files: `FF_<NAME>`
  - Simulation parameters defined in `run_file_templates`: `SIM_VAR_<NAME>`
  - Template files: `TEMPLATE_<STEP_NAME>_INPUT`
- **Example (Python):**
  ```python
  import os
  cycles = os.environ.get("SIM_VAR_NumberOfInitializationCycles")
  force_field = os.environ.get("FF_FORCE_FIELD")
  ```
- **Example (Bash):**
  ```bash
  echo "Force field: ${FF_FORCE_FIELD}"
  echo "Cycles: ${SIM_VAR_NumberOfInitializationCycles}"
  ```

### Working Directory

- By default, python scripts are run from their original location.
- For shell scripts and any step with `change_dir: true`, the script is copied to and run from its output directory. For example, `mps_run.sh` is copied to `simulation` folder and run from there for the setup in [this config file](examples/config-coremof-clean.yaml)

### Exit Status

- Your script should exit with code `0` on success.
- Write an `exit_status.log` file in the output directory.
- Exit status is used to determine if the job was successful or failed. If your script fails, it should exit with a non-zero code, and the job tracker will mark the job as failed.

### Example Script Skeletons

**Python:**
```python
import sys, os
batch_id = sys.argv[1]
input_path = sys.argv[2]
output_dir = sys.argv[3]
# Optionally: template_path = sys.argv[4]. It is availalable as an environment variable as well with format $TEMPLATE_<STEP_NAME>_INPUT

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
```

**Bash:**
```bash
#!/bin/bash
batch_id=$1
input_path=$2
output_dir=$3
template_path=$4. #It is availalable as an environment variable as well with format $TEMPLATE_<STEP_NAME>_INPUT

echo "Processing batch ${batch_id}"
echo "Input file/directory: ${input_file}"
echo "Output directory: ${output_dir}"
echo "Template file path: ${template_path}"

# Access simulation parameters from environment variables
NUM_CYCLES=${SIM_VAR_NumberOfProductionCycles}
echo "Using ${NUM_CYCLES} production cycles"

# Access forcefield paths from environment variables
echo "Force field mixing rules: ${FF_FORCE_FIELD_MIXING_RULES}"
echo "Molecule forcefield: ${FF_CO2}"

# Your script logic here
# ...
```

### Crux:gRASPA simulation script (`mps_run.sh`)
- Receives: `batch_id input_dir output_dir`
- Always run from its output directory
- Expects environment variables for forcefields, simulation parameters, and template files
- Writes `exit_status.log` for job tracking
- Copies required auxiliary scripts like `start_as_root`,`min_cells.py` from `scripts_dir`

### Best Practices
- Use the standard argument order for all scripts.
- Use environment variables for all configuration and parameter values.
- Write results and status files to the output directory provided.
- For maximum compatibility, document in your script what type of input it expects (file or directory).
