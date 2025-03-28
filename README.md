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

```bash
# Clone the repository
git clone https://github.com/sbkashif/gRASPA_job_tracker.git
cd gRASPA_job_tracker

# Create a virtual environment
conda create -n graspa_job_tracker python=3.9
conda activate graspa_job_tracker

# You will need install following dependencies separately:
# - gRASPA (https://github.com/snurr-group/gRASPA)
# - PACMOF2 (https://github.com/snurr-group/pacmof2/)

# Install the package
pip install -e .
```

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

See the example configuration file in `examples/config.yaml` for details on available options.

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

## Requirements

- Python 3.6+
- SLURM workload manager
- PyYAML
- pandas
- wget (for downloading databases)
