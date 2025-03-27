# gRASPA Job Tracker

A Python package for automating SLURM job submissions for GRASPA simulations with CIF files.

## Features

- Divide CIF files into batches for parallel processing using configurable strategies
- Download and prepare databases automatically with wget
- Submit SLURM jobs to process each batch with automatic job management
- Customaziable workflow by specifying paths to scripts
- Tested on a three-step workflow: generate partial charges, run GRASPA simulations, and analyze results
- Track job completion and submit new jobs as previous ones finish
- [Optional] Resubmit them
- Configurable runs via a YAML configuration file

## Directory Structure

```
gRASPA_job_tracker/
├── graspa_job_tracker/     # Python package
│   ├── __init__.py
│   ├── cli.py              # Command-line interface
│   ├── configuration.py    # Configuration handling
│   ├── database.py         # Database management
│   ├── job.py              # Job submission logic
│   ├── main.py             # Main program execution
│   ├── batch_splitter.py   # Batch splitting functionality
│   └── utils.py            # Utility functions
├── examples/               # Example files
│   ├── config.yaml         # Example configuration
│   └── data/               # Example data
│       ├── raw/            # Original database files
│       └── processed/      # Processed results
│           ├── batch_001/
│           └── ...
├── forcefields/            # forcefield files
│   └── forcefiled_1_dir/
│   ├── forcefiled_2_dir/
├── templates/              # templates for the job submission
│   ├── simulation.input    # grapsa simulation input file
│   ├── slurm_template.sh   # slurm job submission template
| ...
```

## Installation

```bash
# Clone the repository
git clone https://github.com/sbkashif/gRASPA_job_tracker.git
cd gRASPA_job_tracker

# Create a virtual environment
conda create -n graspa_job_tracker python=3.9
conda activate graspa_job_tracker

# Install dependencies
<TBA -- PACMOF2>

# Install the package
pip install -e .
```

You would also need to have the `gRASPA` package installed in your environment. You can install it from the following repository: 

`https://github.com/snurr-group/gRASPA`

## Usage

1. Create a configuration file:
```bash
graspa_job_tracker --create-default-config my_config.yaml
```

2. Edit the configuration file with your specific settings:
   - Configure database source (local path or URL for download)
   - Set batch splitting strategy (alphabetical, size-based, etc.)
   - Configure your SLURM account settings
   - Specify paths to your partial charge, simulation, and analysis scripts

3. Run the job tracker:
```bash
graspa_job_tracker --config my_config.yaml
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

## Requirements

- Python 3.6+
- SLURM workload manager
- PyYAML
- pandas
- wget (for downloading databases)
