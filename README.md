# GRASPA Job Tracker

A Python package for automating SLURM job submissions for GRASPA simulations with CIF files.

## Features

- Divide CIF files into batches for parallel processing
- Submit SLURM jobs to process each batch with automatic job management
- Two-step workflow: generate partial charges, then run GRASPA simulations
- Track job completion and submit new jobs as previous ones finish
- Handle failed jobs and resubmit them
- Configurable via a YAML configuration file

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/graspa-job-tracker.git
cd graspa-job-tracker

# Install the package
pip install -e .
```

## Usage

1. Create a configuration file:
```bash
graspa-job-tracker --create-default-config my_config.yaml
```

2. Edit the configuration file with your specific settings:
   - Set the database path containing CIF files
   - Configure your SLURM account settings
   - Specify paths to your partial charge and simulation scripts

3. Run the job tracker:
```bash
graspa-job-tracker --config my_config.yaml
```

## Configuration Options

See the example configuration file in `examples/config.yaml` for details on available options.

## Requirements

- Python 3.6+
- SLURM workload manager
- PyYAML
- pandas
