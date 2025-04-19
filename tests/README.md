# gRASPA Job Tracker - Testing Utilities

This directory contains tools for testing and validating gRASPA simulation results.

## Manual and Automated Testing Options

The testing utilities provide two main approaches to testing:

1. **Interactive/Manual Testing**: For ad-hoc testing and quick comparisons
2. **Automated Unit Testing**: For integration with CI/CD and more standardized testing

## Manual Batch Results Testing

The `test_batch_results.py` script allows you to manually verify gRASPA simulation results against 
expected values. This is useful for:

- Verifying results after making changes to the codebase
- Cross-checking results from different environments
- Manual validation of structure properties

### Manual Usage

#### Interactive Mode

```bash
# List all available batches
python test_batch_results.py --config ../examples/config-coremof-clean.yaml --list-batches

# Test a specific structure interactively
python test_batch_results.py --config ../examples/config-coremof-clean.yaml --batch 524 --structure XIWKUJ_clean_pacmof

# Select batch and structure interactively
python test_batch_results.py --config ../examples/config-coremof-clean.yaml
```

#### JSON Mode (Recommended for Reproducible Testing)

```bash
# Create a JSON template with all structures in a batch
python test_batch_results.py --config ../examples/config-coremof-clean.yaml --batch 524 --create-template expected_values.json

# Edit the JSON file with your expected values
# Then run the test using the JSON file (will test all structures in all batches)
python test_batch_results.py --config ../examples/config-coremof-clean.yaml --json expected_values.json

# Test a specific structure using values from the JSON file
python test_batch_results.py --config ../examples/config-coremof-clean.yaml --batch 524 --structure XISLAM_clean_pacmof --json expected_values.json
```

## Automated Unit Testing

There are multiple ways to run automated unit tests:

### 1. Using test_batch_results.py with unittest mode

```bash
# Run with unittest framework
python test_batch_results.py --config ../examples/config-coremof-clean.yaml --json expected_values.json --unittest
```

### 2. Using Python's unittest framework directly

```bash
# Run all tests in the current directory
python -m unittest discover

# Run a specific test file
python -m unittest test_batch_results_cli
```

### 3. Using the gRASPA Job Tracker CLI (Recommended)

The main CLI tool now integrates testing capabilities:

```bash
# Run tests using the default expected_values.json
gRASPA_job_tracker -c config.yaml --test

# Run tests for a specific batch
gRASPA_job_tracker -c config.yaml --test --test-batch 524

# Run tests with a custom JSON file
gRASPA_job_tracker -c config.yaml --test --test-json tests/my_test_values.json

# Run tests with unittest framework
gRASPA_job_tracker -c config.yaml --test --test-unittest
```

## Setting Up Tests

To get started with testing:

1. Create an `expected_values.json` file in the tests directory using:
   ```bash
   gRASPA_job_tracker -c config.yaml --test --test-batch 524 --test-json tests/expected_values.json
   ```
   This will create a template file with all structures from batch 524.

2. Fill in the expected values for structures you want to test

3. Run the tests using one of the methods above

## JSON Format

The expected values JSON file should have the following format:

```json
{
  "BATCH_ID": {
    "STRUCTURE_NAME": {
      "column_name_1": expected_value_1,
      "column_name_2": expected_value_2,
      ...
    },
    ...
  },
  ...
}
```

Example:
```json
{
  "524": {
    "XIWKUJ_clean_pacmof": {
      "loading_mol_kg_co2_avg": 1.12647,
      "loading_mg_g_co2_avg": 49.56335,
      ...
    }
  },
  "401": {
    "XISLUI01_clean_pacmof": {
      "loading_mol_kg_co2_avg": 4.63903,
      ...
    }
  }
}
```

## Common Workflow

1. Generate a template JSON file for a batch
2. Manually enter or copy expected values for structures you want to test
3. Run the test with the JSON file
4. Review results to ensure actual values match expected values

## Integration with CI/CD

For continuous integration, you can use the unittest-based approach:

```bash
# Example CI command
cd /projects/bcvz/sbinkashif/gRASPA_job_tracker && python -m unittest discover tests
```

This will run all tests defined in the expected_values.json file and report success or failure.

## Running a Standard Suite of Tests

The expected_values.json file contains a standard suite of test structures from multiple batches:

1. Batch 524: XIWKUJ_clean_pacmof
2. Batch 524: XITYEF_clean_pacmof
3. Batch 401: XISLUI01_clean_pacmof
4. Batch 402: XISPUM01_clean_pacmof
5. Batch 421: XITYOP_clean_pacmof

These structures represent diverse cases across different batches to ensure comprehensive testing of the loading values.

## Notes

- Values are compared with a 1% tolerance (configurable in the code)
- Calculated values like selectivity are excluded from comparison
- The script focuses on validating the loading values are parsed correctly
- You can test multiple structures in multiple batches using a single JSON file
- The script will automatically find the correct results files based on your configuration
