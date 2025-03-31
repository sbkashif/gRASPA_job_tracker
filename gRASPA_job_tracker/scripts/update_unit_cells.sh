#!/bin/bash

# Default values
INPUT_FILE=""
CIF_FILE=""

# Ensure that mincell is installed
if ! python -c "import mincell" &> /dev/null; then
    echo "mincell is not installed. Please install it."
    exit 1
fi

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -i|--input) INPUT_FILE="$2"; shift ;;
        -c|--cif) CIF_FILE="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 0 ;;
    esac
    shift
done

# Validate arguments
if [[ -z "$INPUT_FILE" || -z "$CIF_FILE" ]]; then
    echo "Usage: $0 -i <simulation.input> -c <path_to_cif>"
    exit 0
fi

# Extract a, b, c values while ignoring warnings
if ! a_b_c=$(python -c "import warnings; warnings.simplefilter('ignore'); import mincell; a,b,c=mincell.minCells_strict('$CIF_FILE',12.8); print(f'{a} {b} {c}')"); then
    echo "Failed to extract a, b, c values from CIF file: $CIF_FILE" >&2
    echo "Skipping update for $INPUT_FILE"
    return 1
fi

# Update the UnitCells line in the input file
if ! sed -i "s/^UnitCells.*/UnitCells 0 $a_b_c/" "$INPUT_FILE"; then
    echo "Failed to update UnitCells in input file: $INPUT_FILE" >&2
    echo "Skipping update for $INPUT_FILE"
    return 1
fi

echo "Updated $INPUT_FILE with UnitCells 0 $a_b_c"

