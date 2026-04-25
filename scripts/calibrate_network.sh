#!/usr/bin/env bash
# Network calibration: measures cost-per-byte at various message sizes.
# The network_calibrator example internally loops over exponentially increasing
# message sizes (1B–4KB), reusing the same machines for consistency.
#
# Usage: ./scripts/calibrate_network.sh [--aws | --gcp PROJECT] [--output FILE]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_FILE="$PROJECT_DIR/optimization_results/network_calibration.json"
CLOUD_ARGS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --aws)
            CLOUD_ARGS="--aws"
            shift
            ;;
        --gcp)
            CLOUD_ARGS="--gcp $2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

mkdir -p "$(dirname "$OUTPUT_FILE")"

echo "=== Network Calibration ==="

cargo run --example network_calibrator -- $CLOUD_ARGS > "$OUTPUT_FILE"run.txt

# Parse calibration results from benchmark_results/NetworkCalibration_*/ dirs
echo "  Parsing calibration results..."
python3 "$SCRIPT_DIR/parse_calibration.py" "$PROJECT_DIR/benchmark_results" "$OUTPUT_FILE"

echo "=== Calibration complete ==="
