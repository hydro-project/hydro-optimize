#!/usr/bin/env bash
# Network calibration: measures cost-per-byte at various message sizes.
# Launches parallel benchmark runs with exponentially increasing message sizes (1B–4KB),
# parses SAR network metrics and throughput, computes cost-per-byte, saves to JSON.
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

CALIBRATION_DIR="$(dirname "$OUTPUT_FILE")/calibration"
mkdir -p "$CALIBRATION_DIR"

echo "=== Network Calibration ==="

# Launch calibration runs in parallel with exponentially increasing message sizes
PIDS=()
for size in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096; do
    echo "  Launching calibration with message_size=$size"
    cargo run --example network_calibrator -- \
        $CLOUD_ARGS --message-size "$size" \
        > "$CALIBRATION_DIR/calibrate_${size}b.log" 2>&1 &
    PIDS+=($!)
done

# Wait for all calibration runs
FAILED=0
for pid in "${PIDS[@]}"; do
    if ! wait "$pid"; then
        echo "  WARNING: calibration process $pid failed"
        FAILED=$((FAILED + 1))
    fi
done

if [ "$FAILED" -gt 0 ]; then
    echo "  $FAILED calibration runs failed. Check logs in $CALIBRATION_DIR"
fi

# Parse calibration results
echo "  Parsing calibration results..."
python3 "$SCRIPT_DIR/parse_calibration.py" "$CALIBRATION_DIR" "$OUTPUT_FILE"

echo "=== Calibration complete ==="
