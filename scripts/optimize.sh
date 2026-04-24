#!/usr/bin/env bash
# Orchestrates the full optimization pipeline:
#   1. Network calibration (run once, results cached)
#   2. Baseline benchmark (no counters)
#   3. Concurrent: counter run + byte-size inspection + greedy decouple analysis
#   4. Combine metrics, find bottleneck, run ILP
#   5. Apply best rewrite, re-benchmark, iterate
#
# Usage: ./scripts/optimize.sh [--aws | --gcp PROJECT] [--skip-calibration]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_DIR/optimization_results/$(date +%Y-%m-%d_%H-%M-%S)"
CALIBRATION_FILE="$PROJECT_DIR/optimization_results/network_calibration.json"

CLOUD_ARGS=""
SKIP_CALIBRATION=false

# Parse arguments
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
        --skip-calibration)
            SKIP_CALIBRATION=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

# ─── Step 1: Network Calibration ───
if [ "$SKIP_CALIBRATION" = true ] && [ -f "$CALIBRATION_FILE" ]; then
    echo "=== Skipping calibration (using cached $CALIBRATION_FILE) ==="
else
    "$SCRIPT_DIR/calibrate_network.sh" $CLOUD_ARGS --output "$CALIBRATION_FILE"
fi

# ─── Step 2: Baseline Benchmark (no counters) ───
echo "=== Step 2: Baseline Benchmark ==="
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS \
    > "$OUTPUT_DIR/baseline.log" 2>&1

# Extract baseline throughput
BASELINE_THROUGHPUT=$(grep -oP '\d+(?=\s*requests/s)' "$OUTPUT_DIR/baseline.log" | tail -1)
echo "  Baseline throughput: ${BASELINE_THROUGHPUT:-unknown} requests/s"

# ─── Step 3: Concurrent analysis runs ───
echo "=== Step 3: Concurrent Analysis ==="

# 3a: Run with counters
echo "  Launching counter run..."
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS \
    > "$OUTPUT_DIR/counters.log" 2>&1 &
PID_COUNTERS=$!

# 3b: Run with byte-size inspection (greedy decouple + inspect)
echo "  Launching byte-size inspection run..."
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS --decouple-analysis \
    > "$OUTPUT_DIR/byte_sizes.log" 2>&1 &
PID_BYTES=$!

# Wait for both
wait "$PID_COUNTERS" || echo "  WARNING: counter run failed"
wait "$PID_BYTES" || echo "  WARNING: byte-size run failed"

echo "  Analysis runs complete."

# ─── Step 4: Combine and optimize ───
echo "=== Step 4: Combine Metrics & Find Optimal Configuration ==="
# The Rust binary handles combining OperatorMetrics, finding bottlenecks,
# running the ILP with increasing machine budgets, and outputting the best rewrite.
# TODO: Create a dedicated Rust example (e.g., optimize_paxos) that:
#   1. Loads calibration from $CALIBRATION_FILE
#   2. Loads counter data from $OUTPUT_DIR/counters.log
#   3. Loads byte-size data from $OUTPUT_DIR/byte_sizes.log
#   4. Loads SAR data from the decouple analysis run
#   5. Builds OperatorMetrics
#   6. Calls find_optimal_budget
#   7. Outputs the best rewrite to $OUTPUT_DIR/rewrite.json

echo "  TODO: Run optimize_paxos binary to combine metrics and produce rewrite"

# ─── Step 5: Iterative improvement ───
echo "=== Step 5: Apply & Re-benchmark ==="
ITERATION=0
CURRENT_THROUGHPUT="${BASELINE_THROUGHPUT:-0}"

# TODO: Loop:
#   1. Apply rewrite from step 4
#   2. Re-benchmark
#   3. If throughput improved, re-analyze and repeat
#   4. If not, halt

echo "=== Optimization complete ==="
echo "  Baseline throughput: ${BASELINE_THROUGHPUT:-unknown}"
echo "  Results in: $OUTPUT_DIR"
