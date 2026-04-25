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
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS --no-counters \
    > "$OUTPUT_DIR/baseline.log" 2>&1

# Extract baseline throughput
BASELINE_THROUGHPUT=$(grep -oP '\d+(?=\s*requests/s)' "$OUTPUT_DIR/baseline.log" | tail -1)
echo "  Baseline throughput: ${BASELINE_THROUGHPUT:-unknown} requests/s"

# ─── Step 3: Concurrent analysis runs ───
echo "=== Step 3: Concurrent Analysis ==="

# 3a: Run with counters (default, no extra flags)
echo "  Launching counter run..."
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS \
    > "$OUTPUT_DIR/counters.log" 2>&1 &
PID_COUNTERS=$!

# 3b: Run with byte-size inspection (inserts Inspect nodes at network boundaries)
echo "  Launching byte-size inspection run..."
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS --size-analysis \
    > "$OUTPUT_DIR/byte_sizes.log" 2>&1 &
PID_BYTES=$!

# 3c: Run with blow-up analysis (applies greedy decoupling, deploys decoupled system, gathers per-operator SAR)
echo "  Launching blow-up analysis run..."
cargo run --release --example benchmark_paxos -- $CLOUD_ARGS --blow-up-analysis \
    > "$OUTPUT_DIR/blow_up.log" 2>&1 &
PID_BLOWUP=$!

# Wait for all
wait "$PID_COUNTERS" || echo "  WARNING: counter run failed"
wait "$PID_BYTES" || echo "  WARNING: byte-size run failed"
wait "$PID_BLOWUP" || echo "  WARNING: blow-up analysis run failed"

# Find the profiling JSONs from each run's benchmark_results directory
find_profiling_json() {
    # benchmark_paxos outputs to benchmark_results/Paxos_<timestamp>/profiling_*.json
    # Find the most recent one
    find "$PROJECT_DIR/benchmark_results" -name "profiling_*.json" -newer "$1" -print | sort | tail -1
}

COUNTERS_JSON=$(find_profiling_json "$OUTPUT_DIR/counters.log" 2>/dev/null || echo "")
BYTE_SIZES_JSON=$(find_profiling_json "$OUTPUT_DIR/byte_sizes.log" 2>/dev/null || echo "")
BLOW_UP_JSON=$(find_profiling_json "$OUTPUT_DIR/blow_up.log" 2>/dev/null || echo "")

if [ -z "$COUNTERS_JSON" ] || [ -z "$BYTE_SIZES_JSON" ] || [ -z "$BLOW_UP_JSON" ]; then
    echo "  ERROR: Could not find all profiling JSONs"
    echo "    counters: ${COUNTERS_JSON:-missing}"
    echo "    byte_sizes: ${BYTE_SIZES_JSON:-missing}"
    echo "    blow_up: ${BLOW_UP_JSON:-missing}"
    exit 1
fi

METRICS_JSON="$OUTPUT_DIR/operator_metrics.json"

# Run the ILP optimizer, passing all data files directly
REWRITE_JSON="$OUTPUT_DIR/rewrite.json"
cargo run --release --example optimize_paxos -- \
    --counters "$COUNTERS_JSON" \
    --byte-sizes "$BYTE_SIZES_JSON" \
    --blow-up "$BLOW_UP_JSON" \
    --calibration "$CALIBRATION_FILE" \
    --output "$REWRITE_JSON" \
    $CLOUD_ARGS

# ─── Step 5: Iterative improvement ───
echo "=== Step 5: Apply & Re-benchmark ==="
ITERATION=0
CURRENT_THROUGHPUT="${BASELINE_THROUGHPUT:-0}"

while true; do
    ITERATION=$((ITERATION + 1))
    echo "  Iteration $ITERATION: applying rewrite and benchmarking..."

    # Run benchmark with the rewrite applied
    ITER_LOG="$OUTPUT_DIR/iteration_${ITERATION}.log"
    cargo run --release --example benchmark_paxos -- $CLOUD_ARGS \
        --rewrite "$REWRITE_JSON" --no-counters \
        > "$ITER_LOG" 2>&1

    # Extract throughput
    NEW_THROUGHPUT=$(grep -oP '\d+(?=\s*requests/s)' "$ITER_LOG" | tail -1)
    echo "  Iteration $ITERATION throughput: ${NEW_THROUGHPUT:-0} requests/s (previous: $CURRENT_THROUGHPUT)"

    # Check if throughput improved
    if [ "${NEW_THROUGHPUT:-0}" -le "$CURRENT_THROUGHPUT" ]; then
        echo "  No improvement. Halting."
        break
    fi

    CURRENT_THROUGHPUT="$NEW_THROUGHPUT"

    # Re-analyze: run blow-up analysis on the rewritten system
    echo "  Re-analyzing with blow-up analysis..."
    ITER_BLOWUP_LOG="$OUTPUT_DIR/iteration_${ITERATION}_blowup.log"
    cargo run --release --example benchmark_paxos -- $CLOUD_ARGS \
        --rewrite "$REWRITE_JSON" --blow-up-analysis \
        > "$ITER_BLOWUP_LOG" 2>&1

    ITER_BLOWUP_JSON=$(find_profiling_json "$ITER_BLOWUP_LOG" 2>/dev/null || echo "")
    if [ -z "$ITER_BLOWUP_JSON" ]; then
        echo "  Could not find blow-up profiling JSON. Halting."
        break
    fi

    # Re-run ILP with updated blow-up data
    PREV_REWRITE="$REWRITE_JSON"
    REWRITE_JSON="$OUTPUT_DIR/iteration_${ITERATION}_rewrite.json"
    cargo run --release --example optimize_paxos -- \
        --counters "$COUNTERS_JSON" \
        --byte-sizes "$BYTE_SIZES_JSON" \
        --blow-up "$ITER_BLOWUP_JSON" \
        --calibration "$CALIBRATION_FILE" \
        --previous-rewrite "$PREV_REWRITE" \
        --output "$REWRITE_JSON" \
        $CLOUD_ARGS

    # If no rewrite was produced (ILP found no benefit), halt
    if [ ! -f "$REWRITE_JSON" ]; then
        echo "  ILP found no beneficial rewrite. Halting."
        break
    fi
done

echo "=== Optimization complete ==="
echo "  Baseline throughput: ${BASELINE_THROUGHPUT:-unknown}"
echo "  Results in: $OUTPUT_DIR"
