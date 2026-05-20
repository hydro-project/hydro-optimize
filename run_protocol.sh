#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <example name>"
    exit 1
fi

PROTOCOL="$1"
STOP_MSG="exhausted all budgets. Nothing to do."

echo "=== Running base $PROTOCOL ==="
cargo run --example "$PROTOCOL" -- --aws 2>&1 | tee "${PROTOCOL}.txt"

iteration=0
while true; do
    iteration=$((iteration + 1))
    echo "=== Optimization iteration $iteration ==="
    
    output=$(cargo run --example "$PROTOCOL" -- --aws --optimize 2>&1 | tee "${PROTOCOL}_optimize_${iteration}.txt")
    exit_code=${PIPESTATUS[0]}

    if [ $exit_code -ne 0 ]; then
        echo "Process exited with code $exit_code (panic or error). Stopping."
        break
    fi

    if echo "$output" | grep -q "$STOP_MSG"; then
        echo "Optimization exhausted. Stopping."
        break
    fi
done
