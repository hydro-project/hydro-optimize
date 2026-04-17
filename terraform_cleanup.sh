#!/usr/bin/env bash
set -euo pipefail

HYDRO_DIR=".hydro"

if [[ ! -d "$HYDRO_DIR" ]]; then
  echo "No $HYDRO_DIR directory found."
  exit 1
fi

pids=()
dirs=()

shopt -s dotglob
for dir in "$HYDRO_DIR"/*/; do
  [[ -d "$dir" ]] || continue
  dirs+=("$dir")
  echo "Starting terraform destroy in $dir"
  (cd "$dir" && terraform destroy -auto-approve) &
  pids+=($!)
done

if [[ ${#pids[@]} -eq 0 ]]; then
  echo "No subdirectories found in $HYDRO_DIR."
  exit 0
fi

failed=0
for i in "${!pids[@]}"; do
  if wait "${pids[$i]}"; then
    echo "✓ ${dirs[$i]} — done"
  else
    echo "✗ ${dirs[$i]} — failed"
    ((failed++))
  fi
done

echo ""
echo "Completed: ${#pids[@]} directories, $failed failed."
exit $failed
