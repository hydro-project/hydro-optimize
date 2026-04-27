#!/usr/bin/env python3
"""Parses network calibration results from benchmark CSVs and outputs a JSON cost-per-byte table.

For each message size, reads the server CSV with the highest virtual client count
at MEASUREMENT_SECOND and computes cost-per-byte for CPU, memory, and I/O.

Usage: python3 parse_calibration.py <benchmark_results_dir> <output_file>

Expects benchmark_results_dir to contain directories like:
  NetworkCalibration_1b_<timestamp>/server_10c_81vc_r0.csv
  NetworkCalibration_2b_<timestamp>/server_10c_81vc_r0.csv
  ...

CSV columns: time_s,cpu,network,memory,io,throughput_rps,...
  cpu      = max single-core CPU % (user + system)
  network  = bytes/sec (in + out)
  memory   = percent used
  io       = rtps + wtps
"""

import csv
import json
import os
import re
import sys

# Must match MEASUREMENT_SECOND in deploy_and_analyze.rs
MEASUREMENT_SECOND = 59


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <benchmark_results_dir> <output_file>", file=sys.stderr)
        sys.exit(1)

    results_dir = sys.argv[1]
    output_file = sys.argv[2]
    entries = []

    for dirname in sorted(os.listdir(results_dir)):
        m = re.match(r"NetworkCalibration_(\d+)b_", dirname)
        if not m:
            continue
        size = int(m.group(1))
        run_dir = os.path.join(results_dir, dirname)
        if not os.path.isdir(run_dir):
            continue

        # Find server CSV with highest virtual client count
        best_vc = -1
        server_csv = None
        for f in os.listdir(run_dir):
            vm = re.match(r"server_\d+c_(\d+)vc_r\d+\.csv$", f)
            if vm and int(vm.group(1)) > best_vc:
                best_vc = int(vm.group(1))
                server_csv = os.path.join(run_dir, f)

        if server_csv is None:
            print(f"  WARNING: no server CSV found for size={size}", file=sys.stderr)
            continue

        with open(server_csv) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if len(rows) <= MEASUREMENT_SECOND:
            print(f"  WARNING: not enough rows in CSV for size={size} (got {len(rows)})", file=sys.stderr)
            continue

        row = rows[MEASUREMENT_SECOND]
        throughput = float(row["throughput_rps"])
        network_bytes = float(row["network"])
        if throughput <= 0 or network_bytes <= 0:
            print(f"  WARNING: zero throughput/network for size={size}", file=sys.stderr)
            continue

        cpu_pct = float(row["cpu"])
        memory_pct = float(row["memory"])
        io_tps = float(row["io"])

        entry = {
            "message_size": size,
            "cpu_pct_per_byte": cpu_pct / network_bytes,
            "memory_pct_per_byte": memory_pct / network_bytes,
            "io_tps_per_byte": io_tps / network_bytes,
        }
        entries.append(entry)
        print(
            f"  size={size}: throughput={throughput:.0f}, network={network_bytes:.0f}B/s, "
            f"cpu_per_byte={entry['cpu_pct_per_byte']:.10f}%, "
            f"mem_per_byte={entry['memory_pct_per_byte']:.10f}%, "
            f"io_per_byte={entry['io_tps_per_byte']:.10f}tps"
        )

    entries.sort(key=lambda e: e["message_size"])
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(entries, f, indent=2)
    print(f"  Saved calibration to {output_file} ({len(entries)} entries)")


if __name__ == "__main__":
    main()
