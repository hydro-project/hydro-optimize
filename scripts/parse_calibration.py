#!/usr/bin/env python3
"""Parses network calibration results from benchmark CSVs and outputs a JSON cost-per-byte table.

For each message size, reads the server CSV at MEASUREMENT_SECOND and computes
how much CPU (%), memory (KB), and I/O (tps) is consumed per byte of network traffic.

Usage: python3 parse_calibration.py <calibration_dir> <output_file>

Expects calibration_dir to contain subdirectories like:
  Network_<timestamp>/server_10c_1vc_r0.csv
One per message size, with the message size encoded in a calibrate_<size>b.log file.
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
        print(f"Usage: {sys.argv[0]} <calibration_dir> <output_file>", file=sys.stderr)
        sys.exit(1)

    calibration_dir = sys.argv[1]
    output_file = sys.argv[2]
    entries = []

    for fname in sorted(os.listdir(calibration_dir)):
        if not fname.startswith("calibrate_") or not fname.endswith(".log"):
            continue
        size = int(re.search(r"calibrate_(\d+)b", fname).group(1))

        # Find the corresponding CSV for the server cluster
        # The benchmark outputs CSVs in a subdirectory like Network_<timestamp>/
        csv_dir = None
        for d in os.listdir(calibration_dir):
            candidate = os.path.join(calibration_dir, d)
            if os.path.isdir(candidate) and d.startswith("Network"):
                csv_dir = candidate
                break

        if csv_dir is None:
            print(f"  WARNING: no benchmark output dir found for size={size}", file=sys.stderr)
            continue

        # Find server CSV
        server_csv = None
        for f in os.listdir(csv_dir):
            if f.startswith("server") and f.endswith(".csv"):
                server_csv = os.path.join(csv_dir, f)
                break

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
        if throughput <= 0:
            print(f"  WARNING: zero throughput for size={size}", file=sys.stderr)
            continue

        bytes_per_sec = throughput * size
        cpu_pct = float(row["cpu_user"]) + float(row["cpu_system"])
        mem_kb = float(row["mem_kb_used"])
        io_tps = float(row["io_rtps"]) + float(row["io_wtps"])

        entry = {
            "message_size": size,
            "cpu_pct_per_byte": cpu_pct / bytes_per_sec,
            "memory_kb_per_byte": mem_kb / bytes_per_sec,
            "io_tps_per_byte": io_tps / bytes_per_sec,
        }
        entries.append(entry)
        print(
            f"  size={size}: throughput={throughput:.0f}, "
            f"cpu_per_byte={entry['cpu_pct_per_byte']:.8f}%, "
            f"mem_per_byte={entry['memory_kb_per_byte']:.8f}KB, "
            f"io_per_byte={entry['io_tps_per_byte']:.8f}tps"
        )

    entries.sort(key=lambda e: e["message_size"])
    with open(output_file, "w") as f:
        json.dump(entries, f, indent=2)
    print(f"  Saved calibration to {output_file} ({len(entries)} entries)")


if __name__ == "__main__":
    main()
