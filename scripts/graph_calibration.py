#!/usr/bin/env python3
"""Plot CPU/msg vs virtual clients for network calibration results.
Produces one PNG per message size, with one line per physical client count.
"""
import os
import re
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

RESULTS_DIR = Path(__file__).parent.parent / "benchmark_results"
OUTPUT_DIR = Path(__file__).parent / "network"
START_SECOND = 30
END_SECOND = 59

dir_re = re.compile(r"^Network_(\d+)b_(\d+)c_default_none$")
csv_re = re.compile(r"^server_(\d+)c_(\d+)vc_r\d+\.csv$")


def parse_csv(path):
    """Return (avg_cpu, avg_throughput) over measurement window."""
    with open(path) as f:
        reader = list(csv.DictReader(f))
    if len(reader) <= END_SECOND:
        return None, None
    window = reader[START_SECOND : END_SECOND + 1]
    n = len(window)
    cpu = sum(float(r["cpu"]) for r in window) / n
    thr = sum(float(r["throughput_rps"]) for r in window) / n
    return cpu, thr


def main():
    # data[msg_size][num_clients] = [(virtual_clients, cpu_per_msg)]
    data = defaultdict(lambda: defaultdict(list))

    for entry in os.listdir(RESULTS_DIR):
        m = dir_re.match(entry)
        if not m:
            continue
        msg_size = int(m.group(1))
        num_clients = int(m.group(2))
        dir_path = RESULTS_DIR / entry

        for fname in os.listdir(dir_path):
            cm = csv_re.match(fname)
            if not cm:
                continue
            vc = int(cm.group(2))
            cpu, thr = parse_csv(dir_path / fname)
            if cpu is not None and thr and thr > 0 and vc > 1:
                data[msg_size][num_clients].append((vc, cpu / thr))

    OUTPUT_DIR.mkdir(exist_ok=True)

    for msg_size in sorted(data.keys()):
        fig, ax = plt.subplots(figsize=(10, 6))
        for num_clients in sorted(data[msg_size].keys()):
            points = sorted(data[msg_size][num_clients])
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            ax.plot(xs, ys, marker="o", label=f"{num_clients} clients")
        ax.set_xlabel("Virtual Clients")
        ax.set_ylabel("CPU % per message")
        ax.set_title(f"Network Calibration: {msg_size}B messages")
        ax.legend()
        ax.grid(True, alpha=0.3)
        out_path = OUTPUT_DIR / f"calibration_{msg_size}b.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved {out_path}")


if __name__ == "__main__":
    main()
