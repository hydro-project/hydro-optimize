#!/usr/bin/env python3
"""
Benchmark Saturation Graph

Reads all proposer CSVs from a benchmark_results folder and plots:
  1. Throughput (x) vs p99 Latency (y)
  2. CPU usage * multiplier
  3. Network bandwidth (GB/s)

All values are taken at a specific measurement time (--time).
"""

import argparse
import csv
import glob
import os
import re
import sys
import matplotlib.pyplot as plt


def parse_clients(filename):
    """Extract (physical, virtual) from proposer_<N>c_<N>vc.csv."""
    m = re.search(r"_(\d+)c_(\d+)vc\.csv$", filename)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def read_point(filepath, time_s, cpu_multiplier):
    phys, virt = parse_clients(os.path.basename(filepath))
    if phys is None:
        return None
    with open(filepath, "r") as f:
        for row in csv.DictReader(f):
            if int(row["time_s"]) == time_s:
                throughput = float(row["throughput_rps"])
                latency = float(row["latency_p50_ms"])
                cpu = (float(row["cpu_user"]) + float(row["cpu_system"])) * cpu_multiplier
                net_gb = (float(row["network_tx_bytes_per_sec"]) + float(row["network_rx_bytes_per_sec"])) / 1e9
                return throughput, latency, cpu, net_gb, phys, virt
    return None


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark saturation curves from proposer CSVs")
    parser.add_argument("folder", help="Path to a benchmark_results subfolder")
    parser.add_argument("-t", "--time", type=int, required=True, help="Measurement time_s to extract")
    parser.add_argument("-m", "--cpu-multiplier", type=float, default=1.0, help="CPU usage multiplier (default: 1.0)")
    parser.add_argument("-o", "--output", default=None, help="Output image path")
    args = parser.parse_args()

    csvs = sorted(glob.glob(os.path.join(args.folder, "proposer_*.csv")))
    if not csvs:
        print(f"Error: no proposer CSVs found in {args.folder}", file=sys.stderr)
        sys.exit(1)

    points = []
    for path in csvs:
        pt = read_point(path, args.time, args.cpu_multiplier)
        if pt:
            points.append(pt)

    if not points:
        print(f"Error: no data at time_s={args.time}", file=sys.stderr)
        sys.exit(1)

    points.sort(key=lambda p: (p[4], p[5]))  # sort by (phys, virt)
    throughput, latency, cpu, net, phys, virt = zip(*points)
    total_clients = [p * v for p, v in zip(phys, virt)]

    # Assign a color per unique physical client count
    unique_phys = sorted(set(phys))
    cmap = plt.cm.get_cmap("tab10", len(unique_phys))
    phys_color = {p: cmap(i) for i, p in enumerate(unique_phys)}
    colors = [phys_color[p] for p in phys]

    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)

    # Latency plot: colored by physical clients, labeled with total clients
    ax_lat = axes[0]
    for p in unique_phys:
        idx = [i for i, x in enumerate(phys) if x == p]
        ax_lat.plot(
            [throughput[i] for i in idx], [latency[i] for i in idx],
            "o-", color=phys_color[p], linewidth=1.5, markersize=5,
            label=f"{p} physical",
        )
    for i, tc in enumerate(total_clients):
        ax_lat.annotate(str(tc), (throughput[i], latency[i]),
                        textcoords="offset points", xytext=(4, 4), fontsize=7,
                        color=colors[i])
    ax_lat.set_ylabel("p50 Latency (ms)", fontsize=11)
    ax_lat.grid(True, alpha=0.3)
    ax_lat.legend(fontsize=9)

    # CPU and network plots
    for ax, data, label, color in [
        (axes[1], cpu, f"CPU (%) ×{args.cpu_multiplier}", "tab:red"),
        (axes[2], net, "Network (GB/s)", "tab:blue"),
    ]:
        for p in unique_phys:
            idx = [i for i, x in enumerate(phys) if x == p]
            ax.plot([throughput[i] for i in idx], [data[i] for i in idx],
                    "o-", color=color, linewidth=1.5, markersize=5)
        ax.set_ylabel(label, fontsize=11)
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Throughput (rps)", fontsize=12)
    fig.suptitle(f"{os.path.basename(args.folder)} @ t={args.time}s", fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    output = args.output or os.path.join(args.folder, "saturation.png")
    plt.savefig(output, dpi=200, bbox_inches="tight")
    print(f"Saved to {output}")
    plt.show()


if __name__ == "__main__":
    main()
