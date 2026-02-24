#!/usr/bin/env python3
"""
Benchmark Saturation Graph

Reads all cluster CSVs from a benchmark_results folder and plots:
  1. Throughput (x) vs p50 Latency (y)
  2. Per-cluster CPU usage
  3. Per-cluster Network bandwidth (GB/s)

Values are averaged over a time range (--time start,end).
"""

import argparse
import csv
import glob
import os
import re
import sys
from collections import defaultdict
import matplotlib.pyplot as plt


def parse_csv_name(filename):
    """Extract (cluster, physical, virtual) from cluster_<N>c_<N>vc.csv."""
    m = re.match(r"(.+?)_(\d+)c_(\d+)vc\.csv$", filename)
    if not m:
        return None, None, None
    return m.group(1), int(m.group(2)), int(m.group(3))


def read_point(filepath, time_start, time_end):
    rows = []
    with open(filepath, "r") as f:
        for row in csv.DictReader(f):
            t = int(row["time_s"])
            if time_start <= t <= time_end:
                rows.append(row)
    if not rows:
        return None
    n = len(rows)
    return {
        "throughput": sum(float(r["throughput_rps"]) for r in rows) / n,
        "latency": sum(float(r["latency_p50_ms"]) for r in rows) / n,
        "cpu": sum(float(r["cpu_user"]) + float(r["cpu_system"]) for r in rows) / n,
        "net_gb": sum(float(r["network_tx_bytes_per_sec"]) + float(r["network_rx_bytes_per_sec"]) for r in rows) / n / 1e9,
    }


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark saturation curves")
    parser.add_argument("folder", help="Path to a benchmark_results subfolder")
    parser.add_argument("-t", "--time", type=str, required=True, help="Time range as start,end (e.g. 30,60)")
    parser.add_argument("-m", "--cpu-multiplier", type=float, default=1.0, help="CPU usage multiplier (e.g. 8.0 to scale 8-core percentages to single-core)")
    parser.add_argument("-o", "--output", default=None, help="Output image path")
    args = parser.parse_args()

    time_start, time_end = (int(x) for x in args.time.split(","))

    all_csvs = sorted(glob.glob(os.path.join(args.folder, "*_*c_*vc.csv")))
    if not all_csvs:
        print(f"Error: no CSVs found in {args.folder}", file=sys.stderr)
        sys.exit(1)

    # Group CSVs by cluster name
    # cluster_data[cluster][(phys, virt)] = {throughput, latency, cpu, net_gb}
    cluster_data = defaultdict(dict)
    for path in all_csvs:
        cluster, phys, virt = parse_csv_name(os.path.basename(path))
        if cluster is None:
            continue
        pt = read_point(path, time_start, time_end)
        if pt:
            cluster_data[cluster][(phys, virt)] = pt

    if not cluster_data:
        print(f"Error: no data for time range {time_start}-{time_end}", file=sys.stderr)
        sys.exit(1)

    # Pick first cluster for throughput/latency (same across all)
    ref_cluster = next(iter(cluster_data))
    configs = sorted(cluster_data[ref_cluster].keys())  # sorted by (phys, virt)
    throughput = [cluster_data[ref_cluster][c]["throughput"] for c in configs]
    latency = [cluster_data[ref_cluster][c]["latency"] for c in configs]
    total_clients = [p * v for p, v in configs]
    phys_list = [c[0] for c in configs]

    unique_phys = sorted(set(phys_list))
    phys_cmap = plt.cm.get_cmap("tab10", len(unique_phys))
    phys_color = {p: phys_cmap(i) for i, p in enumerate(unique_phys)}

    # Consistent colors for clusters
    clusters = sorted(cluster_data.keys())
    cluster_cmap = plt.cm.get_cmap("Set2", len(clusters))
    cluster_color = {c: cluster_cmap(i) for i, c in enumerate(clusters)}

    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)

    # Latency plot
    ax_lat = axes[0]
    for p in unique_phys:
        idx = [i for i, x in enumerate(phys_list) if x == p]
        ax_lat.plot(
            [throughput[i] for i in idx], [latency[i] for i in idx],
            "o-", color=phys_color[p], linewidth=1.5, markersize=5,
            label=f"{p} physical",
        )
    for i, tc in enumerate(total_clients):
        ax_lat.annotate(str(tc), (throughput[i], latency[i]),
                        textcoords="offset points", xytext=(4, 4), fontsize=7,
                        color=phys_color[phys_list[i]])
    ax_lat.set_ylabel("p50 Latency (ms)", fontsize=11)
    ax_lat.grid(True, alpha=0.3)
    ax_lat.legend(fontsize=9)

    # CPU and network plots per cluster
    for ax, metric, label in [
        (axes[1], "cpu", f"CPU (%) ×{args.cpu_multiplier}"),
        (axes[2], "net_gb", "Network (GB/s)"),
    ]:
        for cluster in clusters:
            data = cluster_data[cluster]
            cfgs = sorted(data.keys())
            thr = [cluster_data[ref_cluster][c]["throughput"] for c in cfgs if c in cluster_data[ref_cluster]]
            vals = [data[c][metric] * (args.cpu_multiplier if metric == "cpu" else 1) for c in cfgs if c in cluster_data[ref_cluster]]
            ax.plot(thr, vals, "o-", color=cluster_color[cluster],
                    linewidth=1.5, markersize=5, label=cluster)
        ax.set_ylabel(label, fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=9)

    axes[-1].set_xlabel("Throughput (rps)", fontsize=12)
    fig.suptitle(f"{os.path.basename(args.folder)} @ t={time_start}-{time_end}s (avg)", fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    output = args.output or os.path.join(args.folder, "saturation.png")
    plt.savefig(output, dpi=200, bbox_inches="tight")
    print(f"Saved to {output}")
    plt.show()


if __name__ == "__main__":
    main()
