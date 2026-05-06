#!/usr/bin/env python3
"""
Benchmark Saturation Graph

Reads all cluster CSVs from a benchmark_results folder and plots:
  1. Throughput (x) vs p50 Latency (y)
  2. Per-cluster CPU usage
  3. Per-cluster Network bandwidth (MB/s)

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
    """Extract (cluster, physical, virtual, run) from cluster_<N>c_<N>vc_r<N>.csv."""
    m = re.match(r"(.+?)_(\d+)c_(\d+)vc_r(\d+)\.csv$", filename)
    if not m:
        return None, None, None, None
    return m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))


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
        "cpu": sum(float(r["cpu"]) for r in rows) / n,
        "net_mb": sum(float(r["network"]) for r in rows) / n / 1e6,
    }


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark saturation curves")
    parser.add_argument("folder", help="Path to a benchmark_results subfolder")
    parser.add_argument("-t", "--time", type=str, default="30,59", help="Time range as start,end (default: 30,59)")
    parser.add_argument("-o", "--output", default=None, help="Output image path")
    args = parser.parse_args()

    time_start, time_end = (int(x) for x in args.time.split(","))

    all_csvs = sorted(glob.glob(os.path.join(args.folder, "*_*c_*vc_r*.csv")))
    if not all_csvs:
        print(f"Error: no CSVs found in {args.folder}", file=sys.stderr)
        sys.exit(1)

    # Group CSVs by cluster name
    cluster_runs = defaultdict(lambda: defaultdict(list))
    for path in all_csvs:
        cluster, phys, virt, run = parse_csv_name(os.path.basename(path))
        if cluster is None:
            continue
        pt = read_point(path, time_start, time_end)
        if pt:
            cluster_runs[cluster][(phys, virt)].append(pt)

    if not cluster_runs:
        print(f"Error: no data for time range {time_start}-{time_end}", file=sys.stderr)
        sys.exit(1)

    # Aggregate runs
    METRICS = ["throughput", "latency", "cpu", "net_mb"]
    cluster_data = defaultdict(dict)
    for cluster, configs_runs in cluster_runs.items():
        for cfg, pts in configs_runs.items():
            agg = {}
            for m in METRICS:
                vals = [p[m] for p in pts]
                agg[m] = sum(vals) / len(vals)
                agg[f"{m}_lo"] = min(vals)
                agg[f"{m}_hi"] = max(vals)
            cluster_data[cluster][cfg] = agg

    # Use first cluster for throughput reference
    ref_cluster = next(iter(cluster_data))
    configs = sorted(cluster_data[ref_cluster].keys())
    throughput = [cluster_data[ref_cluster][c]["throughput"] for c in configs]
    latency = [cluster_data[ref_cluster][c]["latency"] for c in configs]
    total_clients = [p * v for p, v in configs]

    clusters = sorted(cluster_data.keys())
    cluster_cmap = plt.cm.get_cmap("Set2", max(len(clusters), 3))
    cluster_color = {c: cluster_cmap(i) for i, c in enumerate(clusters)}

    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)

    # 1. Latency vs throughput
    ax_lat = axes[0]
    ax_lat.plot(throughput, latency, "o-", linewidth=1.5, markersize=5)
    for i, tc in enumerate(total_clients):
        ax_lat.annotate(str(tc), (throughput[i], latency[i]),
                        textcoords="offset points", xytext=(4, 4), fontsize=7)
    ax_lat.set_ylabel("p50 Latency (ms)", fontsize=11)
    ax_lat.grid(True, alpha=0.3)
    ax_lat.set_title("Throughput vs Latency (labels = total virtual clients)")

    # 2. CPU per cluster
    ax_cpu = axes[1]
    for cluster in clusters:
        data = cluster_data[cluster]
        cfgs = sorted(c for c in data if c in cluster_data[ref_cluster])
        thr = [cluster_data[ref_cluster][c]["throughput"] for c in cfgs]
        vals = [data[c]["cpu"] for c in cfgs]
        ax_cpu.plot(thr, vals, "o-", color=cluster_color[cluster],
                    linewidth=1.5, markersize=5, label=cluster)
    ax_cpu.axhline(100, color="red", linestyle="--", alpha=0.5, label="100% (1 core)")
    ax_cpu.set_ylabel("CPU (%)", fontsize=11)
    ax_cpu.grid(True, alpha=0.3)
    ax_cpu.legend(fontsize=9)

    # 3. Network per cluster
    ax_net = axes[2]
    for cluster in clusters:
        data = cluster_data[cluster]
        cfgs = sorted(c for c in data if c in cluster_data[ref_cluster])
        thr = [cluster_data[ref_cluster][c]["throughput"] for c in cfgs]
        vals = [data[c]["net_mb"] for c in cfgs]
        ax_net.plot(thr, vals, "o-", color=cluster_color[cluster],
                    linewidth=1.5, markersize=5, label=cluster)
    ax_net.set_ylabel("Network (MB/s)", fontsize=11)
    ax_net.set_xlabel("Throughput (rps)", fontsize=12)
    ax_net.grid(True, alpha=0.3)
    ax_net.legend(fontsize=9)

    fig.suptitle(os.path.basename(args.folder), fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    output = args.output or os.path.join(args.folder, "saturation.png")
    plt.savefig(output, dpi=200, bbox_inches="tight")
    print(f"Saved to {output}")


if __name__ == "__main__":
    main()
