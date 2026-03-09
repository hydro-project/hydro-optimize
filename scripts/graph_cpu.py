#!/usr/bin/env python3
"""
CPU Usage Time-Series Graphs

Reads all cluster CSVs from a benchmark_results folder and creates one PDF per
cluster type.  Each PDF contains one graph per CPU (aggregate "all" + per-core),
ordered by descending average usage.  Lines are colored by run.
"""

import argparse
import csv
import glob
import os
import re
from collections import defaultdict

import matplotlib.pyplot as plt


def parse_csv_name(filename):
    m = re.match(r"(.+?)_(\d+)c_(\d+)vc_r(\d+)\.csv$", filename)
    if not m:
        return None, None, None, None
    return m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))


def detect_cores(header):
    """Return sorted list of core indices found in the CSV header."""
    cores = set()
    for col in header:
        m = re.match(r"core(\d+)_user", col)
        if m:
            cores.add(int(m.group(1)))
    return sorted(cores)


def read_cpu_timeseries(filepath):
    """Return dict mapping cpu_name -> [(time, usage%), ...]."""
    series = defaultdict(list)
    with open(filepath) as f:
        reader = csv.DictReader(f)
        cores = detect_cores(reader.fieldnames or [])
        for row in reader:
            t = int(row["time_s"])
            series["all"].append((t, float(row["cpu_user"]) + float(row["cpu_system"])))
            for c in cores:
                u = float(row.get(f"core{c}_user", 0))
                s = float(row.get(f"core{c}_system", 0))
                series[f"core{c}"].append((t, u + s))
    return dict(series)


def main():
    parser = argparse.ArgumentParser(description="Plot per-core CPU usage time series")
    parser.add_argument("folder", help="Path to a benchmark_results subfolder")
    parser.add_argument("-o", "--output-dir", default=None, help="Output directory (default: same as folder)")
    args = parser.parse_args()

    out_dir = args.output_dir or "."

    all_csvs = sorted(glob.glob(os.path.join(args.folder, "*_*c_*vc_r*.csv")))
    if not all_csvs:
        print(f"No CSVs found in {args.folder}")
        return

    # cluster -> [(label, {cpu_name -> timeseries}), ...]
    cluster_runs = defaultdict(list)
    for path in all_csvs:
        cluster, phys, virt, run = parse_csv_name(os.path.basename(path))
        if cluster is None:
            continue
        series = read_cpu_timeseries(path)
        cluster_runs[cluster].append(((phys, virt, run), f"{phys}c_{virt}vc_r{run}", series))

    cmap = plt.cm.get_cmap("tab10")

    clusters = sorted(cluster_runs.keys())
    for runs in cluster_runs.values():
        runs.sort(key=lambda r: r[0])

    # Collect all cpu names across everything, sort numerically
    all_cpu_names = set()
    for runs in cluster_runs.values():
        for _, _, series in runs:
            all_cpu_names.update(k for k in series.keys() if k != "all")
    ranked = sorted(all_cpu_names, key=lambda n: int(re.search(r'\d+', n).group()))
    cpu_color = {name: cmap(i % 10) for i, name in enumerate(ranked)}

    # Rows = unique run configs (union across clusters), sorted numerically
    all_configs = sorted({r[0] for runs in cluster_runs.values() for r in runs})

    ncols = len(clusters)
    nrows = len(all_configs)
    fig, axes = plt.subplots(nrows, ncols, figsize=(10 * ncols, 3 * nrows), sharex=True, sharey=True, squeeze=False)

    for col, cluster in enumerate(clusters):
        axes[0][col].set_title(cluster, fontsize=12)
        run_map = {r[0]: (r[1], r[2]) for r in cluster_runs[cluster]}
        for row, cfg in enumerate(all_configs):
            ax = axes[row][col]
            if cfg in run_map:
                label, series = run_map[cfg]
                for cpu_name in ranked:
                    ts = series.get(cpu_name, [])
                    if not ts:
                        continue
                    times, usages = zip(*ts)
                    ax.plot(times, usages, color=cpu_color[cpu_name], linewidth=1, label=cpu_name)
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3)
            if col == 0:
                ax.set_ylabel(f"{cfg[0]}c_{cfg[1]}vc_r{cfg[2]}")
            if row == 0:
                ax.legend(fontsize=7, ncol=4)

    for ax in axes[-1]:
        ax.set_xlabel("Time (s)")
    fig.suptitle("CPU Usage", fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    out_path = os.path.join(out_dir, "cpu.png")
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
