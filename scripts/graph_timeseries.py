#!/usr/bin/env python3
"""
Benchmark Timeseries Visualization

Takes a CSV file (from benchmark_results) and plots a timeseries with 5 subplots:
  1. Total CPU usage: (cpu_user + cpu_system) * 2
  2. Network usage in GB/s: (tx_bytes + rx_bytes) per sec
  3. Throughput (rps)
  4. p50 latency (ms)
  5. p99 latency (ms)
"""

import argparse
import csv
import sys
import matplotlib.pyplot as plt
import os


def read_csv(filepath):
    times, cpu, net_gb, throughput, p50, p99 = [], [], [], [], [], []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = float(row["time_s"])
            cpu_total = (float(row["cpu_user"]) + float(row["cpu_system"])) * 2
            net_bytes = float(row["network_tx_bytes_per_sec"]) + float(row["network_rx_bytes_per_sec"])
            net_gbs = net_bytes / 1e9

            times.append(t)
            cpu.append(cpu_total)
            net_gb.append(net_gbs)
            throughput.append(float(row["throughput_rps"]))
            p50.append(float(row["latency_p50_ms"]))
            p99.append(float(row["latency_p99_ms"]))
    return times, cpu, net_gb, throughput, p50, p99


def plot(times, cpu, net_gb, throughput, p50, p99, title, output):
    fig, axes = plt.subplots(5, 1, figsize=(12, 14), sharex=True)

    configs = [
        (cpu, "Total CPU (%)", "tab:red"),
        (net_gb, "Network (GB/s)", "tab:blue"),
        (throughput, "Throughput (rps)", "tab:green"),
        (p50, "p50 Latency (ms)", "tab:orange"),
        (p99, "p99 Latency (ms)", "tab:purple"),
    ]

    for ax, (data, label, color) in zip(axes, configs):
        ax.plot(times, data, color=color, linewidth=1.5)
        ax.set_ylabel(label, fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.fill_between(times, data, alpha=0.15, color=color)

    axes[-1].set_xlabel("Time (s)", fontsize=12)
    fig.suptitle(title, fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(output, dpi=200, bbox_inches="tight")
    print(f"Saved to {output}")
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark CSV timeseries")
    parser.add_argument("csv_file", help="Path to the benchmark CSV file")
    parser.add_argument("-o", "--output", default=None, help="Output image path (default: <input>_timeseries.png)")
    args = parser.parse_args()

    if not os.path.isfile(args.csv_file):
        print(f"Error: {args.csv_file} not found", file=sys.stderr)
        sys.exit(1)

    output = args.output or os.path.splitext(args.csv_file)[0] + "_timeseries.png"
    title = os.path.basename(args.csv_file)

    times, cpu, net_gb, throughput, p50, p99 = read_csv(args.csv_file)
    plot(times, cpu, net_gb, throughput, p50, p99, title, output)


if __name__ == "__main__":
    main()
