#!/usr/bin/env python3
"""Plot throughput-latency curves across optimization configurations.

Examples:
  scripts/plot_throughput_latency.py Paxos
  scripts/plot_throughput_latency.py CAS write100
  scripts/plot_throughput_latency.py CAS --workload write0 --metric p99

If deployment produced repeated runs for the same client count, the plot places
the marker at the mean across runs and shades a +/-1 standard-deviation band
around each curve. When a client count has more than 3 runs, some are likely
unstable, so only the 3 highest-throughput runs are kept.
"""

import argparse
import csv
import glob
import os
import re
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update(
    {
        "font.size": 15,
        "axes.labelsize": 15,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "legend.fontsize": 12,
        "legend.title_fontsize": 12,
    }
)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "benchmark_results")
START_S, END_S = 30, 59  # matches START_MEASUREMENT_SECOND / MEASUREMENT_SECOND

FNAME_RE = re.compile(r"client_aggregator_(\d+)c_(\d+)vc_r(\d+)\.csv$")
OPT_RE = re.compile(r"opt(\d+)$")


def config_sort_key(config):
    if config == "default":
        return (0, 0)
    match = OPT_RE.fullmatch(config)
    if match:
        return (1, int(match.group(1)))
    return (2, config)


def parse_result_dir(protocol, name):
    if not name.startswith(f"{protocol}_"):
        return None

    parts = name.split("_")
    if len(parts) < 3:
        return None

    suffix = parts[-1]
    if suffix != "none":
        return None

    if parts[1] == "default":
        return "default", "default"

    if OPT_RE.fullmatch(parts[1]):
        if len(parts) < 4:
            return None
        return parts[1], "_".join(parts[2:-1])

    return "default", "_".join(parts[1:-1])


def find_config_dirs(protocol, workload):
    dirs = []
    for path in glob.glob(os.path.join(RESULTS_DIR, f"{protocol}_*")):
        if not os.path.isdir(path):
            continue
        parsed = parse_result_dir(protocol, os.path.basename(path))
        if parsed is None:
            continue
        config, dir_workload = parsed
        if dir_workload == workload:
            dirs.append((config, path))
    return sorted(dirs, key=lambda item: config_sort_key(item[0]))


def window_avg(path):
    """Average throughput and latency percentiles over the steady-state window."""
    with open(path) as f:
        rows = list(csv.DictReader(f))
    sel = [r for r in rows if START_S <= int(r["time_s"]) <= END_S]
    if not sel:
        return None
    n = len(sel)
    return {
        "throughput": sum(float(r["throughput_rps"]) for r in sel) / n,
        "p50": sum(float(r["latency_p50_ms"]) for r in sel) / n,
        "p99": sum(float(r["latency_p99_ms"]) for r in sel) / n,
        "p999": sum(float(r["latency_p999_ms"]) for r in sel) / n,
    }


def mean_std(values):
    """Sample mean and (unbiased) standard deviation; std is 0 for a single run."""
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, var**0.5


MAX_RUNS = 3


def aggregate_runs(runs):
    """Collapse repeated runs into one point: mean marker plus per-metric std."""
    point = {
        "vc": runs[0]["vc"],
        "clients": runs[0]["clients"],
        "num_runs": len(runs),
        "runs": sorted(r["run"] for r in runs),
    }
    for key in ("throughput", "p50", "p99", "p999"):
        mean, std = mean_std([r[key] for r in runs])
        point[key] = mean
        point[f"{key}_std"] = std
    return point


def collect_config(path):
    """Return one aggregated point per virtual-client count across repeated runs."""
    by_vc = defaultdict(list)
    for csv_path in glob.glob(os.path.join(path, "client_aggregator_*.csv")):
        match = FNAME_RE.search(os.path.basename(csv_path))
        if not match:
            continue
        physical_clients = int(match.group(1))
        virtual_clients = int(match.group(2))
        run_idx = int(match.group(3))
        agg = window_avg(csv_path)
        if agg is None:
            continue
        agg["vc"] = virtual_clients
        agg["clients"] = physical_clients * virtual_clients
        agg["run"] = run_idx
        by_vc[virtual_clients].append(agg)

    points = []
    for virtual_clients, runs in by_vc.items():
        # More than 3 runs implies some were unstable; keep the fastest 3.
        if len(runs) > MAX_RUNS:
            runs = sorted(runs, key=lambda p: p["throughput"], reverse=True)[:MAX_RUNS]
        points.append(aggregate_runs(runs))

    return sorted(points, key=lambda p: p["vc"])


def output_path(protocol, workload, metric, explicit_output):
    if explicit_output:
        return explicit_output
    basename = f"{protocol.lower()}_{workload}_throughput_latency_{metric}.pdf"
    return os.path.join(os.path.dirname(__file__), basename)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot throughput-latency curves for default/opt benchmark configs."
    )
    parser.add_argument("protocol", help="Protocol name, e.g. Paxos or CAS.")
    parser.add_argument(
        "workload_pos",
        nargs="?",
        help="Workload name, e.g. default, write0, or write100.",
    )
    parser.add_argument(
        "--workload",
        default=None,
        help="Workload name. Overrides the optional positional workload.",
    )
    parser.add_argument(
        "--metric",
        choices=["p50", "p99", "p999"],
        default="p50",
        help="Latency percentile to plot.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path. Defaults to scripts/<protocol>_<workload>_throughput_latency_<metric>.pdf.",
    )
    parser.add_argument(
        "--stop",
        default=None,
        help="Comma-separated point counts per config curve, in plotted order "
        "(default, opt1, ...). Each value keeps only that many lowest-client-count "
        "points, dropping the saturated tail (e.g. --stop 3,5,6). Leave an entry "
        "empty to skip a config (e.g. --stop ,,6); trailing configs are untouched.",
    )
    return parser.parse_args()


def parse_stops(raw):
    """Parse --stop into a positional list of point counts (None = no truncation)."""
    if not raw:
        return []
    stops = []
    for token in raw.split(","):
        token = token.strip()
        stops.append(int(token) if token else None)
    return stops


def main():
    args = parse_args()
    protocol = args.protocol
    workload = args.workload or args.workload_pos or "default"
    metric = args.metric

    config_dirs = find_config_dirs(protocol, workload)
    if not config_dirs:
        raise SystemExit(
            f"No benchmark result directories found for protocol={protocol!r}, workload={workload!r}"
        )

    data = [(config, collect_config(path)) for config, path in config_dirs]
    data = [(config, points) for config, points in data if points]
    if not data:
        raise SystemExit(
            f"No client_aggregator CSV data found for protocol={protocol!r}, workload={workload!r}"
        )

    stops = parse_stops(args.stop)
    if len(stops) > len(data):
        print(
            f"Warning: --stop has {len(stops)} values but only {len(data)} config "
            f"curves ({', '.join(config for config, _ in data)}); extra values ignored."
        )
    data = [
        (config, points[:stops[i]] if i < len(stops) and stops[i] is not None else points)
        for i, (config, points) in enumerate(data)
    ]

    fig, ax = plt.subplots(figsize=(8, 3.5))
    cmap = plt.get_cmap("tab10")

    for idx, (config, points) in enumerate(data):
        color = cmap(idx % 10)
        xs = [p["throughput"] / 1000 for p in points]
        ys = [p[metric] for p in points]
        x_lo = [(p["throughput"] - p["throughput_std"]) / 1000 for p in points]
        x_hi = [(p["throughput"] + p["throughput_std"]) / 1000 for p in points]
        ax.plot(
            xs,
            ys,
            "-o",
            color=color,
            label=config,
            markersize=4,
            linewidth=1.5,
        )
        # Shade +/-1 std of throughput across runs (zero-width where a single run exists).
        if any(p["throughput_std"] > 0 for p in points):
            ax.fill_betweenx(ys, x_lo, x_hi, color=color, alpha=0.2, linewidth=0)
        last = points[-1]
        ax.annotate(
            f"{int(last['clients'])}c",
            (last["throughput"], last[metric]),
            fontsize=7,
            xytext=(4, 4),
            textcoords="offset points",
        )

    ax.set_xlabel("Throughput (thousands of requests/s)")
    ax.set_ylabel(f"{metric} latency (ms)")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Configuration")
    fig.tight_layout()

    out = output_path(protocol, workload, metric, args.output)
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    for config, points in data:
        print(f"\n=== {config} ===")
        print(
            f"{'clients':>8} {'runs':>4} {'thr_rps':>10} {'thr_std':>9} "
            f"{'p50_ms':>8} {'p99_ms':>8}"
        )
        for p in points:
            print(
                f"{int(p['clients']):>8} {p['num_runs']:>4} {p['throughput']:>10.0f} "
                f"{p['throughput_std']:>9.0f} {p['p50']:>8.3f} {p['p99']:>8.3f}"
            )


if __name__ == "__main__":
    main()
