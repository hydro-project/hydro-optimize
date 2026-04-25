Hydro Optimize — Automated Protocol Optimization
=================================================

This script automates the process of profiling a distributed protocol,
identifying bottlenecks, and applying decoupling/partitioning rewrites
to improve throughput.

Prerequisites
-------------
- Rust toolchain (cargo)
- cmake (for the HiGHS ILP solver)
- Python 3 (for parsing calibration results)
- AWS credentials configured (if using --aws)
- Or a GCP project (if using --gcp)

Quick Start
-----------
    cd hydro-optimize
    ./scripts/optimize.sh --aws

This runs the full pipeline for Paxos on AWS. Replace --aws with
--gcp YOUR_PROJECT for GCP.

Usage
-----
    ./scripts/optimize.sh [OPTIONS]

    Options:
      --aws                 Deploy on AWS (default if no cloud specified)
      --gcp PROJECT         Deploy on GCP with the given project name
      --skip-calibration    Skip network calibration if results already exist

Pipeline Steps
--------------
1. NETWORK CALIBRATION (one-time)
   Launches parallel benchmark runs with message sizes 1B to 4KB.
   Measures throughput and network bytes via SAR to compute cost-per-byte
   at each message size. Results are saved to:
     optimization_results/network_calibration.json

   To re-run calibration, delete that file or omit --skip-calibration.

2. BASELINE BENCHMARK
   Runs benchmark_paxos without counter instrumentation (--no-counters)
   to measure unmodified protocol throughput.

3. CONCURRENT ANALYSIS (runs in parallel)
   a) Counter run: benchmark_paxos with counters to measure per-operator
      cardinality (elements/second).
   b) Decouple analysis run: benchmark_paxos --decouple-analysis which:
      - Runs greedy_decouple_analysis on all non-excluded locations
      - Inserts byte-size inspection (Inspect nodes) at network boundaries
      - Measures per-operator output tuple sizes via sampling

4. COMBINE METRICS & OPTIMIZE
   Combines counter data, byte-size data, SAR metrics, and calibration
   into OperatorMetrics (per-operator cost, cardinality, output bytes).
   Finds the bottleneck location via find_bottlenecks().
   Runs the ILP with increasing machine budgets (2, 3, ...) trying both
   pure decoupling and decouple+partition configurations.

5. ITERATIVE IMPROVEMENT
   Applies the best rewrite (fewest machines that reduces bottleneck cost),
   re-benchmarks, and repeats until throughput stops improving.

Output
------
All results are saved under:
    optimization_results/YYYY-MM-DD_HH-MM-SS/

Including:
  - calibration/       Network calibration logs
  - baseline.log       Baseline benchmark output
  - counters.log       Counter instrumentation output
  - byte_sizes.log     Byte-size inspection output

Individual Benchmark Flags
--------------------------
benchmark_paxos supports these flags directly:

    cargo run --release --example benchmark_paxos -- [OPTIONS]

    --aws                 Deploy on AWS
    --gcp PROJECT         Deploy on GCP
    --no-counters         Disable counter instrumentation (faster baseline)
    --decouple-analysis   Run greedy decouple + byte-size inspection

network_calibrator supports:

    cargo run --release --example network_calibrator -- [OPTIONS]

    --aws                 Deploy on AWS
    --gcp PROJECT         Deploy on GCP
    --message-size N      Message size in bytes (default: 8)
