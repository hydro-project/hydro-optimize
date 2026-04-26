use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy_and_analyze::{
    MEASUREMENT_SECOND, MetricLogs, Optimizations, START_MEASUREMENT_SECOND,
};

#[derive(Default, Clone)]
pub struct RunMetadata {
    pub throughputs: Vec<usize>,
    pub latencies: Vec<(f64, f64, f64, u64)>, // per-second: (p50, p99, p999, count)
    pub counters: HashMap<usize, Vec<usize>>, // op_id -> per-second rate time series
    pub byte_sizes: HashMap<usize, Vec<u64>>, // op_id -> sampled byte sizes
    pub sar_stats: HashMap<LocationId, Vec<SarStats>>,
    /// Maps each LocationId → original op_ids assigned to it (populated when size_analysis is used)
    pub location_to_original_ops: HashMap<LocationId, Vec<usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BottleneckMetric {
    Cpu,
    Network,
    Memory,
    IO,
}

pub struct ResourceLimits {
    pub network_bytes_per_sec: f64,
    pub io_tps: f64,
    /// Resources that exceed this percentage will be considered bottlenecked
    pub bottleneck_threshold: f64,
}

impl RunMetadata {
    /// Returns all locations that are bottlenecked at the given measurement second,
    /// along with which metric(s) each is bottlenecked on.
    pub fn find_bottlenecks(
        &self,
        measurement_second: usize,
        limits: &ResourceLimits,
    ) -> Vec<(LocationId, HashSet<BottleneckMetric>)> {
        let mut result = Vec::new();
        for (location, stats) in &self.sar_stats {
            let Some(sar) = stats.get(measurement_second) else {
                continue;
            };
            let mut metrics = HashSet::new();

            let cpu_threshold = 100.0 * limits.bottleneck_threshold;
            println!(
                "  {:?} CPU: {:.1}% threshold={:.1}%",
                location, sar.cpu, cpu_threshold
            );
            if sar.cpu >= cpu_threshold {
                metrics.insert(BottleneckMetric::Cpu);
            }

            let net_threshold = limits.network_bytes_per_sec * limits.bottleneck_threshold;
            println!(
                "  {:?} Network: {:.0} B/s threshold={:.0} B/s",
                location, sar.network, net_threshold
            );
            if sar.network >= net_threshold {
                metrics.insert(BottleneckMetric::Network);
            }

            let mem_threshold = 100.0 * limits.bottleneck_threshold;
            println!(
                "  {:?} Memory: {:.1}% threshold={:.1}%",
                location, sar.memory, mem_threshold
            );
            if sar.memory >= mem_threshold {
                metrics.insert(BottleneckMetric::Memory);
            }

            let io_threshold = limits.io_tps * limits.bottleneck_threshold;
            println!(
                "  {:?} IO: {:.1} tps threshold={:.1} tps",
                location, sar.io, io_threshold
            );
            if sar.io >= io_threshold {
                metrics.insert(BottleneckMetric::IO);
            }

            if !metrics.is_empty() {
                result.push((location.clone(), metrics));
            }
        }
        result
    }

    /// Average throughput over the measurement window.
    pub fn avg_throughput(&self) -> usize {
        avg_over(&self.throughputs, |&v| v as f64) as usize
    }

    /// Average per-second counter rates over the measurement window for each op.
    pub fn avg_counters(&self) -> HashMap<usize, usize> {
        self.counters
            .iter()
            .map(|(&op_id, rates)| (op_id, avg_over(rates, |&v| v as f64) as usize))
            .collect()
    }

    /// Median byte size for a single op.
    pub fn median_byte_size(&self, op_id: usize) -> u64 {
        self.byte_sizes
            .get(&op_id)
            .map(|sizes| median(&mut sizes.clone()))
            .unwrap_or(0)
    }

    /// Average SAR stats over the measurement window for each location.
    pub fn avg_sar_stats(&self) -> HashMap<LocationId, SarStats> {
        self.sar_stats
            .iter()
            .map(|(loc, stats)| {
                (
                    loc.clone(),
                    SarStats {
                        cpu: avg_over(stats, |s| s.cpu),
                        network: avg_over(stats, |s| s.network),
                        memory: avg_over(stats, |s| s.memory),
                        io: avg_over(stats, |s| s.io),
                    },
                )
            })
            .collect()
    }

    /// Prints a human-readable summary of metrics at the given measurement second.
    pub fn print_run_summary(
        &self,
        location_id_to_cluster: &HashMap<LocationId, String>,
        measurement_second: usize,
    ) {
        if let Some(&throughput) = self.throughputs.get(measurement_second) {
            println!(
                "Throughput @{}s: {} requests/s",
                measurement_second + 1,
                throughput
            );
        }
        if let Some(&(p50, p99, p999, samples)) = self.latencies.get(measurement_second) {
            println!(
                "Latency @{}s: p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({} samples)",
                measurement_second + 1,
                p50,
                p99,
                p999,
                samples
            );
        }
        for (location, sar_stats) in &self.sar_stats {
            let name = location_id_to_cluster.get(location).unwrap();
            if let Some(sar) = sar_stats.get(measurement_second) {
                println!(
                    "{} @{}s: CPU {:.2}% | Net {:.0} B/s | Mem {:.2}% | IO {:.2} tps",
                    name,
                    measurement_second + 1,
                    sar.cpu,
                    sar.network,
                    sar.memory,
                    sar.io,
                );
            }
        }
    }

    /// Saves per-location CSVs and per-cluster profiling JSON files to `output_dir`.
    pub fn save_run_metadata(
        &self,
        location_id_to_cluster: &HashMap<LocationId, String>,
        output_dir: &Path,
        num_clients: usize,
        num_clients_per_node: usize,
        run: usize,
    ) {
        fs::create_dir_all(output_dir).ok();

        // Write per-location CSV
        for (location, stats) in &self.sar_stats {
            if stats.is_empty() {
                continue;
            }
            let location_name = location_id_to_cluster.get(location).unwrap();
            let filename = output_dir.join(format!(
                "{}_{}c_{}vc_r{}.csv",
                location_name, num_clients, num_clients_per_node, run
            ));
            let num_rows = stats
                .len()
                .max(self.throughputs.len())
                .max(self.latencies.len());

            let write_csv = || -> Result<(), Box<dyn std::error::Error>> {
                let mut file = File::create(&filename)?;
                writeln!(
                    file,
                    "time_s,cpu,network,memory,io,\
                     throughput_rps,latency_p50_ms,latency_p99_ms,latency_p999_ms,latency_samples",
                )?;

                let default_sar = SarStats::default();
                for i in 0..num_rows {
                    let sar = stats.get(i).unwrap_or(&default_sar);
                    let thr = self.throughputs.get(i).copied().unwrap_or(0);
                    let (p50, p99, p999, count) =
                        self.latencies.get(i).copied().unwrap_or_default();
                    writeln!(
                        file,
                        "{},{:.2},{:.2},{:.2},{:.2},{},{:.3},{:.3},{:.3},{}",
                        i, sar.cpu, sar.network, sar.memory, sar.io, thr, p50, p99, p999, count,
                    )?;
                }
                Ok(())
            };

            match write_csv() {
                Ok(()) => println!("Generated CSV: {}", filename.display()),
                Err(e) => eprintln!("Failed to write CSV {}: {}", filename.display(), e),
            }
        }
    }

    /// Saves median output byte size per decoupleable operator.
    /// `decoupleable_ops` is the set of op_ids where decoupling could insert a network.
    pub fn save_size_analysis(&self, output_dir: &Path, decoupleable_ops: &HashSet<usize>) {
        let sizes: HashMap<usize, u64> = decoupleable_ops
            .iter()
            .map(|&op_id| (op_id, self.median_byte_size(op_id)))
            .collect();
        save_json(output_dir, "size_analysis.json", &sizes);
    }

    /// Saves per-location blow-up stats: sar_stats, operator list, per-op counts.
    pub fn save_blow_up_stats(&self, output_dir: &Path) {
        let avg_counters = self.avg_counters();
        let avg_sar = self.avg_sar_stats();

        let mut location_stats: HashMap<String, BlowUpLocationStats> = HashMap::new();
        for (loc, ops) in &self.location_to_original_ops {
            if ops.is_empty() {
                continue;
            }
            let sar = avg_sar.get(loc).cloned().unwrap_or_default();
            let counts: HashMap<usize, usize> = ops
                .iter()
                .map(|&op| (op, avg_counters.get(&op).copied().unwrap_or(0)))
                .collect();
            location_stats.insert(
                format!("{:?}", loc),
                BlowUpLocationStats {
                    sar_stats: sar,
                    operators: ops.clone(),
                    counts,
                },
            );
        }

        save_json(output_dir, "blow_up_stats.json", &location_stats);
    }
}

/// Per-second SAR statistics
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct SarStats {
    /// Max single-core CPU usage (user + system %)
    pub cpu: f64,
    /// Network in + out bytes/sec
    pub network: f64,
    /// Memory percent used
    pub memory: f64,
    /// IO rtps + wtps
    pub io: f64,
}

impl SarStats {
    fn max(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.max(other.cpu),
            network: self.network.max(other.network),
            memory: self.memory.max(other.memory),
            io: self.io.max(other.io),
        }
    }

    fn lerp(self, other: Self, t: f64) -> Self {
        Self {
            cpu: self.cpu + t * (other.cpu - self.cpu),
            network: self.network + t * (other.network - self.network),
            memory: self.memory + t * (other.memory - self.memory),
            io: self.io + t * (other.io - self.io),
        }
    }

    /// Multiply each field by `s`.
    pub fn scale(self, s: f64) -> Self {
        Self {
            cpu: self.cpu * s,
            network: self.network * s,
            memory: self.memory * s,
            io: self.io * s,
        }
    }

    /// Subtract `other`, clamping each field to 0.
    pub fn sub_floor(self, other: Self) -> Self {
        Self {
            cpu: (self.cpu - other.cpu).max(0.0),
            network: (self.network - other.network).max(0.0),
            memory: (self.memory - other.memory).max(0.0),
            io: (self.io - other.io).max(0.0),
        }
    }

    pub fn add(&mut self, other: Self) {
        self.cpu += other.cpu;
        self.network += other.network;
        self.memory += other.memory;
        self.io += other.io;
    }
}

/// Per-location blow-up analysis output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlowUpLocationStats {
    pub sar_stats: SarStats,
    pub operators: Vec<usize>,
    pub counts: HashMap<usize, usize>,
}

/// Derives per-operator load for the ILP solver.
///
/// For each blown-up location, subtracts the network overhead introduced by blowing up
/// (using greedy analysis to identify where networks were inserted), then assigns the
/// full adjusted SarStats to the first operator of each location group.
///
/// Network costs are attributed to both sender and receiver locations. The byte size
/// used is the parent's output size (since `op_to_network` inserts BEFORE the op).
///
/// Returns `op_id → SarStats` load assignment. Will not contain entries for operators that were not the first in their blown-up location group.
pub fn derive_per_op_load(
    blow_up_stats: &HashMap<LocationId, BlowUpLocationStats>,
    op_output_sizes: &HashMap<usize, u64>,
    op_counts: &HashMap<usize, usize>,
    // op_id → (src_loc_idx, dst_loc_idx): greedy analysis would insert networks BEFORE this op
    op_to_network: &HashMap<usize, (usize, usize)>,
    // op_id → parent op_ids
    op_to_parents: &HashMap<usize, Vec<usize>>,
    calibration: &NetworkCostTable,
    // op_id → location index from the greedy rewrite
    op_to_loc: &HashMap<usize, usize>,
) -> HashMap<usize, SarStats> {
    // Build loc_idx → LocationId mapping from blow_up_stats
    // (location_to_original_ops maps LocationId → ops, op_to_loc maps op → loc_idx;
    //  we invert to get loc_idx → LocationId)
    let mut loc_idx_to_location: HashMap<usize, LocationId> = HashMap::new();
    for (loc, stats) in blow_up_stats {
        for &op_id in &stats.operators {
            if let Some(&loc_idx) = op_to_loc.get(&op_id) {
                loc_idx_to_location
                    .entry(loc_idx)
                    .or_insert_with(|| loc.clone());
            }
        }
    }

    // For each network insertion, compute cost and attribute to both sender and receiver locations.
    let mut loc_network_cost: HashMap<LocationId, SarStats> = HashMap::new();
    for (&op_id, &(src_loc_idx, dst_loc_idx)) in op_to_network {
        // The network sends the parent's output, so use parent's size and count.
        // If there are multiple parents, each contributes a network.
        let parents = op_to_parents.get(&op_id).cloned().unwrap();
        for parent in parents {
            let count = op_counts.get(&parent).copied().unwrap();
            let size = op_output_sizes.get(&parent).copied().unwrap();
            let cost = calibration.network_cost(count, size);

            for loc_idx in [src_loc_idx, dst_loc_idx] {
                if let Some(loc) = loc_idx_to_location.get(&loc_idx) {
                    loc_network_cost.entry(loc.clone()).or_default().add(cost);
                }
            }
        }
    }

    // Subtract network overhead from each location's SarStats
    let loc_to_no_network_stats: HashMap<LocationId, SarStats> = blow_up_stats
        .iter()
        .map(|(loc, stats)| {
            let net_cost = loc_network_cost.get(loc).copied().unwrap();
            (loc.clone(), stats.sar_stats.sub_floor(net_cost))
        })
        .collect();

    // Assign load: first op of each location group gets the full adjusted SarStats
    let mut per_op_load: HashMap<usize, SarStats> = HashMap::new();
    for (loc, stats) in blow_up_stats {
        let no_network_stats = loc_to_no_network_stats.get(loc).copied().unwrap();
        let first_op = stats.operators.first().unwrap();
        per_op_load.insert(*first_op, no_network_stats);
    }

    per_op_load
}

/// Parses `sar -n DEV -u -P ALL -r -b` output lines and returns per-second SarStats.
pub fn parse_sar_output(lines: Vec<String>) -> Vec<SarStats> {
    let cpu_regex = Regex::new(
        r"\s(all|\d{1,3})\s+(\d+\.?\d*)\s+\d+\.?\d*\s+(\d+\.?\d*)\s+\d+\.?\d*\s+\d+\.?\d*\s+(\d+\.?\d*)$",
    ).unwrap();
    let iface_regex =
        Regex::new(r"([a-zA-Z]\S*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)")
            .unwrap();
    let mem_regex =
        Regex::new(r"\d+:\d+:\d+\s+(\d+)\s+\d+\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+)\s+(\d+)")
            .unwrap();
    let io_re7 = Regex::new(
        r"\d+:\d+:\d+\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+\d+\.?\d*\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+\d+\.?\d*\s*$",
    ).unwrap();
    let io_re5 = Regex::new(
        r"\d+:\d+:\d+\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s*$",
    )
    .unwrap();

    let mut result: Vec<SarStats> = vec![];

    for line in &lines {
        // CPU: "all" marks a new second, per-core lines update the max
        if let Some(caps) = cpu_regex.captures(line) {
            let is_all = &caps[1] == "all";
            if let (Some(user), Some(system)) =
                (caps[2].parse::<f64>().ok(), caps[3].parse::<f64>().ok())
            {
                let usage = user + system;
                if is_all {
                    result.push(SarStats {
                        cpu: usage,
                        ..Default::default()
                    });
                } else if let Some(last) = result.last_mut() {
                    last.cpu = last.cpu.max(usage);
                }
            }
            continue;
        }

        // Network
        if let Some(caps) = iface_regex.captures(line) {
            let iface = &caps[1];
            if iface != "lo"
                && iface != "docker0"
                && iface != "IFACE"
                && let (Some(rx_kb), Some(tx_kb)) =
                    (caps[4].parse::<f64>().ok(), caps[5].parse::<f64>().ok())
                && let Some(last) = result.last_mut()
            {
                last.network = (rx_kb + tx_kb) * 1024.0;
            }
            continue;
        }

        // Memory
        if !line.contains("kbmemfree")
            && let Some(caps) = mem_regex.captures(line)
        {
            if let Some(kbmemfree) = caps[1].parse::<f64>().ok()
                && kbmemfree >= 10000.0
                && let Some(pct) = caps[3].parse::<f64>().ok()
                && let Some(last) = result.last_mut()
            {
                last.memory = pct;
            }
            continue;
        }

        // IO
        if !line.contains("tps") {
            let io_val = io_re7
                .captures(line)
                .and_then(|caps| Some(caps[2].parse::<f64>().ok()? + caps[3].parse::<f64>().ok()?))
                .or_else(|| {
                    io_re5.captures(line).and_then(|caps| {
                        Some(caps[2].parse::<f64>().ok()? + caps[3].parse::<f64>().ok()?)
                    })
                });
            if let Some(io) = io_val
                && let Some(last) = result.last_mut()
            {
                last.io = io;
            }
        }
    }

    result
}

/// Parses throughput output from `print_parseable_bench_results`.
/// Format: "HYDRO_OPTIMIZE_THR: {throughput} requests/s"
/// Returns all per-second throughput values found.
pub fn parse_throughput(lines: Vec<String>) -> Vec<usize> {
    let regex = Regex::new(r"(\d+\.?\d*)\s*requests/s").unwrap();
    lines
        .iter()
        .filter_map(|line| {
            regex
                .captures(line)
                .map(|cap| cap[1].parse::<f64>().unwrap() as usize)
        })
        .collect()
}

/// Parses latency output from `print_parseable_bench_results`.
/// Format: "HYDRO_OPTIMIZE_LAT: p50: {p50:.3} | p99 {p99:.3} | p999 {p999:.3} ms ({num_samples} samples)"
/// Returns all per-second (p50, p99, p999, num_samples) tuples found.
pub fn parse_latency(lines: Vec<String>) -> Vec<(f64, f64, f64, u64)> {
    let regex = Regex::new(r"p50:\s*(\d+\.?\d*)\s*\|\s*p99\s+(\d+\.?\d*)\s*\|\s*p999\s+(\d+\.?\d*)\s*ms\s*\((\d+)\s*samples\)").unwrap();
    lines
        .iter()
        .filter_map(|line| {
            regex.captures(line).map(|cap| {
                (
                    cap[1].parse::<f64>().unwrap(),
                    cap[2].parse::<f64>().unwrap(),
                    cap[3].parse::<f64>().unwrap(),
                    cap[4].parse::<u64>().unwrap(),
                )
            })
        })
        .collect()
}

/// Parses cumulative counter lines into per-second rates for each op.
/// Returns op_id → Vec of per-second counts (delta between consecutive cumulative values).
pub fn parse_counter_usage(lines: Vec<String>) -> HashMap<usize, Vec<usize>> {
    let regex = Regex::new(r"\((\d+)\): (\d+)").unwrap();
    let mut op_to_cumulative_count: HashMap<usize, Vec<usize>> = HashMap::new();
    for measurement in lines {
        let matches = regex.captures_iter(&measurement).last().unwrap();
        let op_id = matches[1].parse::<usize>().unwrap();
        let count = matches[2].parse::<usize>().unwrap();
        op_to_cumulative_count.entry(op_id).or_default().push(count);
    }
    // Convert cumulative to per-second deltas
    let mut result = HashMap::new();
    for (op_id, cumulative_count) in op_to_cumulative_count {
        let mut rates = vec![];
        for i in 1..cumulative_count.len() {
            // Subtraction where min = 0
            rates.push(cumulative_count[i].saturating_sub(cumulative_count[i - 1]));
        }
        result.insert(op_id, rates);
    }
    result
}

/// Lookup table mapping message size (bytes) → per-message resource costs.
/// Built from calibration runs at various message sizes. Interpolates linearly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkCostTable {
    /// Sorted by message size. Each entry is (message_size, cost_per_message).
    entries: Vec<(u64, SarStats)>,
}

impl NetworkCostTable {
    pub fn from_calibration(mut entries: Vec<(u64, SarStats)>) -> Self {
        entries.sort_by_key(|(size, _)| *size);
        assert!(
            !entries.is_empty(),
            "NetworkCostTable requires at least one calibration point"
        );
        Self { entries }
    }

    /// Returns the interpolated cost per message for the given message size.
    /// Clamps to the nearest endpoint if outside the calibrated range.
    pub fn cost_per_message(&self, message_size_bytes: u64) -> SarStats {
        let n = self.entries.len();
        if n == 1 || message_size_bytes <= self.entries[0].0 {
            return self.entries[0].1;
        }
        if message_size_bytes >= self.entries[n - 1].0 {
            return self.entries[n - 1].1;
        }
        let i = self
            .entries
            .partition_point(|(s, _)| *s <= message_size_bytes);
        let (s0, c0) = self.entries[i - 1];
        let (s1, c1) = self.entries[i];
        let t = (message_size_bytes - s0) as f64 / (s1 - s0) as f64;
        c0.lerp(c1, t)
    }

    /// Total resource cost for sending `count` messages of `message_size_bytes` each.
    pub fn network_cost(&self, count: usize, message_size_bytes: u64) -> SarStats {
        self.cost_per_message(message_size_bytes)
            .scale(count as f64)
    }
}

/// Parses byte-size measurement lines of the form `_optimize_byte_size(op_id): size`.
pub fn parse_byte_sizes(lines: Vec<String>) -> HashMap<usize, Vec<u64>> {
    let regex = Regex::new(r"\((\d+)\): (\d+)").unwrap();
    let mut op_to_sizes: HashMap<usize, Vec<u64>> = HashMap::new();
    for line in lines {
        if let Some(cap) = regex.captures(&line) {
            let op_id = cap[1].parse::<usize>().unwrap();
            let size = cap[2].parse::<u64>().unwrap();
            op_to_sizes.entry(op_id).or_default().push(size);
        }
    }
    op_to_sizes
}

/// Computes median of a sorted slice.
fn median(sorted: &mut [u64]) -> u64 {
    sorted.sort_unstable();
    if sorted.is_empty() {
        0
    } else {
        sorted[sorted.len() / 2]
    }
}

/// Drains all currently available messages from a receiver into a Vec.
async fn drain_receiver(receiver: &mut UnboundedReceiver<String>) -> Vec<String> {
    let mut lines = Vec::new();
    if receiver.is_empty() {
        // If the receiver is empty but not closed, calling recv() will block.
        return lines;
    }
    while let Some(line) = receiver.recv().await {
        lines.push(line);
    }
    lines
}

/// Serialize `value` as pretty JSON and write to `dir/filename`.
fn save_json(dir: &Path, filename: &str, value: &impl Serialize) {
    fs::create_dir_all(dir).ok();
    let path = dir.join(filename);
    let json = serde_json::to_string_pretty(value).expect("failed to serialize JSON");
    match fs::write(&path, json) {
        Ok(()) => println!("Saved {}", path.display()),
        Err(e) => eprintln!("Failed to save {}: {}", path.display(), e),
    }
}

/// Average of `f(item)` over the measurement window
fn avg_over<T>(slice: &[T], f: impl Fn(&T) -> f64) -> f64 {
    assert!(
        slice.len() > MEASUREMENT_SECOND,
        "measurement window [{}, {}] out of range for slice of length {}",
        START_MEASUREMENT_SECOND,
        MEASUREMENT_SECOND,
        slice.len()
    );
    let window = &slice[START_MEASUREMENT_SECOND..=MEASUREMENT_SECOND];
    window.iter().map(&f).sum::<f64>() / window.len() as f64
}

/// Element-wise max merge of a time series, extending `dst` as needed.
fn merge_max_vec<T: Clone>(dst: &mut Vec<T>, src: &[T], max_fn: fn(T, T) -> T) {
    for (i, s) in src.iter().enumerate() {
        if i >= dst.len() {
            dst.push(s.clone());
        } else {
            dst[i] = max_fn(dst[i].clone(), s.clone());
        }
    }
}

pub async fn analyze_cluster_results(
    nodes: &DeployResult<'_, HydroDeploy>,
    mut cluster_metrics: HashMap<(LocationId, String, usize), MetricLogs>,
    optimizations: &Optimizations,
) -> RunMetadata {
    let mut run_metadata = RunMetadata::default();
    let mut op_to_count: HashMap<usize, Vec<usize>> = HashMap::new();

    // Drain all receivers and parse in parallel across all nodes
    let mut set = tokio::task::JoinSet::new();
    for ((location, name, idx), mut metrics) in cluster_metrics.drain() {
        set.spawn(async move {
            println!("Analyzing cluster {:?}: {}", name, idx);
            let (sar_stats, op_to_count, throughputs, latencies, byte_sizes) = tokio::join!(
                async { parse_sar_output(drain_receiver(&mut metrics.sar).await) },
                async { parse_counter_usage(drain_receiver(&mut metrics.counters).await) },
                async { parse_throughput(drain_receiver(&mut metrics.throughputs).await) },
                async { parse_latency(drain_receiver(&mut metrics.latencies).await) },
                async { parse_byte_sizes(drain_receiver(&mut metrics.byte_sizes).await) },
            );
            println!("Parsed stats from cluster {:?}: {}", name, idx);
            (
                (location, name, idx),
                sar_stats,
                op_to_count,
                throughputs,
                latencies,
                byte_sizes,
            )
        });
    }

    // Join metric drain tasks
    let mut drained: HashMap<(LocationId, String), Vec<_>> = HashMap::new();
    while let Some(result) = set.join_next().await {
        let ((id, name, _idx), sar_stats, op_to_count, throughputs, latencies, byte_sizes) =
            result.unwrap();
        drained.entry((id, name)).or_default().push((
            sar_stats,
            op_to_count,
            throughputs,
            latencies,
            byte_sizes,
        ));
    }

    // Merge metrics across nodes in each cluster
    for (id, name, _cluster) in nodes.get_all_clusters() {
        if optimizations.excludes(&id) {
            continue;
        }

        let cluster_data = drained.get(&(id.clone(), name.to_string())).unwrap();

        let mut max_sar: Vec<SarStats> = vec![];
        let mut max_counters: HashMap<usize, Vec<usize>> = HashMap::new();

        for (sar_stats, counters, _, _, byte_sizes) in cluster_data {
            merge_max_vec(&mut max_sar, sar_stats, SarStats::max);
            for (op_id, rates) in counters {
                let entry = max_counters.entry(*op_id).or_default();
                merge_max_vec(entry, rates, usize::max);
            }
            for (op_id, sizes) in byte_sizes {
                run_metadata
                    .byte_sizes
                    .entry(*op_id)
                    .or_default()
                    .extend(sizes);
            }
        }

        for (op_id, rates) in max_counters {
            let entry = op_to_count.entry(op_id).or_default();
            merge_max_vec(entry, &rates, usize::max);
        }

        if !max_sar.is_empty() {
            run_metadata.sar_stats.insert(id.clone(), max_sar);
        }
    }

    // Collect throughput/latency from all processes (aggregator outputs these)
    for node_data in drained.into_values().flatten() {
        let (_, _, throughputs, latencies, _) = node_data;
        let has_throughput = !throughputs.is_empty();
        run_metadata.throughputs.extend(throughputs);
        run_metadata.latencies.extend(latencies);
        if has_throughput {
            // Found aggregator, we're done
            break;
        }
    }

    run_metadata.counters = op_to_count;
    run_metadata
}
