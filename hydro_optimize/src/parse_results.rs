use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::HydroRoot;
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy_and_analyze::{
    MEASUREMENT_SECOND, MetricLogs, START_MEASUREMENT_SECOND, inject_inferred_counters,
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
    /// Per-operator CPU usage fractions from perf: op_id → fraction of total samples.
    pub perf: HashMap<usize, f64>,
}

impl RunMetadata {
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
    pub fn median_byte_size(&self, op_id: usize) -> Option<u64> {
        self.byte_sizes
            .get(&op_id)
            .map(|sizes| median(&mut sizes.clone()))
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
                        cpu_user: avg_over(stats, |s| s.cpu_user),
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
            let default_name = format!("{:?}", location);
            let name = location_id_to_cluster
                .get(location)
                .unwrap_or(&default_name);
            if let Some(sar) = sar_stats.get(measurement_second) {
                println!(
                    "{} @{}s: CPU {:.2}% (user {:.2}%) | Net {:.0} B/s | Mem {:.2}% | IO {:.2} tps",
                    name,
                    measurement_second + 1,
                    sar.cpu,
                    sar.cpu_user,
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
            let default_name = format!("{:?}", location);
            let location_name = location_id_to_cluster
                .get(location)
                .unwrap_or(&default_name);
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
                    "time_s,cpu,cpu_user,network,memory,io,\
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
                        "{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{:.3},{:.3},{:.3},{}",
                        i,
                        sar.cpu,
                        sar.cpu_user,
                        sar.network,
                        sar.memory,
                        sar.io,
                        thr,
                        p50,
                        p99,
                        p999,
                        count,
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

    /// Saves per-location perf profiling data to a JSON file.
    pub fn save_perf(
        &self,
        output_dir: &Path,
        num_clients: usize,
        num_virtual: usize,
        run: usize,
    ) {
        if self.perf.is_empty() {
            return;
        }
        save_json(
            output_dir,
            &format!("perf_{}c_{}vc_r{}.json", num_clients, num_virtual, run),
            &self.perf,
        );
    }

    /// Saves median output byte size per decoupleable operator and existing network nodes.
    pub fn save_size_analysis(&self, output_dir: &Path, decoupleable_ops: &HashSet<usize>) {
        let sizes: HashMap<usize, u64> = self
            .byte_sizes
            .keys()
            .chain(decoupleable_ops.iter())
            .filter_map(|&op_id| self.median_byte_size(op_id).map(|size| (op_id, size)))
            .collect();
        save_json(output_dir, "size_analysis.json", &sizes);
    }

    /// Saves average per-op counters over the measurement window to a JSON file.
    pub fn save_counters(&self, output_dir: &Path) {
        let avg = self.avg_counters();
        save_json(output_dir, "counters.json", &avg);
    }

    /// Saves per-location blow-up stats: sar_stats, operator list, per-op counts.
    pub fn save_blow_up_stats(
        &self,
        output_dir: &Path,
        ir: &mut [HydroRoot],
        pre_rewrite_parents: &HashMap<usize, Vec<usize>>,
        num_clients: usize,
        num_virtual: usize,
        run: usize,
    ) {
        let mut avg_counters = self.avg_counters();
        inject_inferred_counters(ir, pre_rewrite_parents, &mut avg_counters);
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

        save_json(
            output_dir,
            &format!(
                "blow_up_stats_{}c_{}vc_r{}.json",
                num_clients, num_virtual, run
            ),
            &location_stats,
        );
    }
}

/// Per-second SAR statistics
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct SarStats {
    /// Max single-core CPU usage (user + system %)
    pub cpu: f64,
    /// Max single-core CPU user % only (excludes system)
    pub cpu_user: f64,
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
            cpu_user: self.cpu_user.max(other.cpu_user),
            network: self.network.max(other.network),
            memory: self.memory.max(other.memory),
            io: self.io.max(other.io),
        }
    }

    fn lerp(self, other: Self, t: f64) -> Self {
        Self {
            cpu: self.cpu + t * (other.cpu - self.cpu),
            cpu_user: self.cpu_user + t * (other.cpu_user - self.cpu_user),
            network: self.network + t * (other.network - self.network),
            memory: self.memory + t * (other.memory - self.memory),
            io: self.io + t * (other.io - self.io),
        }
    }

    /// Multiply each field by `s`.
    pub fn scale(self, s: f64) -> Self {
        Self {
            cpu: self.cpu * s,
            cpu_user: self.cpu_user * s,
            network: self.network * s,
            memory: self.memory * s,
            io: self.io * s,
        }
    }

    pub fn add(&mut self, other: Self) {
        self.cpu += other.cpu;
        self.cpu_user += other.cpu_user;
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

/// Derives per-operator resource load for the ILP solver.
///
/// - Non-network ops: CPU = perf fraction × 100 (direct measurement on saturated machine)
/// - Network ops: CPU = count × cost_per_message from calibration (accounts for ser/deser + kernel)
/// - Memory and IO costs are added from blow-up stats (assigned to first op of each location group)
pub fn derive_per_op_load(
    perf: &HashMap<usize, f64>,
    network_op_ids: &HashSet<usize>,
    blow_up_stats: &HashMap<String, BlowUpLocationStats>,
) -> HashMap<usize, SarStats> {
    let mut result: HashMap<usize, SarStats> = HashMap::new();

    for (&op_id, &fraction) in perf {
        // Perf not calculated for network ops
        if !network_op_ids.contains(&op_id) {
            let entry = result.entry(op_id).or_default();
            entry.cpu += fraction * 100.0;
            entry.cpu_user += fraction * 100.0;
        }
    }

    // Add memory and IO costs from blow-up stats. Assign to 1st op; it's ok since they'll always decouple together
    for stats in blow_up_stats.values() {
        if let Some(&first_op) = stats.operators.first() {
            let entry = result.entry(first_op).or_default();
            entry.memory += stats.sar_stats.memory;
            entry.io += stats.sar_stats.io;
        }
    }

    result
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
                        cpu_user: user,
                        ..Default::default()
                    });
                } else if let Some(last) = result.last_mut()
                    && usage > last.cpu
                {
                    last.cpu = usage;
                    last.cpu_user = user;
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
        // Scale by 0.5: calibration measures round-trips but we want cost per message (one direction)
        for (_, stats) in &mut entries {
            *stats = stats.scale(0.5);
        }
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

/// Finds the bottleneck cluster from a run folder's CSV data.
/// 1. Finds the run suffix (e.g. "10c_500vc_r0") with highest avg throughput.
/// 2. For that run, reads all cluster CSVs and returns the cluster name whose SAR stats
///    are closest to saturation (100% CPU, 100% memory, or at/above max network/IO).
pub fn find_bottleneck_from_run(
    run_dir: &Path,
    network_bytes_per_sec: f64,
    io_tps: f64,
) -> String {
    let csv_re = Regex::new(r"^(.+)_(\d+c_\d+vc_r\d+)\.csv$").unwrap();

    // Group CSVs by suffix
    let mut suffix_to_files: HashMap<String, Vec<(String, PathBuf)>> = HashMap::new();
    for entry in fs::read_dir(run_dir)
        .unwrap_or_else(|e| panic!("Failed to read run dir {}: {}", run_dir.display(), e))
        .filter_map(|e| e.ok())
    {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(caps) = csv_re.captures(&name) {
            let cluster_name = caps[1].to_string();
            let suffix = caps[2].to_string();
            suffix_to_files
                .entry(suffix)
                .or_default()
                .push((cluster_name, entry.path()));
        }
    }

    // Find suffix with highest throughput (use first CSV that has throughput data)
    let best_suffix = suffix_to_files
        .keys()
        .max_by_key(|suffix| {
            suffix_to_files[*suffix]
                .iter()
                .map(|(_, path)| csv_avg_throughput(path))
                .max()
                .unwrap_or(0)
        })
        .expect("No CSVs found in run dir")
        .clone();

    // For the best suffix, score each cluster by saturation
    let score = |s: &SarStats| -> f64 {
        [
            s.cpu / 100.0,
            s.memory / 100.0,
            if network_bytes_per_sec > 0.0 { s.network / network_bytes_per_sec } else { 0.0 },
            if io_tps > 0.0 { s.io / io_tps } else { 0.0 },
        ]
        .into_iter()
        .fold(0.0_f64, f64::max)
    };

    let (bottleneck_name, _) = suffix_to_files[&best_suffix]
        .iter()
        .map(|(cluster_name, path)| {
            let avg_sar = csv_avg_sar(path);
            (cluster_name.clone(), score(&avg_sar))
        })
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
        .expect("No cluster CSVs for best suffix");

    println!(
        "Bottleneck from run: {} (suffix: {})",
        bottleneck_name, best_suffix
    );
    bottleneck_name
}

/// Reads a CSV and returns the average SAR stats over the measurement window.
fn csv_avg_sar(path: &Path) -> SarStats {
    let content = fs::read_to_string(path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    let start = START_MEASUREMENT_SECOND + 1; // +1 for header
    let end = MEASUREMENT_SECOND + 1;
    if lines.len() <= end {
        return SarStats::default();
    }
    let header: Vec<&str> = lines[0].split(',').collect();
    let col = |name: &str| header.iter().position(|&h| h == name);
    let cpu_col = col("cpu").unwrap();
    let net_col = col("network").unwrap();
    let mem_col = col("memory").unwrap();
    let io_col = col("io").unwrap();

    let n = (end - start + 1) as f64;
    let mut sum = SarStats::default();
    for line in &lines[start..=end] {
        let fields: Vec<&str> = line.split(',').collect();
        sum.cpu += fields[cpu_col].parse::<f64>().unwrap_or(0.0);
        sum.network += fields[net_col].parse::<f64>().unwrap_or(0.0);
        sum.memory += fields[mem_col].parse::<f64>().unwrap_or(0.0);
        sum.io += fields[io_col].parse::<f64>().unwrap_or(0.0);
    }
    SarStats {
        cpu: sum.cpu / n,
        cpu_user: 0.0,
        network: sum.network / n,
        memory: sum.memory / n,
        io: sum.io / n,
    }
}


/// Reads a benchmark CSV and returns the average throughput over the measurement window.
/// Returns 0 if the file is too short or has no throughput data.
fn csv_avg_throughput(path: &Path) -> usize {
    let content = fs::read_to_string(path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    let start = START_MEASUREMENT_SECOND + 1; // +1 for header
    let end = MEASUREMENT_SECOND + 1;
    if lines.len() <= end {
        return 0;
    }
    let header: Vec<&str> = lines[0].split(',').collect();
    let thr_col = header.iter().position(|&h| h == "throughput_rps").unwrap();
    let n = (end - start + 1) as f64;
    let sum: f64 = lines[start..=end]
        .iter()
        .map(|line| line.split(',').nth(thr_col).unwrap().parse::<f64>().unwrap())
        .sum();
    (sum / n) as usize
}

/// Finds the most recent directory in `base_dir` whose name starts with `prefix`.
/// "Most recent" = lexicographically last (works because timestamps are YYYY-MM-DD_HH-MM-SS).
pub fn find_latest_dir(base_dir: &Path, prefix: &str) -> PathBuf {
    fs::read_dir(base_dir)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", base_dir.display(), e))
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
        .max_by_key(|e| e.file_name().to_string_lossy().to_string())
        .unwrap_or_else(|| panic!("No directory matching '{}*' in {}", prefix, base_dir.display()))
        .path()
}

/// Locates the blow_up_stats JSON, size_analysis JSON, counters JSON, and perf JSON for a given
/// benchmark name (e.g. "Paxos") by finding the latest directory of each type.
/// Returns (blow_up_stats_path, size_analysis_path, counters_path, perf_path).
/// ILP input data loaded from benchmark results.
pub struct IlpInputs {
    pub blow_up_stats: HashMap<String, BlowUpLocationStats>,
    pub op_output_sizes: HashMap<usize, u64>,
    pub op_counts: HashMap<usize, usize>,
    pub perf: HashMap<usize, f64>,
    pub network_cost_table: NetworkCostTable,
}

/// Locates and loads all ILP inputs for a given benchmark name (e.g. "Paxos")
/// by finding the latest directory of each type in `base_dir`.
pub fn load_ilp_inputs(base_dir: &Path, name: &str) -> IlpInputs {
    let blow_up_dir = find_latest_dir(base_dir, &format!("{}_blow_up_", name));
    let size_dir = find_latest_dir(base_dir, &format!("{}_size_", name));
    let counters_dir = find_latest_dir(base_dir, &format!("{}_counters_", name));
    let perf_dir = find_latest_dir(base_dir, &format!("{}_perf_", name));

    // Find the blow_up_stats JSON whose vc had the highest throughput
    let blow_up_csvs: Vec<_> = fs::read_dir(&blow_up_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".csv"))
        .collect();
    let best_csv = blow_up_csvs
        .iter()
        .max_by_key(|e| csv_avg_throughput(&e.path()))
        .expect("No CSVs in blow_up dir");
    let best_name = best_csv.file_name().to_string_lossy().to_string();
    let vc_suffix = Regex::new(r"(\d+c_\d+vc_r\d+)\.csv$").unwrap()
        .captures(&best_name).unwrap()[1].to_string();
    let blow_up_path = blow_up_dir.join(format!("blow_up_stats_{}.json", vc_suffix));
    assert!(blow_up_path.exists(), "Missing {}", blow_up_path.display());

    let size_path = size_dir.join("size_analysis.json");
    assert!(size_path.exists(), "Missing {}", size_path.display());

    let counters_path = counters_dir.join("counters.json");
    assert!(counters_path.exists(), "Missing {}", counters_path.display());

    // Find the perf JSON with highest throughput
    let perf_csvs: Vec<_> = fs::read_dir(&perf_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".csv"))
        .collect();
    let best_perf_csv = perf_csvs
        .iter()
        .max_by_key(|e| csv_avg_throughput(&e.path()))
        .expect("No CSVs in perf dir");
    let best_perf_name = best_perf_csv.file_name().to_string_lossy().to_string();
    let perf_vc_suffix = Regex::new(r"(\d+c_\d+vc_r\d+)\.csv$").unwrap()
        .captures(&best_perf_name).unwrap()[1].to_string();
    let perf_path = perf_dir.join(format!("perf_{}.json", perf_vc_suffix));
    assert!(perf_path.exists(), "Missing {}", perf_path.display());

    IlpInputs {
        blow_up_stats: load_json(&blow_up_path),
        op_output_sizes: load_json(&size_path),
        op_counts: load_json(&counters_path),
        perf: load_json(&perf_path),
        network_cost_table: load_calibration_table(base_dir),
    }
}

/// Loads calibration CSVs from a directory containing NetworkCalibration_*b_none_* subdirs.
/// For each message size, uses the most recent calibration directory.
/// Returns a NetworkCostTable mapping message_size → per-round-trip resource cost.
pub fn load_calibration_table(calibration_dir: &Path) -> NetworkCostTable {
    // Find all unique message sizes, then use find_latest_dir for each
    let size_regex = Regex::new(r"NetworkCalibration_(\d+)b").unwrap();
    let mut sizes: HashSet<u64> = HashSet::new();
    for entry in fs::read_dir(calibration_dir).expect("Failed to read calibration directory") {
        let entry = entry.unwrap();
        let dir_name = entry.file_name().to_string_lossy().to_string();
        if let Some(cap) = size_regex.captures(&dir_name) {
            sizes.insert(cap[1].parse().unwrap());
        }
    }

    let mut entries: Vec<(u64, SarStats)> = Vec::new();
    for &msg_size in &sizes {
        let dir_path = find_latest_dir(calibration_dir, &format!("NetworkCalibration_{}b", msg_size));
        let server_files: Vec<_> = fs::read_dir(&dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("server_"))
            .collect();

        // Pick the server CSV with highest average throughput over the measurement window
        let best_file = server_files
            .iter()
            .max_by_key(|e| csv_avg_throughput(&e.path()));

        let Some(best_file) = best_file else {
            continue;
        };

        let csv_content = fs::read_to_string(best_file.path()).unwrap();
        let lines: Vec<&str> = csv_content.lines().collect();
        let header: Vec<&str> = lines[0].split(',').collect();
        let get_col = |name: &str| header.iter().position(|&h| h == name).unwrap();
        let thr_col = get_col("throughput_rps");
        let cpu_col = get_col("cpu");
        let cpu_user_col = get_col("cpu_user");
        let net_col = get_col("network");

        let start = START_MEASUREMENT_SECOND + 1;
        let end = MEASUREMENT_SECOND + 1;
        assert!(lines.len() > end, "CSV too short: {} lines", lines.len());

        let mut sum_thr = 0.0;
        let mut sum_cpu = 0.0;
        let mut sum_cpu_user = 0.0;
        let mut sum_net = 0.0;
        let n = (end - start + 1) as f64;

        for row_line in &lines[start..=end] {
            let row: Vec<&str> = row_line.split(',').collect();
            sum_thr += row[thr_col].parse::<f64>().unwrap();
            sum_cpu += row[cpu_col].parse::<f64>().unwrap();
            sum_cpu_user += row[cpu_user_col].parse::<f64>().unwrap();
            sum_net += row[net_col].parse::<f64>().unwrap();
        }

        let throughput = (sum_thr / n) as usize;
        if throughput > 0 {
            entries.push((msg_size, SarStats {
                cpu: (sum_cpu / n) / throughput as f64,
                cpu_user: (sum_cpu_user / n) / throughput as f64,
                network: (sum_net / n) / throughput as f64,
                memory: 0.0,
                io: 0.0,
            }));
        }
    }

    assert!(
        !entries.is_empty(),
        "No calibration data found in {:?}",
        calibration_dir
    );
    println!("Loaded {} calibration points", entries.len());
    NetworkCostTable::from_calibration(entries)
}

/// Loads a JSON file and deserializes it.
pub fn load_json<T: DeserializeOwned>(path: &Path) -> T {
    let file = File::open(path)
        .unwrap_or_else(|e| panic!("Failed to open {}: {}", path.display(), e));
    serde_json::from_reader(std::io::BufReader::new(file))
        .unwrap_or_else(|e| panic!("Failed to deserialize {}: {}", path.display(), e))
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

/// Parses folded perf output into per-operator CPU fractions.
/// Returns a map from op_id → fraction of total samples.
pub fn parse_perf(folded: &str) -> HashMap<usize, f64> {
    let operator_regex = Regex::new(r"op_\d+v\d+__(.*?)__(send)?(recv)?(\d+)").unwrap();
    let mut total_samples = 0.0_f64;
    let mut samples_per_id: HashMap<usize, f64> = HashMap::new();

    for line in folded.lines() {
        let n_samples: f64 = line
            .rsplit_once(' ')
            .and_then(|(_, s)| s.parse().ok())
            .unwrap_or(0.0);
        total_samples += n_samples;

        if let Some(cap) = operator_regex.captures_iter(line).last() {
            let id = cap[4].parse::<usize>().unwrap();
            *samples_per_id.entry(id).or_default() += n_samples;
        }
    }

    if total_samples > 0.0 {
        for v in samples_per_id.values_mut() {
            *v /= total_samples;
        }
    }
    samples_per_id
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

    // Collect and parse perf folded data in parallel across all nodes
    let mut perf_set = tokio::task::JoinSet::new();
    for (id, _name, cluster) in nodes.get_all_clusters() {
        for member in cluster.members() {
            if let Some(results) = member.underlying().tracing_results() {
                let folded = results.folded_data.clone();
                let loc = id.clone();
                perf_set.spawn(async move {
                    let parsed = parse_perf(&String::from_utf8_lossy(&folded));
                    (loc, parsed)
                });
            }
        }
    }
    for (id, _name, process) in nodes.get_all_processes() {
        if let Some(results) = process.underlying().tracing_results() {
            let folded = results.folded_data.clone();
            let loc = id.clone();
            perf_set.spawn(async move {
                let parsed = parse_perf(&String::from_utf8_lossy(&folded));
                (loc, parsed)
            });
        }
    }
    while let Some(result) = perf_set.join_next().await {
        let (_loc, parsed) = result.unwrap();
        for (op_id, frac) in parsed {
            let e = run_metadata.perf.entry(op_id).or_default();
            *e = e.max(frac);
        }
    }

    run_metadata
}
