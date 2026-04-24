use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy_and_analyze::{MetricLogs, Optimizations};

#[derive(Default)]
pub struct RunMetadata {
    pub throughputs: Vec<usize>,
    pub latencies: Vec<(f64, f64, f64, u64)>, // per-second: (p50, p99, p999, count)
    pub counters: HashMap<usize, usize>,      // op_id -> cardinality count
    pub byte_sizes: HashMap<usize, Vec<u64>>, // op_id -> sampled byte sizes
    pub sar_stats: HashMap<LocationId, Vec<SarStats>>,
    /// Maps each LocationId → original op_ids assigned to it (populated when size_analysis is used)
    pub location_to_original_ops: HashMap<LocationId, Vec<usize>>,
}

// Purely use for serde on RunMetadata. All fields omitted are not serialized
#[derive(Serialize, Deserialize)]
struct SerdeRunMetadata {
    counters: HashMap<usize, usize>,
    #[serde(default)]
    byte_sizes: HashMap<usize, Vec<u64>>,
}

/// Only `counters` and `byte_sizes` are persisted; everything else is transient.
impl Serialize for RunMetadata {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let serde = SerdeRunMetadata {
            counters: self.counters.clone(),
            byte_sizes: self.byte_sizes.clone(),
        };
        serde.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RunMetadata {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let serde = SerdeRunMetadata::deserialize(deserializer)?;
        Ok(Self {
            counters: serde.counters,
            byte_sizes: serde.byte_sizes,
            ..Default::default()
        })
    }
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

            // If any core is overloaded, consider it CPU overloaded
            let cpu_threshold = 100.0 * limits.bottleneck_threshold;
            let max_core_used = sar
                .cpu
                .core_stats
                .iter()
                .map(|c| c.user + c.system)
                .reduce(f64::max)
                .unwrap_or(0.0);
            println!(
                "  {:?} CPU: max_core_used={:.1}% threshold={:.1}%",
                location, max_core_used, cpu_threshold
            );
            if max_core_used >= cpu_threshold {
                metrics.insert(BottleneckMetric::Cpu);
            }

            // If the sum of input + output bytes exceeds threshold
            let net_threshold = limits.network_bytes_per_sec * limits.bottleneck_threshold;
            let net_used = sar.network.rx_bytes_per_sec + sar.network.tx_bytes_per_sec;
            println!(
                "  {:?} Network: {:.0} B/s threshold={:.0} B/s",
                location, net_used, net_threshold
            );
            if net_used >= net_threshold {
                metrics.insert(BottleneckMetric::Network);
            }

            // If the percentage of memory used exceeds threshold
            let mem_threshold = 100.0 * limits.bottleneck_threshold;
            println!(
                "  {:?} Memory: {:.1}% threshold={:.1}%",
                location, sar.memory.percent_mem_used, mem_threshold
            );
            if sar.memory.percent_mem_used >= mem_threshold {
                metrics.insert(BottleneckMetric::Memory);
            }

            // If the sum of read + write transfers per second exceeds threshold
            let io_threshold = limits.io_tps * limits.bottleneck_threshold;
            let io_used = sar.io.rtps + sar.io.wtps;
            println!(
                "  {:?} IO: {:.1} tps threshold={:.1} tps",
                location, io_used, io_threshold
            );
            if io_used >= io_threshold {
                metrics.insert(BottleneckMetric::IO);
            }

            if !metrics.is_empty() {
                result.push((location.clone(), metrics));
            }
        }
        result
    }

    /// Average throughput over the given inclusive second range.
    pub fn avg_throughput(&self, start: usize, end: usize) -> usize {
        let slice = &self.throughputs[start..=end];
        if slice.is_empty() {
            0
        } else {
            slice.iter().sum::<usize>() / slice.len()
        }
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
                    "{} @{}s: CPU {:.2}% (user {:.2}%, sys {:.2}%, idle {:.2}%) | \
                     Net rx {:.0} pkt/s {:.0} B/s, tx {:.0} pkt/s {:.0} B/s | \
                     Mem {:.2}% ({:.0} KB used, {:.0} KB buf, {:.0} KB cached) | \
                     IO r {:.2} w {:.2} tps, bread {:.2} bwrtn {:.2}/s",
                    name,
                    measurement_second + 1,
                    sar.cpu.all_stats.user + sar.cpu.all_stats.system,
                    sar.cpu.all_stats.user,
                    sar.cpu.all_stats.system,
                    sar.cpu.all_stats.idle,
                    sar.network.rx_packets_per_sec,
                    sar.network.rx_bytes_per_sec,
                    sar.network.tx_packets_per_sec,
                    sar.network.tx_bytes_per_sec,
                    sar.memory.percent_mem_used,
                    sar.memory.kb_mem_used,
                    sar.memory.kb_buffers,
                    sar.memory.kb_cached,
                    sar.io.rtps,
                    sar.io.wtps,
                    sar.io.bread_per_sec,
                    sar.io.bwrtn_per_sec,
                );
            }
        }
    }

    /// Saves per-location CSVs and per-cluster profiling JSON files to `output_dir`.
    pub fn save_run_metadata(
        self,
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
            let num_cores = stats
                .iter()
                .map(|s| s.cpu.core_stats.len())
                .max()
                .unwrap_or(0);

            let write_csv = || -> Result<(), Box<dyn std::error::Error>> {
                let mut file = File::create(&filename)?;
                write!(file, "time_s,cpu_user,cpu_system,cpu_idle")?;
                for c in 0..num_cores {
                    write!(file, ",core{}_user,core{}_system,core{}_idle", c, c, c)?;
                }
                writeln!(
                    file,
                    ",network_tx_packets_per_sec,network_rx_packets_per_sec,\
                     network_tx_bytes_per_sec,network_rx_bytes_per_sec,\
                     mem_kb_used,mem_percent_used,mem_kb_buffers,mem_kb_cached,\
                     io_rtps,io_wtps,io_bread_per_sec,io_bwrtn_per_sec,\
                     throughput_rps,latency_p50_ms,latency_p99_ms,latency_p999_ms,latency_samples",
                )?;

                let default_sar = SarStats::default();
                for i in 0..num_rows {
                    let sar = stats.get(i).unwrap_or(&default_sar);
                    let thr = self.throughputs.get(i).copied().unwrap_or(0);
                    let (p50, p99, p999, count) =
                        self.latencies.get(i).copied().unwrap_or_default();
                    write!(
                        file,
                        "{},{:.2},{:.2},{:.2}",
                        i, sar.cpu.all_stats.user, sar.cpu.all_stats.system, sar.cpu.all_stats.idle,
                    )?;
                    let default_core = CPUStat::default();
                    for c in 0..num_cores {
                        let core = sar.cpu.core_stats.get(c).unwrap_or(&default_core);
                        write!(
                            file,
                            ",{:.2},{:.2},{:.2}",
                            core.user, core.system, core.idle
                        )?;
                    }
                    writeln!(
                        file,
                        ",{:.2},{:.2},{:.2},{:.2},\
                         {:.2},{:.2},{:.2},{:.2},\
                         {:.2},{:.2},{:.2},{:.2},\
                         {},{:.3},{:.3},{:.3},{}",
                        sar.network.tx_packets_per_sec,
                        sar.network.rx_packets_per_sec,
                        sar.network.tx_bytes_per_sec,
                        sar.network.rx_bytes_per_sec,
                        sar.memory.kb_mem_used,
                        sar.memory.percent_mem_used,
                        sar.memory.kb_buffers,
                        sar.memory.kb_cached,
                        sar.io.rtps,
                        sar.io.wtps,
                        sar.io.bread_per_sec,
                        sar.io.bwrtn_per_sec,
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

        // Save profiling JSON
        let path = output_dir.join(format!(
            "profiling_{}c_{}vc_r{}.json",
            num_clients, num_clients_per_node, run
        ));
        let json = serde_json::to_string_pretty(&self).expect("failed to serialize profiling data");
        match fs::write(&path, json) {
            Ok(()) => println!("Saved profiling data: {}", path.display()),
            Err(e) => eprintln!("Failed to save profiling data to {}: {}", path.display(), e),
        }
    }

    /// Loads profiling data from file.
    pub fn load(path: &Path) -> Self {
        let json = fs::read_to_string(path).unwrap_or_else(|e| {
            panic!(
                "Failed to load profiling data from {}: {}",
                path.display(),
                e
            )
        });
        serde_json::from_str(&json).expect("failed to deserialize profiling data")
    }
}

/// Per-second CPU statistics from sar -u output
#[derive(Debug, Default, Clone, Copy)]
pub struct CPUStat {
    pub user: f64,
    pub system: f64,
    pub idle: f64,
}

impl CPUStat {
    fn max(self, other: Self) -> Self {
        Self {
            user: self.user.max(other.user),
            system: self.system.max(other.system),
            idle: self.idle.max(other.idle),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CPUStats {
    pub core_stats: Vec<CPUStat>, // One for each core
    pub all_stats: CPUStat,
}

impl CPUStats {
    fn max(self, other: Self) -> Self {
        let len = self.core_stats.len().max(other.core_stats.len());
        let core_stats = (0..len)
            .map(|i| {
                let a = self.core_stats.get(i).copied().unwrap_or_default();
                let b = other.core_stats.get(i).copied().unwrap_or_default();
                a.max(b)
            })
            .collect();
        Self {
            all_stats: self.all_stats.max(other.all_stats),
            core_stats,
        }
    }
}

/// Per-second network statistics from sar -n DEV output (eth0 only)
#[derive(Debug, Default, Clone, Copy)]
pub struct NetworkStats {
    pub rx_packets_per_sec: f64,
    pub tx_packets_per_sec: f64,
    pub rx_bytes_per_sec: f64,
    pub tx_bytes_per_sec: f64,
}

impl NetworkStats {
    fn max(self, other: Self) -> Self {
        Self {
            rx_packets_per_sec: self.rx_packets_per_sec.max(other.rx_packets_per_sec),
            tx_packets_per_sec: self.tx_packets_per_sec.max(other.tx_packets_per_sec),
            rx_bytes_per_sec: self.rx_bytes_per_sec.max(other.rx_bytes_per_sec),
            tx_bytes_per_sec: self.tx_bytes_per_sec.max(other.tx_bytes_per_sec),
        }
    }
}

/// Per-second memory statistics from sar -r output
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryStats {
    pub kb_mem_used: f64,
    pub percent_mem_used: f64,
    pub kb_buffers: f64,
    pub kb_cached: f64,
}

impl MemoryStats {
    fn max(self, other: Self) -> Self {
        Self {
            kb_mem_used: self.kb_mem_used.max(other.kb_mem_used),
            percent_mem_used: self.percent_mem_used.max(other.percent_mem_used),
            kb_buffers: self.kb_buffers.max(other.kb_buffers),
            kb_cached: self.kb_cached.max(other.kb_cached),
        }
    }
}

/// Per-second aggregate I/O statistics from sar -b output
#[derive(Debug, Default, Clone, Copy)]
pub struct IOStats {
    pub rtps: f64,          // read transfers per second
    pub wtps: f64,          // write transfers per second
    pub bread_per_sec: f64, // blocks read per second
    pub bwrtn_per_sec: f64, // blocks written per second
}

impl IOStats {
    fn max(self, other: Self) -> Self {
        Self {
            rtps: self.rtps.max(other.rtps),
            wtps: self.wtps.max(other.wtps),
            bread_per_sec: self.bread_per_sec.max(other.bread_per_sec),
            bwrtn_per_sec: self.bwrtn_per_sec.max(other.bwrtn_per_sec),
        }
    }
}

/// Combined per-second sar statistics
#[derive(Debug, Default, Clone)]
pub struct SarStats {
    pub cpu: CPUStats,
    pub network: NetworkStats,
    pub memory: MemoryStats,
    pub io: IOStats,
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
}

/// Parses a single CPU line from sar -u -P ALL output.
/// Returns (is_all, CPUStat) where is_all=true for the "all" aggregate line.
/// Format: "HH:MM:SS CPU %user %nice %system %iowait %steal %idle"
fn parse_cpu_line(line: &str) -> Option<(bool, CPUStat)> {
    // Require "all" or a core number 0-999 (avoids matching memory lines with huge numbers)
    let cpu_regex = Regex::new(
        r"\s(all|\d{1,3})\s+(\d+\.?\d*)\s+\d+\.?\d*\s+(\d+\.?\d*)\s+\d+\.?\d*\s+\d+\.?\d*\s+(\d+\.?\d*)$",
    )
    .unwrap();

    cpu_regex.captures(line).and_then(|caps| {
        let is_all = &caps[1] == "all";
        let user = caps[2].parse::<f64>().ok()?;
        let system = caps[3].parse::<f64>().ok()?;
        let idle = caps[4].parse::<f64>().ok()?;
        Some((is_all, CPUStat { user, system, idle }))
    })
}

/// Parses a single network line from sar -n DEV output (any non-loopback interface).
/// Format: "HH:MM:SS IFACE rxpck/s txpck/s rxkB/s txkB/s [rxcmp/s txcmp/s rxmcst/s %ifutil]"
/// Matches eth0, ens5, or any other interface name that isn't "lo".
fn parse_network_line(line: &str) -> Option<NetworkStats> {
    // Require interface name to start with a letter (avoids matching bare numeric IO data lines)
    let iface_regex =
        Regex::new(r"([a-zA-Z]\S*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)")
            .unwrap();

    iface_regex.captures(line).and_then(|caps| {
        let iface = &caps[1];
        // Skip loopback, docker, and header lines
        if iface == "lo" || iface == "docker0" || iface == "IFACE" {
            return None;
        }
        let rx_pkt = caps[2].parse::<f64>().ok()?;
        let tx_pkt = caps[3].parse::<f64>().ok()?;
        let rx_kb = caps[4].parse::<f64>().ok()?;
        let tx_kb = caps[5].parse::<f64>().ok()?;
        Some(NetworkStats {
            rx_packets_per_sec: rx_pkt,
            tx_packets_per_sec: tx_pkt,
            rx_bytes_per_sec: rx_kb * 1024.0,
            tx_bytes_per_sec: tx_kb * 1024.0,
        })
    })
}

/// Parses a single memory line from sar -r output.
/// Format: "HH:MM:SS kbmemfree kbavail kbmemused %memused kbbuffers kbcached [kbcommit %commit kbactive kbinact kbdirty]"
fn parse_memory_line(line: &str) -> Option<MemoryStats> {
    // Require timestamp followed by two large integers (kbmemfree, kbavail) before the captured fields.
    // The first field after timestamp must be a digit (not an interface name like "lo").
    let re = Regex::new(r"\d+:\d+:\d+\s+(\d+)\s+\d+\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+)\s+(\d+)")
        .unwrap();
    re.captures(line).and_then(|caps| {
        if line.contains("kbmemfree") {
            return None;
        }
        // kbmemfree must be large (>10000) to distinguish from other numeric lines
        let kbmemfree = caps[1].parse::<f64>().ok()?;
        if kbmemfree < 10000.0 {
            return None;
        }
        Some(MemoryStats {
            kb_mem_used: caps[2].parse().ok()?,
            percent_mem_used: caps[3].parse().ok()?,
            kb_buffers: caps[4].parse().ok()?,
            kb_cached: caps[5].parse().ok()?,
        })
    })
}

/// Parses a single I/O line from sar -b output.
/// Format: "HH:MM:SS tps rtps wtps [dtps] bread/s bwrtn/s [bdscd/s]"
/// Supports both 5-column (older) and 7-column (newer, with dtps/bdscd/s) formats.
fn parse_io_line(line: &str) -> Option<IOStats> {
    if line.contains("tps") {
        return None;
    }
    // Try 7-column format first: tps rtps wtps dtps bread/s bwrtn/s bdscd/s
    let re7 = Regex::new(
        r"\d+:\d+:\d+\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+\d+\.?\d*\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+\d+\.?\d*\s*$",
    ).unwrap();
    if let Some(caps) = re7.captures(line) {
        return Some(IOStats {
            rtps: caps[2].parse().ok()?,
            wtps: caps[3].parse().ok()?,
            bread_per_sec: caps[4].parse().ok()?,
            bwrtn_per_sec: caps[5].parse().ok()?,
        });
    }
    // Fall back to 5-column format: tps rtps wtps bread/s bwrtn/s
    let re5 = Regex::new(
        r"\d+:\d+:\d+\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s*$",
    )
    .unwrap();
    re5.captures(line).and_then(|caps| {
        Some(IOStats {
            rtps: caps[2].parse().ok()?,
            wtps: caps[3].parse().ok()?,
            bread_per_sec: caps[4].parse().ok()?,
            bwrtn_per_sec: caps[5].parse().ok()?,
        })
    })
}

/// Parses `sar -n DEV -u -P ALL -r -b` output lines and returns per-second SarStats.
/// Groups per-core CPU stats with the aggregate "all" line.
pub fn parse_sar_output(lines: Vec<String>) -> Vec<SarStats> {
    let mut cpu_usages: Vec<CPUStats> = vec![];
    let mut network_usages = vec![];
    let mut memory_usages = vec![];
    let mut io_usages = vec![];

    for line in &lines {
        if let Some((is_all, stat)) = parse_cpu_line(line) {
            // Assumes that "all" line comes before per-core lines
            if is_all {
                cpu_usages.push(CPUStats {
                    all_stats: stat,
                    core_stats: vec![],
                });
            } else if let Some(last) = cpu_usages.last_mut() {
                last.core_stats.push(stat);
            }
        } else if let Some(network) = parse_network_line(line) {
            network_usages.push(network);
        } else if let Some(memory) = parse_memory_line(line) {
            memory_usages.push(memory);
        } else if let Some(io) = parse_io_line(line) {
            io_usages.push(io);
        }
    }

    let len = cpu_usages.len();
    assert!(
        len.abs_diff(network_usages.len()) <= 1,
        "sar output mismatch: {} cpu vs {} network entries",
        len,
        network_usages.len(),
    );

    cpu_usages
        .into_iter()
        .zip(network_usages)
        .enumerate()
        .map(|(i, (cpu, network))| SarStats {
            cpu,
            network,
            memory: memory_usages.get(i).copied().unwrap_or_default(),
            io: io_usages.get(i).copied().unwrap_or_default(),
        })
        .collect()
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

/// Returns (op_id, count)
pub fn parse_counter_usage(lines: Vec<String>) -> HashMap<usize, usize> {
    let regex = Regex::new(r"\((\d+)\): (\d+)").unwrap();
    let mut op_to_count = HashMap::new();
    for measurement in lines {
        let matches = regex.captures_iter(&measurement).last().unwrap();
        let op_id = matches[1].parse::<usize>().unwrap();
        let count = matches[2].parse::<usize>().unwrap();
        op_to_count.insert(op_id, count);
    }
    op_to_count
}

/// Per-operator resource cost breakdown.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ResourceCost {
    /// Fraction of CPU time attributed to this operator (0.0–1.0 per core).
    pub cpu: f64,
    /// I/O transfers per second attributed to this operator.
    pub io: f64,
    /// Memory KB attributed to this operator.
    pub memory: f64,
}

/// Per-byte resource cost from network calibration.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct NetworkCostPerByte {
    /// CPU % consumed per byte of network traffic
    pub cpu_pct_per_byte: f64,
    /// Memory KB consumed per byte of network traffic
    pub memory_kb_per_byte: f64,
    /// I/O transfers per second consumed per byte of network traffic
    pub io_tps_per_byte: f64,
}

impl NetworkCostPerByte {
    fn lerp(self, other: Self, t: f64) -> Self {
        Self {
            cpu_pct_per_byte: self.cpu_pct_per_byte + t * (other.cpu_pct_per_byte - self.cpu_pct_per_byte),
            memory_kb_per_byte: self.memory_kb_per_byte + t * (other.memory_kb_per_byte - self.memory_kb_per_byte),
            io_tps_per_byte: self.io_tps_per_byte + t * (other.io_tps_per_byte - self.io_tps_per_byte),
        }
    }

    /// Total resource cost for `count` messages of `bytes_each`.
    pub fn total_cost(&self, count: usize, bytes_each: u64) -> ResourceCost {
        let total_bytes = count as f64 * bytes_each as f64;
        ResourceCost {
            cpu: self.cpu_pct_per_byte * total_bytes,
            memory: self.memory_kb_per_byte * total_bytes,
            io: self.io_tps_per_byte * total_bytes,
        }
    }
}

/// Lookup table mapping message size (bytes) → per-byte resource costs.
/// Built from calibration runs at various message sizes. Interpolates linearly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkCostTable {
    /// Sorted by message size.
    entries: Vec<(u64, NetworkCostPerByte)>,
}

impl NetworkCostTable {
    pub fn from_calibration(mut entries: Vec<(u64, NetworkCostPerByte)>) -> Self {
        entries.sort_by_key(|(size, _)| *size);
        assert!(!entries.is_empty(), "NetworkCostTable requires at least one calibration point");
        Self { entries }
    }

    /// Returns the interpolated cost-per-byte for the given message size.
    /// Clamps to the nearest endpoint if outside the calibrated range.
    pub fn cost_per_byte(&self, message_size_bytes: u64) -> NetworkCostPerByte {
        let n = self.entries.len();
        if n == 1 || message_size_bytes <= self.entries[0].0 {
            return self.entries[0].1;
        }
        if message_size_bytes >= self.entries[n - 1].0 {
            return self.entries[n - 1].1;
        }
        let i = self.entries.partition_point(|(s, _)| *s <= message_size_bytes);
        let (s0, c0) = self.entries[i - 1];
        let (s1, c1) = self.entries[i];
        let t = (message_size_bytes - s0) as f64 / (s1 - s0) as f64;
        c0.lerp(c1, t)
    }

    /// Total resource cost for sending `count` messages of `message_size_bytes` each.
    pub fn network_cost(&self, count: usize, message_size_bytes: u64) -> ResourceCost {
        self.cost_per_byte(message_size_bytes).total_cost(count, message_size_bytes)
    }
}

/// Combined per-operator metrics gathered from calibration, counters, and byte-size inspection.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct OperatorMetrics {
    /// op_id → median output byte size per tuple
    pub op_to_output_bytes: HashMap<usize, u64>,
    /// op_id → cardinality (elements per measurement interval)
    pub op_to_count: HashMap<usize, usize>,
    /// op_id → resource cost
    pub op_to_cost: HashMap<usize, ResourceCost>,
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
fn median(sorted: &mut Vec<u64>) -> u64 {
    sorted.sort_unstable();
    if sorted.is_empty() {
        0
    } else {
        sorted[sorted.len() / 2]
    }
}

impl OperatorMetrics {
    /// Builds OperatorMetrics from raw measurements.
    /// - `counters`: op_id → cardinality from counter measurements
    /// - `byte_sizes`: op_id → sampled byte sizes from inspect measurements
    /// - `sar_per_location`: per-location SAR stats at the measurement second
    /// - `ops_per_location`: op_id → LocationId mapping (which ops run where)
    /// - `network_cost_per_byte`: per-byte resource costs from calibration
    pub fn from_measurements(
        counters: HashMap<usize, usize>,
        mut byte_sizes: HashMap<usize, Vec<u64>>,
        sar_per_location: &HashMap<LocationId, SarStats>,
        ops_per_location: &HashMap<LocationId, Vec<usize>>,
        network_cost_per_byte: &NetworkCostPerByte,
    ) -> Self {
        let op_to_output_bytes: HashMap<usize, u64> = byte_sizes
            .iter_mut()
            .map(|(&op_id, sizes)| (op_id, median(sizes)))
            .collect();

        // Compute per-operator cost by dividing location cost evenly among its operators,
        // then subtracting estimated network cost.
        let mut op_to_cost = HashMap::new();
        for (location, op_ids) in ops_per_location {
            let Some(sar) = sar_per_location.get(location) else {
                continue;
            };
            let n = op_ids.len().max(1) as f64;
            let net_bytes = sar.network.rx_bytes_per_sec + sar.network.tx_bytes_per_sec;
            let total_cpu = (sar.cpu.all_stats.user + sar.cpu.all_stats.system
                - net_bytes * network_cost_per_byte.cpu_pct_per_byte)
                .max(0.0);
            let total_io = (sar.io.rtps + sar.io.wtps
                - net_bytes * network_cost_per_byte.io_tps_per_byte)
                .max(0.0);
            let total_memory = (sar.memory.kb_mem_used
                - net_bytes * network_cost_per_byte.memory_kb_per_byte)
                .max(0.0);

            for &op_id in op_ids {
                op_to_cost.insert(
                    op_id,
                    ResourceCost {
                        cpu: total_cpu / n,
                        io: total_io / n,
                        memory: total_memory / n,
                    },
                );
            }
        }

        Self {
            op_to_output_bytes,
            op_to_count: counters,
            op_to_cost,
        }
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

/// Merges `src` into `dst`, keeping the max value for each key.
fn merge_max<K: Eq + std::hash::Hash, V: PartialOrd + Copy>(
    dst: &mut HashMap<K, V>,
    src: HashMap<K, V>,
) {
    for (k, v) in src {
        dst.entry(k)
            .and_modify(|existing| {
                if v > *existing {
                    *existing = v;
                }
            })
            .or_insert(v);
    }
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
    let mut op_to_count = HashMap::new();

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
        if optimizations.exclude.contains(&id) {
            continue;
        }

        let cluster_data = drained.get(&(id.clone(), name.to_string())).unwrap();

        let mut max_sar: Vec<SarStats> = vec![];
        let mut max_counters: HashMap<usize, usize> = HashMap::new();

        for (sar_stats, counters, _, _, byte_sizes) in cluster_data {
            merge_max_vec(&mut max_sar, sar_stats, SarStats::max);
            merge_max(&mut max_counters, counters.clone());
            for (op_id, sizes) in byte_sizes {
                run_metadata
                    .byte_sizes
                    .entry(*op_id)
                    .or_default()
                    .extend(sizes);
            }
        }

        merge_max(&mut op_to_count, max_counters);

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


