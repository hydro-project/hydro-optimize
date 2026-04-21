use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy_and_analyze::{MetricLogs, Optimizations};

#[derive(Default, Serialize, Deserialize)]
pub struct RunMetadata {
    #[serde(skip)]
    throughputs: Vec<usize>,
    #[serde(skip)]
    latencies: Vec<(f64, f64, f64, u64)>, // per-second: (p50, p99, p999, count)
    counters: HashMap<usize, usize>, // op_id -> cardinality count
    #[serde(default, with = "perf_serde")]
    perf: HashMap<(usize, bool), f64>, // (op_id, is_recv) -> cpu fraction
    #[serde(skip)]
    send_overhead: HashMap<LocationId, f64>,
    #[serde(skip)]
    recv_overhead: HashMap<LocationId, f64>,
    #[serde(skip)]
    unaccounted_perf: HashMap<LocationId, f64>, // % of perf samples not mapped to any operator
    #[serde(skip)]
    sar_stats: HashMap<LocationId, Vec<SarStats>>,
}

/// Serialize/deserialize perf HashMap as a Vec for JSON compatibility.
mod perf_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        map: &HashMap<(usize, bool), f64>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let vec: Vec<_> = map.iter().map(|(k, v)| (*k, *v)).collect();
        vec.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<HashMap<(usize, bool), f64>, D::Error> {
        let vec: Vec<((usize, bool), f64)> = Vec::deserialize(d)?;
        Ok(vec.into_iter().collect())
    }
}

impl RunMetadata {
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

    /// Loads profiling data from file and injects counters and perf data into the IR.
    pub fn load_and_inject(path: &Path, ir: &mut [HydroRoot]) -> Self {
        let json = fs::read_to_string(path).unwrap_or_else(|e| {
            panic!(
                "Failed to load profiling data from {}: {}",
                path.display(),
                e
            )
        });
        let data: Self = serde_json::from_str(&json).expect("failed to deserialize profiling data");
        inject_count(ir, &data.counters);
        inject_perf(ir, &data.perf);
        data
    }
}

pub fn parse_cpu_usage(measurement: String) -> f64 {
    let regex = Regex::new(r"Total (\d+\.\d+)%").unwrap();
    regex
        .captures_iter(&measurement)
        .last()
        .map(|cap| cap[1].parse::<f64>().unwrap())
        .unwrap_or(0f64)
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

/// Returns a map from (operator ID, is network receiver) to percentage of total samples, and the percentage of samples that are unaccounted
fn parse_perf(file: String) -> (HashMap<(usize, bool), f64>, f64) {
    let mut total_samples = 0f64;
    let mut unidentified_samples = 0f64;
    let mut samples_per_id = HashMap::new();
    let operator_regex = Regex::new(r"op_\d+v\d+__(.*?)__(send)?(recv)?(\d+)").unwrap();

    for line in file.lines() {
        let n_samples_index = line.rfind(' ').unwrap() + 1;
        let n_samples = &line[n_samples_index..].parse::<f64>().unwrap();

        if let Some(cap) = operator_regex.captures_iter(line).last() {
            let id = cap[4].parse::<usize>().unwrap();
            let is_network_recv = cap
                .get(3)
                .is_some_and(|direction| direction.as_str() == "recv");

            let dfir_operator_and_samples =
                samples_per_id.entry((id, is_network_recv)).or_insert(0.0);
            *dfir_operator_and_samples += n_samples;
        } else {
            unidentified_samples += n_samples;
        }
        total_samples += n_samples;
    }

    let percent_unidentified = unidentified_samples / total_samples;
    println!(
        "Out of {} samples, {} were unidentified, {}%",
        total_samples,
        unidentified_samples,
        percent_unidentified * 100.0
    );

    samples_per_id
        .iter_mut()
        .for_each(|(_, samples)| *samples /= total_samples);
    (samples_per_id, percent_unidentified)
}

fn inject_perf_root(
    root: &mut HydroRoot,
    id_to_usage: &HashMap<(usize, bool), f64>,
    next_stmt_id: &mut usize,
) {
    if let Some(cpu_usage) = id_to_usage.get(&(*next_stmt_id, false)) {
        root.op_metadata_mut().cpu_usage = Some(*cpu_usage);
    }
}

fn inject_perf_node(
    node: &mut HydroNode,
    id_to_usage: &HashMap<(usize, bool), f64>,
    next_stmt_id: &mut usize,
) {
    if let Some(cpu_usage) = id_to_usage.get(&(*next_stmt_id, false)) {
        node.op_metadata_mut().cpu_usage = Some(*cpu_usage);
    }
    // If this is a Network node, separately get receiver CPU usage
    if let HydroNode::Network { metadata, .. } = node
        && let Some(cpu_usage) = id_to_usage.get(&(*next_stmt_id, true))
    {
        metadata.op.network_recv_cpu_usage = Some(*cpu_usage);
    }
}

pub fn inject_perf(ir: &mut [HydroRoot], id_to_usage: &HashMap<(usize, bool), f64>) {
    traverse_dfir(
        ir,
        |root, next_stmt_id| {
            inject_perf_root(root, id_to_usage, next_stmt_id);
        },
        |node, next_stmt_id| {
            inject_perf_node(node, id_to_usage, next_stmt_id);
        },
    );
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

// Note: Ensure edits to the match arms are consistent with insert_counter_node
fn inject_count_node(
    node: &mut HydroNode,
    next_stmt_id: &mut usize,
    op_to_count: &HashMap<usize, usize>,
) {
    match node {
        HydroNode::Placeholder => {
            std::panic!("Unexpected {:?} found in inject_count_node", node.print_root());
        }
        HydroNode::Source { metadata, .. }
        | HydroNode::CycleSource { metadata, .. }
        | HydroNode::Chain { metadata, .. } // Can technically be derived by summing parent cardinalities
        | HydroNode::ChainFirst { metadata, .. } // Can technically be derived by taking parent cardinality + 1
        | HydroNode::CrossSingleton { metadata, .. }
        | HydroNode::CrossProduct { metadata, .. } // Can technically be derived by multiplying parent cardinalities
        | HydroNode::Join { metadata, .. }
        | HydroNode::Difference { metadata, .. }
        | HydroNode::AntiJoin { metadata, .. }
        | HydroNode::FlatMap { metadata, .. }
        | HydroNode::FlatMapStreamBlocking { metadata, .. }
        | HydroNode::Filter { metadata, .. }
        | HydroNode::FilterMap { metadata, .. }
        | HydroNode::Unique { metadata, .. }
        | HydroNode::Scan { metadata, .. }
        | HydroNode::ScanAsyncBlocking { metadata, .. }
        | HydroNode::Fold { metadata, .. } // Output 1 value per tick
        | HydroNode::Reduce { metadata, .. } // Output 1 value per tick
        | HydroNode::FoldKeyed { metadata, .. }
        | HydroNode::ReduceKeyed { metadata, .. }
        | HydroNode::ReduceKeyedWatermark { metadata, .. }
        | HydroNode::Network { metadata, .. }
        | HydroNode::ExternalInput { metadata, .. }
        | HydroNode::SingletonSource { metadata, .. }
        | HydroNode::Partition { metadata, .. } => {
            if let Some(count) = op_to_count.get(next_stmt_id) {
                metadata.cardinality = Some(*count);
            }
            else {
                // No counter found, set to 1 so division doesn't result in infinity
                metadata.cardinality = Some(1);
            }
        }
        HydroNode::Tee { inner, metadata, .. } => {
            metadata.cardinality = inner.0.borrow().metadata().cardinality;
        }
        | HydroNode::Map { input, metadata, .. } // Equal to parent cardinality
        | HydroNode::DeferTick { input, metadata, .. }
        | HydroNode::Enumerate { input, metadata, .. }
        | HydroNode::Inspect { input, metadata, .. }
        | HydroNode::Sort { input, metadata, .. }
        | HydroNode::Counter { input, metadata, .. }
        | HydroNode::Cast { inner: input, metadata }
        | HydroNode::ObserveNonDet { inner: input, metadata, .. }
        | HydroNode::BeginAtomic { inner: input, metadata }
        | HydroNode::EndAtomic { inner: input, metadata }
        | HydroNode::Batch { inner: input, metadata }
        | HydroNode::YieldConcat { inner: input, metadata }
        | HydroNode::ResolveFutures { input, metadata }
        | HydroNode::ResolveFuturesOrdered { input, metadata }
        | HydroNode::ResolveFuturesBlocking { input, metadata }
        => {
            metadata.cardinality = input.metadata().cardinality;
        }
    }
}

pub fn inject_count(ir: &mut [HydroRoot], op_to_count: &HashMap<usize, usize>) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            inject_count_node(node, next_stmt_id, op_to_count);
        },
    );
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

pub async fn analyze_perf(process: &impl DeployCrateWrapper) -> (HashMap<(usize, bool), f64>, f64) {
    let underlying = process.underlying();
    let perf_results = underlying.tracing_results().unwrap();

    let folded_data = perf_results.folded_data.clone();
    parse_perf(String::from_utf8(folded_data).unwrap())
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
    ir: &mut [HydroRoot],
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
            let (sar_stats, op_to_count, throughputs, latencies) = tokio::join!(
                async { parse_sar_output(drain_receiver(&mut metrics.sar).await) },
                async { parse_counter_usage(drain_receiver(&mut metrics.counters).await) },
                async { parse_throughput(drain_receiver(&mut metrics.throughputs).await) },
                async { parse_latency(drain_receiver(&mut metrics.latencies).await) },
            );
            println!("Parsed stats from cluster {:?}: {}", name, idx);
            (
                (location, name, idx),
                sar_stats,
                op_to_count,
                throughputs,
                latencies,
            )
        });
    }

    let mut drained: HashMap<(LocationId, String), Vec<_>> = HashMap::new();
    while let Some(result) = set.join_next().await {
        let ((id, name, _idx), sar_stats, op_to_count, throughputs, latencies) = result.unwrap();
        drained.entry((id, name)).or_default().push((
            sar_stats,
            op_to_count,
            throughputs,
            latencies,
        ));
    }

    for (id, name, cluster) in nodes.get_all_clusters() {
        if optimizations.exclude.contains(&id.key()) {
            continue;
        }

        let cluster_data = drained.get(&(id.clone(), name.to_string())).unwrap();

        // Take max of each metric across all nodes in the cluster
        let mut max_sar: Vec<SarStats> = vec![];
        let mut max_counters: HashMap<usize, usize> = HashMap::new();

        for (sar_stats, counters, _, _) in cluster_data {
            merge_max_vec(&mut max_sar, sar_stats, SarStats::max);
            merge_max(&mut max_counters, counters.clone());
        }

        merge_max(&mut op_to_count, max_counters);

        // Aggregate perf across all members
        let mut max_unidentified = 0.0f64;
        for member in cluster.members() {
            let (perf_usage, unidentified_perf) = analyze_perf(&member).await;
            merge_max(&mut run_metadata.perf, perf_usage);
            max_unidentified = max_unidentified.max(unidentified_perf);
        }
        run_metadata
            .unaccounted_perf
            .insert(id.clone(), max_unidentified);

        if !max_sar.is_empty() {
            run_metadata.sar_stats.insert(id.clone(), max_sar);
        }
    }

    // Collect throughput/latency from all processes (aggregator outputs these)
    for node_data in drained.into_values().flatten() {
        let (_, _, throughputs, latencies) = node_data;
        let has_throughput = !throughputs.is_empty();
        run_metadata.throughputs.extend(throughputs);
        run_metadata.latencies.extend(latencies);
        if has_throughput {
            // Found aggregator, we're done
            break;
        }
    }

    inject_count(ir, &op_to_count);
    inject_perf(ir, &run_metadata.perf);
    run_metadata.counters = op_to_count;
    run_metadata
}

pub async fn get_usage(usage_out: &mut UnboundedReceiver<String>) -> f64 {
    let measurement = usage_out.recv().await.unwrap();
    parse_cpu_usage(measurement)
}

// Track the max of each so we decouple conservatively
pub fn analyze_send_recv_overheads(ir: &mut [HydroRoot], run_metadata: &mut RunMetadata) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            if let HydroNode::Network {
                input, metadata, ..
            } = node
            {
                let sender = input.metadata().location_id.root();
                let receiver = metadata.location_id.root();

                // Use cardinality from the network's input, not the network itself.
                // Reason: Cardinality is measured at ONE recipient, but the sender may be sending to MANY machines.
                if let Some(cpu_usage) = metadata.op.cpu_usage
                    && let Some(cardinality) = input.metadata().cardinality
                {
                    let overhead = cpu_usage / cardinality as f64;
                    run_metadata
                        .send_overhead
                        .entry(sender.clone())
                        .and_modify(|max_send_overhead| {
                            if overhead > *max_send_overhead {
                                *max_send_overhead = overhead;
                            }
                        })
                        .or_insert(overhead);
                }

                if let Some(cardinality) = metadata.cardinality
                    && let Some(cpu_usage) = metadata.op.network_recv_cpu_usage
                {
                    let overhead = cpu_usage / cardinality as f64;

                    run_metadata
                        .recv_overhead
                        .entry(receiver.clone())
                        .and_modify(|max_recv_overhead| {
                            if overhead > *max_recv_overhead {
                                *max_recv_overhead = overhead;
                            }
                        })
                        .or_insert(overhead);
                }
            }
        },
    );

    // Print
    for (location, overhead) in &run_metadata.send_overhead {
        println!("Max send overhead at {:?}: {}", location, overhead);
    }
    for (location, overhead) in &run_metadata.recv_overhead {
        println!("Max recv overhead at {:?}: {}", location, overhead);
    }
}
