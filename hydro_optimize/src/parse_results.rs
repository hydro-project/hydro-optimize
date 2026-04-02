use std::collections::HashMap;

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy_and_analyze::MetricLogs;

#[derive(Default)]
pub struct RunMetadata {
    pub throughputs: Vec<usize>,
    pub latencies: Vec<(f64, f64, f64, u64)>, // per-second: (p50, p99, p999, count)
    pub send_overhead: HashMap<LocationId, f64>,
    pub recv_overhead: HashMap<LocationId, f64>,
    pub unaccounted_perf: HashMap<LocationId, f64>, // % of perf samples not mapped to any operator
    pub sar_stats: HashMap<LocationId, Vec<SarStats>>,
}

impl RunMetadata {
    /// Returns the location of the bottlenecked node by comparing CPU usages at `measurement_sec`.
    /// Panics if no sar_stats exist for the given `measurement_sec`
    pub fn cpu_bottleneck(&self, measurement_sec: usize) -> LocationId {
        let (loc, _stats) = self
            .sar_stats
            .iter()
            .reduce(|(max_loc, max_stats), (curr_loc, curr_stats)| {
                let max_cpu = &max_stats[measurement_sec].cpu.all_stats;
                let curr_cpu = &curr_stats[measurement_sec].cpu.all_stats;
                if max_cpu.system + max_cpu.user < curr_cpu.system + curr_cpu.user {
                    (curr_loc, curr_stats)
                } else {
                    (max_loc, max_stats)
                }
            })
            .unwrap();
        loc.clone()
    }

    /// Returns the location of the bottlenecked node by comparing network usage at `measurement_sec`.
    /// Panics if no sar_stats exist for the given `measurement_sec`
    pub fn network_bottlenck(&self, measurement_sec: usize) -> LocationId {
        let (loc, _stats) = self
            .sar_stats
            .iter()
            .reduce(|(max_loc, max_stats), (curr_loc, curr_stats)| {
                let max_network = max_stats[measurement_sec].network;
                let curr_network = curr_stats[measurement_sec].network;
                if max_network.rx_bytes_per_sec + max_network.tx_bytes_per_sec
                    < curr_network.rx_bytes_per_sec + curr_network.tx_bytes_per_sec
                {
                    (curr_loc, curr_stats)
                } else {
                    (max_loc, max_stats)
                }
            })
            .unwrap();
        loc.clone()
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
#[derive(Debug, Default, Clone)]
pub struct CPUStats {
    pub core_stats: Vec<CPUStat>, // One for each core
    pub all_stats: CPUStat,
}

/// Per-second network statistics from sar -n DEV output (eth0 only)
#[derive(Debug, Default, Clone, Copy)]
pub struct NetworkStats {
    pub rx_packets_per_sec: f64,
    pub tx_packets_per_sec: f64,
    pub rx_bytes_per_sec: f64,
    pub tx_bytes_per_sec: f64,
}

/// Combined per-second sar statistics
#[derive(Debug, Default, Clone)]
pub struct SarStats {
    pub cpu: CPUStats,
    pub network: NetworkStats,
}

/// Parses a single CPU line from sar -u -P ALL output.
/// Returns (is_all, CPUStat) where is_all=true for the "all" aggregate line.
fn parse_cpu_line(line: &str) -> Option<(bool, CPUStat)> {
    let cpu_regex = Regex::new(
        r"\s(all|\d+)\s+(\d+\.?\d*)\s+\d+\.?\d*\s+(\d+\.?\d*)\s+\d+\.?\d*\s+\d+\.?\d*\s+(\d+\.?\d*)$",
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
/// Format: "HH:MM:SS AM/PM IFACE rxpck/s txpck/s rxkB/s txkB/s ..."
/// Matches eth0, ens5, or any other interface name that isn't "lo".
fn parse_network_line(line: &str) -> Option<NetworkStats> {
    // Match any interface: captures interface name followed by numeric stats
    let iface_regex =
        Regex::new(r"(\S+)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)").unwrap();

    iface_regex.captures(line).and_then(|caps| {
        let iface = &caps[1];
        // Skip loopback and header lines
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

/// Parses `sar -n DEV -u -P ALL` output lines and returns per-second SarStats.
/// Groups per-core CPU stats with the aggregate "all" line.
pub fn parse_sar_output(lines: Vec<String>) -> Vec<SarStats> {
    let mut cpu_usages: Vec<CPUStats> = vec![];
    let mut network_usages = vec![];

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
        }
    }

    assert!(
        cpu_usages.len().abs_diff(network_usages.len()) <= 1,
        "sar output mismatch: {} cpu vs {} network entries",
        cpu_usages.len(),
        network_usages.len(),
    );

    cpu_usages
        .into_iter()
        .zip(network_usages)
        .map(|(cpu, network)| SarStats { cpu, network })
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

pub fn inject_perf(ir: &mut [HydroRoot], folded_data: Vec<u8>) -> f64 {
    let (id_to_usage, unidentified_usage) = parse_perf(String::from_utf8(folded_data).unwrap());
    traverse_dfir(
        ir,
        |root, next_stmt_id| {
            inject_perf_root(root, &id_to_usage, next_stmt_id);
        },
        |node, next_stmt_id| {
            inject_perf_node(node, &id_to_usage, next_stmt_id);
        },
    );
    unidentified_usage
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
        | HydroNode::Filter { metadata, .. }
        | HydroNode::FilterMap { metadata, .. }
        | HydroNode::Unique { metadata, .. }
        | HydroNode::Scan { metadata, .. }
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

pub async fn analyze_perf(process: &impl DeployCrateWrapper, ir: &mut [HydroRoot]) -> f64 {
    let underlying = process.underlying();
    let perf_results = underlying.tracing_results().unwrap();

    // Inject perf usages into metadata, return unidentified perf
    inject_perf(ir, perf_results.folded_data.clone())
}

pub async fn analyze_cluster_results(
    nodes: &DeployResult<'_, HydroDeploy>,
    ir: &mut [HydroRoot],
    mut cluster_metrics: HashMap<(LocationId, String, usize), MetricLogs>,
    client_id: &LocationId,
    measurement_second: Option<usize>,
) -> RunMetadata {
    let mut run_metadata = RunMetadata::default();
    let mut op_to_count = HashMap::new();

    // Drain all receivers and parse in parallel across all nodes
    let mut set = tokio::task::JoinSet::new();
    for ((location, name, idx), mut metrics) in cluster_metrics.drain() {
        if location == *client_id && idx > 0 {
            // Only analyze the client with index 0, since all clients should have similar perf. Saves time
            continue;
        }
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

    for (id, name, _cluster) in nodes.get_all_clusters() {
        let cluster_data = drained.get(&(id.clone(), name.to_string())).unwrap();

        // Find the node with max CPU usage
        let max_usage = cluster_data.iter().reduce(|max_data, data| {
            let stat = measurement_second
                .and_then(|s| data.0.get(s))
                .or_else(|| data.0.last());
            let max_stat = measurement_second
                .and_then(|s| max_data.0.get(s))
                .or_else(|| max_data.0.last());
            match (stat, max_stat) {
                (Some(s), Some(ms))
                    if s.cpu.all_stats.user + s.cpu.all_stats.system
                        > ms.cpu.all_stats.user + ms.cpu.all_stats.system =>
                {
                    data
                }
                _ => max_data,
            }
        });

        if let Some((max_sar_stat, counters, _, _)) = max_usage {
            // Parse perf
            // let unidentified_perf = analyze_perf(cluster.members().get(*idx).unwrap(), ir).await;
            op_to_count.extend(counters.clone());

            // run_metadata
            //     .unaccounted_perf
            //     .insert(id.clone(), unidentified_perf);
            run_metadata
                .sar_stats
                .insert(id.clone(), max_sar_stat.clone());
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
