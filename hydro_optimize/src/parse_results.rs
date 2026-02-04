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
    pub throughput: (f64, f64, f64), // mean - 2 std, mean, mean + 2 std
    pub latencies: (f64, f64, f64, u64), // p50, p99, p999, count
    pub send_overhead: HashMap<LocationId, f64>,
    pub recv_overhead: HashMap<LocationId, f64>,
    pub unaccounted_perf: HashMap<LocationId, f64>, // % of perf samples not mapped to any operator
    pub total_usage: HashMap<LocationId, f64>,      // 100% CPU = 1.0
    pub network_stats: HashMap<LocationId, NetworkStats>,
}

pub fn parse_cpu_usage(measurement: String) -> f64 {
    let regex = Regex::new(r"Total (\d+\.\d+)%").unwrap();
    regex
        .captures_iter(&measurement)
        .last()
        .map(|cap| cap[1].parse::<f64>().unwrap())
        .unwrap_or(0f64)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PercentileStats {
    pub p50: f64,
    pub p99: f64,
    pub p999: f64,
}

#[derive(Debug, Default, Clone)]
pub struct NetworkStats {
    pub rx_packets_per_sec: PercentileStats,
    pub tx_packets_per_sec: PercentileStats,
    pub rx_bytes_per_sec: PercentileStats,
    pub tx_bytes_per_sec: PercentileStats,
}

impl PercentileStats {
    fn from_samples(samples: &mut [f64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let len = samples.len();
        Self {
            p50: samples[len * 50 / 100],
            p99: samples[len * 99 / 100],
            p999: samples[len.saturating_sub(1).min(len * 999 / 1000)],
        }
    }
}

/// Parses `sar -n DEV` output lines and computes p50, p99, p999 for network metrics.
/// Only considers eth0 interface data.
pub fn parse_network_usage(lines: Vec<String>) -> NetworkStats {
    let mut rx_pkt_samples = Vec::new();
    let mut tx_pkt_samples = Vec::new();
    let mut rx_kb_samples = Vec::new();
    let mut tx_kb_samples = Vec::new();

    // sar output format: TIME IFACE rxpck/s txpck/s rxkB/s txkB/s ...
    // We look for lines containing "eth0" with numeric data
    let eth0_regex =
        Regex::new(r"eth0\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)").unwrap();

    for line in &lines {
        if let Some(caps) = eth0_regex.captures(line)
            && let (Ok(rx_pkt), Ok(tx_pkt), Ok(rx_kb), Ok(tx_kb)) = (
                caps[1].parse::<f64>(),
                caps[2].parse::<f64>(),
                caps[3].parse::<f64>(),
                caps[4].parse::<f64>(),
            )
        {
            rx_pkt_samples.push(rx_pkt);
            tx_pkt_samples.push(tx_pkt);
            // Convert kB/s to bytes/s
            rx_kb_samples.push(rx_kb * 1024.0);
            tx_kb_samples.push(tx_kb * 1024.0);
        }
    }

    NetworkStats {
        rx_packets_per_sec: PercentileStats::from_samples(&mut rx_pkt_samples),
        tx_packets_per_sec: PercentileStats::from_samples(&mut tx_pkt_samples),
        rx_bytes_per_sec: PercentileStats::from_samples(&mut rx_kb_samples),
        tx_bytes_per_sec: PercentileStats::from_samples(&mut tx_kb_samples),
    }
}

/// Parses throughput output from `print_parseable_bench_results`.
/// Format: "HYDRO_OPTIMIZE_THR: {lower:.2} - {mean:.2} - {upper:.2} requests/s"
/// Returns the last (lower, mean, upper) tuple found.
pub fn parse_throughput(lines: Vec<String>) -> (f64, f64, f64) {
    let regex =
        Regex::new(r"(\d+\.?\d*)\s*-\s*(\d+\.?\d*)\s*-\s*(\d+\.?\d*)\s*requests/s").unwrap();
    lines
        .iter()
        .filter_map(|line| {
            regex.captures(line).map(|cap| {
                (
                    cap[1].parse::<f64>().unwrap(),
                    cap[2].parse::<f64>().unwrap(),
                    cap[3].parse::<f64>().unwrap(),
                )
            })
        })
        .next_back()
        .unwrap()
}

/// Parses latency output from `print_parseable_bench_results`.
/// Format: "HYDRO_OPTIMIZE_LAT: p50: {p50:.3} | p99 {p99:.3} | p999 {p999:.3} ms ({num_samples} samples)"
/// Returns the last (p50, p99, p999, num_samples) tuple found.
pub fn parse_latency(lines: Vec<String>) -> (f64, f64, f64, u64) {
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
        .next_back()
        .unwrap()
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
    traverse_dfir::<HydroDeploy>(
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
pub fn parse_counter_usage(lines: Vec<String>, op_to_count: &mut HashMap<usize, usize>) {
    let regex = Regex::new(r"\((\d+)\): (\d+)").unwrap();
    for measurement in lines {
        let matches = regex.captures_iter(&measurement).last().unwrap();
        let op_id = matches[1].parse::<usize>().unwrap();
        let count = matches[2].parse::<usize>().unwrap();
        op_to_count.insert(op_id, count);
    }
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
        | HydroNode::SingletonSource { metadata, .. } => {
            if let Some(count) = op_to_count.get(next_stmt_id) {
                metadata.cardinality = Some(*count);
            }
            else {
                // No counter found, set to 1 so division doesn't result in infinity
                metadata.cardinality = Some(1);
            }
        }
        HydroNode::Tee { inner ,metadata, .. } => {
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
        => {
            metadata.cardinality = input.metadata().cardinality;
        }
    }
}

pub fn inject_count(ir: &mut [HydroRoot], op_to_count: &HashMap<usize, usize>) {
    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            inject_count_node(node, next_stmt_id, op_to_count);
        },
    );
}

/// Drains all currently available messages from a receiver into a Vec.
/// Uses try_recv() to avoid blocking if the channel is empty but not closed.
fn drain_receiver(receiver: &mut UnboundedReceiver<String>) -> Vec<String> {
    let mut lines = Vec::new();
    while let Ok(line) = receiver.try_recv() {
        lines.push(line);
    }
    lines
}

pub async fn analyze_process_results(
    process: &impl DeployCrateWrapper,
    ir: &mut [HydroRoot],
    op_to_count: &mut HashMap<usize, usize>,
    metrics: &mut MetricLogs,
) -> (f64, NetworkStats) {
    let underlying = process.underlying();
    let perf_results = underlying.tracing_results().unwrap();

    // Inject perf usages into metadata
    let unidentified_perf = inject_perf(ir, perf_results.folded_data.clone());

    // Parse all metric streams
    parse_counter_usage(drain_receiver(&mut metrics.counters), op_to_count);
    let network_stats = parse_network_usage(drain_receiver(&mut metrics.network));
    (unidentified_perf, network_stats)
}

pub async fn analyze_cluster_results(
    nodes: &DeployResult<'_, HydroDeploy>,
    ir: &mut [HydroRoot],
    mut cluster_metrics: HashMap<(LocationId, String, usize), MetricLogs>,
    run_metadata: &mut RunMetadata,
    exclude: &[String],
) -> (LocationId, String, usize) {
    let mut max_usage_cluster_id = None;
    let mut max_usage_cluster_size = 0;
    let mut max_usage_cluster_name = String::new();
    let mut max_usage_overall = 0f64;
    let mut op_to_count = HashMap::new();

    for (id, name, cluster) in nodes.get_all_clusters() {
        println!("Analyzing cluster {:?}: {}", id, name);

        // Iterate through nodes' usages and keep the max usage one
        let mut max_usage = None;
        for (idx, _) in cluster.members().iter().enumerate() {
            let usage = get_usage(
                &mut cluster_metrics
                    .get_mut(&(id.clone(), name.to_string(), idx))
                    .unwrap()
                    .cpu,
            )
            .await;
            println!("Node {} usage: {}", idx, usage);
            if let Some((prev_usage, _)) = max_usage {
                if usage > prev_usage {
                    max_usage = Some((usage, idx));
                }
            } else {
                max_usage = Some((usage, idx));
            }
        }

        if let Some((usage, idx)) = max_usage {
            // Modify IR with perf & cardinality numbers
            let metrics = cluster_metrics
                .get_mut(&(id.clone(), name.to_string(), idx))
                .unwrap();
            let (unidentified_perf, network_stats) = analyze_process_results(
                cluster.members().get(idx).unwrap(),
                ir,
                &mut op_to_count,
                metrics,
            )
            .await;

            run_metadata.total_usage.insert(id.clone(), usage);
            run_metadata
                .unaccounted_perf
                .insert(id.clone(), unidentified_perf);
            run_metadata
                .network_stats
                .insert(id.clone(), network_stats.clone());
            println!("Network stats for {}: {:?}", idx, network_stats);

            // Update cluster with max usage
            if max_usage_overall < usage && !exclude.contains(&name.to_string()) {
                max_usage_cluster_id = Some(id);
                max_usage_cluster_name = name.to_string();
                max_usage_cluster_size = cluster.members().len();
                max_usage_overall = usage;
                println!("The bottleneck is {}", name);
            }
        }
    }

    // Collect throughput/latency from all processes (aggregator outputs these)
    for metrics in cluster_metrics.values_mut() {
        // Filter metrics until we find the Aggregator
        let throughputs = drain_receiver(&mut metrics.throughputs);
        if throughputs.is_empty() {
            continue;
        }

        run_metadata.throughput = parse_throughput(throughputs);
        run_metadata.latencies = parse_latency(drain_receiver(&mut metrics.latencies));
    }

    inject_count(ir, &op_to_count);

    (
        max_usage_cluster_id.unwrap(),
        max_usage_cluster_name,
        max_usage_cluster_size,
    )
}

pub async fn get_usage(usage_out: &mut UnboundedReceiver<String>) -> f64 {
    let measurement = usage_out.recv().await.unwrap();
    parse_cpu_usage(measurement)
}

// Track the max of each so we decouple conservatively
pub fn analyze_send_recv_overheads(ir: &mut [HydroRoot], run_metadata: &mut RunMetadata) {
    traverse_dfir::<HydroDeploy>(
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
