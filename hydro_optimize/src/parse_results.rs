use std::collections::HashMap;

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy_and_analyze::Metrics;

#[derive(Default)]
pub struct RunMetadata {
    pub send_overhead: HashMap<LocationId, f64>,
    pub recv_overhead: HashMap<LocationId, f64>,
    pub unaccounted_perf: HashMap<LocationId, f64>, // % of perf samples not mapped to any operator
    pub total_usage: HashMap<LocationId, f64>,      // 100% CPU = 1.0
}

pub fn parse_cpu_usage(measurement: String) -> f64 {
    let regex = Regex::new(r"Total (\d+\.\d+)%").unwrap();
    regex
        .captures_iter(&measurement)
        .last()
        .map(|cap| cap[1].parse::<f64>().unwrap())
        .unwrap_or(0f64)
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
pub fn parse_counter_usage(measurement: String) -> (usize, usize) {
    let regex = Regex::new(r"\((\d+)\): (\d+)").unwrap();
    let matches = regex.captures_iter(&measurement).last().unwrap();
    let op_id = matches[1].parse::<usize>().unwrap();
    let count = matches[2].parse::<usize>().unwrap();
    (op_id, count)
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

pub async fn analyze_process_results(
    process: &impl DeployCrateWrapper,
    ir: &mut [HydroRoot],
    op_to_count: &mut HashMap<usize, usize>,
    metrics: &mut Metrics,
) -> f64 {
    let underlying = process.underlying();
    let perf_results = underlying.tracing_results().unwrap();

    // Inject perf usages into metadata
    let unidentified_usage = inject_perf(ir, perf_results.folded_data.clone());

    // Get cardinality data. Allow later values to overwrite earlier ones
    while let Some(measurement) = metrics.counters.recv().await {
        let (op_id, count) = parse_counter_usage(measurement.clone());
        op_to_count.insert(op_id, count);
    }
    while let Some(network_usage) = metrics.network.recv().await {
        println!("Network: {}", network_usage);
    }

    unidentified_usage
}

pub async fn analyze_cluster_results(
    nodes: &DeployResult<'_, HydroDeploy>,
    ir: &mut [HydroRoot],
    mut cluster_metrics: HashMap<(LocationId, String, usize), Metrics>,
    run_metadata: &mut RunMetadata,
    exclude: &Vec<String>,
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
            println!("{} measurements", idx);
            let unidentified_perf = analyze_process_results(
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
