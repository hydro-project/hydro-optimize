use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{
    HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, traverse_dfir,
};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::dynamic::LocationId;
use regex::Regex;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::debug::print_id;

#[derive(Default)]
pub struct RunMetadata {
    pub send_overhead: HashMap<LocationId, f64>,
    pub recv_overhead: HashMap<LocationId, f64>,
    pub unaccounted_perf: HashMap<LocationId, f64>, // % of perf samples not mapped to any operator
    pub total_usage: HashMap<LocationId, f64>,      // 100% CPU = 1.0
    pub op_id_to_prev_iteration_op_id: HashMap<usize, usize>,
    pub op_id_to_location: HashMap<usize, LocationId>,
    pub op_id_to_cpu_usage: HashMap<usize, f64>,
    pub op_id_to_recv_cpu_usage: HashMap<usize, f64>,
    pub op_id_to_cardinality: HashMap<usize, usize>,
    pub op_id_to_parent_op_id: HashMap<usize, Vec<usize>>,
    pub network_op_id: HashSet<usize>,
}

pub type MultiRunMetadata = Vec<RunMetadata>;

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
        | HydroNode::Persist { metadata, .. }
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
    traverse_dfir(
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
    node_cardinality: &mut UnboundedReceiver<String>,
) -> f64 {
    let perf_results = process.tracing_results().await.unwrap();

    // Inject perf usages into metadata
    let unidentified_usage = inject_perf(ir, perf_results.folded_data);

    // Get cardinality data. Allow later values to overwrite earlier ones
    while let Some(measurement) = node_cardinality.recv().await {
        let (op_id, count) = parse_counter_usage(measurement.clone());
        op_to_count.insert(op_id, count);
    }

    unidentified_usage
}

pub async fn analyze_cluster_results(
    nodes: &DeployResult<'_, HydroDeploy>,
    ir: &mut [HydroRoot],
    usage_out: &mut HashMap<(LocationId, String, usize), UnboundedReceiver<String>>,
    cardinality_out: &mut HashMap<(LocationId, String, usize), UnboundedReceiver<String>>,
    run_metadata: &mut RunMetadata,
    exclude_from_decoupling: Vec<String>,
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
            let usage =
                get_usage(usage_out.get_mut(&(id.clone(), name.clone(), idx)).unwrap()).await;
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
            let node_cardinality = cardinality_out
                .get_mut(&(id.clone(), name.clone(), idx))
                .unwrap();
            let unidentified_perf = analyze_process_results(
                cluster.members().get(idx).unwrap(),
                ir,
                &mut op_to_count,
                node_cardinality,
            )
            .await;

            run_metadata.total_usage.insert(id.clone(), usage);
            run_metadata
                .unaccounted_perf
                .insert(id.clone(), unidentified_perf);

            // Update cluster with max usage
            if max_usage_overall < usage && !exclude_from_decoupling.contains(&name) {
                max_usage_cluster_id = Some(id);
                max_usage_cluster_name = name.clone();
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
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            if let HydroNode::Network {
                input, metadata, ..
            } = node
            {
                let sender = input.metadata().location_kind.root();
                let receiver = metadata.location_kind.root();

                // Use cardinality from the network's parent, not the network itself.
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

pub fn get_or_append_run_metadata(
    multi_run_metadata: &mut MultiRunMetadata,
    iteration: usize,
) -> &mut RunMetadata {
    while multi_run_metadata.len() < iteration + 1 {
        multi_run_metadata.push(RunMetadata::default());
    }
    multi_run_metadata.get_mut(iteration).unwrap()
}

fn op_id_to_orig_id(
    op_id: usize,
    multi_run_metadata: &RefCell<MultiRunMetadata>,
    iteration: usize,
) -> Option<usize> {
    if iteration == 0 {
        return Some(op_id);
    }
    if let Some(prev_iter_id) = multi_run_metadata
        .borrow()
        .get(iteration)
        .unwrap()
        .op_id_to_prev_iteration_op_id
        .get(&op_id)
    {
        op_id_to_orig_id(*prev_iter_id, multi_run_metadata, iteration - 1)
    } else {
        None
    }
}

fn record_metadata(
    metadata: &HydroIrOpMetadata,
    input_metadata: Vec<&HydroIrMetadata>,
    run_metadata: &mut RunMetadata,
) {
    let id = metadata.id.unwrap();

    if let Some(cpu_usage) = metadata.cpu_usage {
        run_metadata.op_id_to_cpu_usage.insert(id, cpu_usage);
    }
    if let Some(network_recv_cpu_usage) = metadata.network_recv_cpu_usage {
        run_metadata
            .op_id_to_recv_cpu_usage
            .insert(id, network_recv_cpu_usage);
    }

    let parent_ids = input_metadata
        .iter()
        .filter_map(|parent| parent.op.id)
        .collect();
    run_metadata.op_id_to_parent_op_id.insert(id, parent_ids);
}

fn record_metadata_root(root: &mut HydroRoot, run_metadata: &mut RunMetadata) {
    record_metadata(
        root.op_metadata(),
        vec![root.input_metadata()],
        run_metadata,
    );

    // Location = parent's location, cardinality = parent's cardinality
    let id = root.op_metadata().id.unwrap();
    let parent = root.input_metadata();
    run_metadata
        .op_id_to_location
        .insert(id, parent.location_kind.root().clone());
    if let Some(cardinality) = parent.cardinality {
        run_metadata.op_id_to_cardinality.insert(id, cardinality);
    }
}

fn record_metadata_node(node: &mut HydroNode, run_metadata: &mut RunMetadata) {
    record_metadata(node.op_metadata(), node.input_metadata(), run_metadata);

    let id = node.op_metadata().id.unwrap();
    let metadata = node.metadata();
    run_metadata
        .op_id_to_location
        .insert(id, metadata.location_kind.root().clone());
    if let Some(cardinality) = metadata.cardinality {
        run_metadata.op_id_to_cardinality.insert(id, cardinality);
    }

    // Track network nodes
    if let HydroNode::Network { .. } = node {
        run_metadata.network_op_id.insert(id);
    }
}

fn compare_expected_values(
    new_value: f64,
    old_value: f64,
    new_location: &LocationId,
    old_location: &LocationId,
    orig_id: usize,
    value_name: &str,
) {
    println!(
        "New location {:?}, old location {:?}: Operator {} {}, new value {:?}, old value {:?}",
        new_location, old_location, orig_id, value_name, new_value, old_value
    );
}

/// If the op_id is a network node, return the sender's location by checking its parent. Otherwise return the operator's location
fn sender_location_if_network(run_metadata: &RunMetadata, op_id: usize) -> &LocationId {
    if run_metadata.network_op_id.contains(&op_id) {
        let parent = run_metadata.op_id_to_parent_op_id.get(&op_id).unwrap();
        assert!(
            parent.len() == 1,
            "Network operator should have exactly one input"
        );
        run_metadata.op_id_to_location.get(&parent[0]).unwrap()
    } else {
        run_metadata.op_id_to_location.get(&op_id).unwrap()
    }
}

/// Compares the performance of the current iteration against the previous one.
pub fn compare_expected_performance(
    ir: &mut [HydroRoot],
    multi_run_metadata: &RefCell<MultiRunMetadata>,
    iteration: usize,
) {
    print_id(ir);

    // Record run_metadata
    traverse_dfir(
        ir,
        |root, _| {
            record_metadata_root(
                root,
                multi_run_metadata.borrow_mut().get_mut(iteration).unwrap(),
            );
        },
        |node, _| {
            record_metadata_node(
                node,
                multi_run_metadata.borrow_mut().get_mut(iteration).unwrap(),
            );
        },
    );

    // Nothing to compare against for the 1st run, return
    if iteration == 0 {
        return;
    }

    // Compare against previous runs
    let borrowed_multi_run_metadata = multi_run_metadata.borrow();
    let run_metadata = borrowed_multi_run_metadata.get(iteration).unwrap();
    let prev_run_metadata = borrowed_multi_run_metadata.get(iteration - 1).unwrap();

    // 1. Compare operators with an orig_id
    for (op_id, prev_id) in run_metadata.op_id_to_prev_iteration_op_id.iter() {
        let orig_id = op_id_to_orig_id(*op_id, multi_run_metadata, iteration).unwrap();
        let new_location = run_metadata.op_id_to_location.get(op_id).unwrap();
        let old_location = prev_run_metadata.op_id_to_location.get(prev_id).unwrap();

        // Compare CPU usage
        if let Some(cpu_usage) = run_metadata.op_id_to_cpu_usage.get(op_id)
            && let Some(prev_cpu_usage) = prev_run_metadata.op_id_to_cpu_usage.get(prev_id)
        {
            compare_expected_values(
                *cpu_usage,
                *prev_cpu_usage,
                sender_location_if_network(run_metadata, *op_id),
                sender_location_if_network(prev_run_metadata, *prev_id),
                orig_id,
                "CPU usage",
            );
        }

        // Compare recv CPU usage
        if let Some(network_recv_cpu_usage) = run_metadata.op_id_to_recv_cpu_usage.get(op_id)
            && let Some(prev_network_recv_cpu_usage) =
                prev_run_metadata.op_id_to_recv_cpu_usage.get(prev_id)
        {
            compare_expected_values(
                *network_recv_cpu_usage,
                *prev_network_recv_cpu_usage,
                new_location,
                old_location,
                orig_id,
                "recv CPU usage",
            );
        }

        // Compare cardinality
        if let Some(cardinality) = run_metadata.op_id_to_cardinality.get(op_id)
            && let Some(prev_cardinality) = prev_run_metadata.op_id_to_cardinality.get(prev_id)
        {
            compare_expected_values(
                *cardinality as f64,
                *prev_cardinality as f64,
                new_location,
                old_location,
                orig_id,
                "cardinality",
            );
        }
    }

    // 2. Compare operators without orig_id (added by decoupling)
    let mut prev_id_and_loc_to_send_usage = HashMap::new(); // (id in prev iteration, LocationId of sender) -> CPU usage of decoupled output nodes
    let mut prev_id_and_loc_to_recv_usage = HashMap::new(); // (id in prev iteration, LocationId of receiver) -> CPU usage of decoupled parent nodes
    for (id, location) in run_metadata.op_id_to_location.iter() {
        if run_metadata.op_id_to_prev_iteration_op_id.contains_key(id) {
            continue;
        }

        // A. Find the ancestor that existed in the previous iteration
        let mut parent_id = None;
        let mut parent_prev_id = None;
        let mut curr_id = *id;
        while parent_prev_id.is_none() {
            let parents = run_metadata.op_id_to_parent_op_id.get(&curr_id).unwrap();
            assert_eq!(
                parents.len(),
                1,
                "Warning: Location {:?}: Created operator {} has {} parents, expected 1",
                location,
                id,
                parents.len()
            );
            let parent = parents[0];

            if let Some(prev_id) = run_metadata.op_id_to_prev_iteration_op_id.get(&parent) {
                parent_prev_id = Some(*prev_id);
                parent_id = Some(parent);
            } else {
                curr_id = parent;
            }
        }

        // B. Add this operator's usages
        let parent_location = run_metadata
            .op_id_to_location
            .get(&parent_id.unwrap())
            .unwrap();
        let is_network = run_metadata.network_op_id.contains(id);

        if parent_location == location || is_network {
            // This operator is on the sender
            if let Some(cpu_usage) = run_metadata.op_id_to_cpu_usage.get(id) {
                prev_id_and_loc_to_send_usage
                    .entry((
                        parent_prev_id.unwrap(),
                        sender_location_if_network(run_metadata, *id),
                    ))
                    .and_modify(|usage| {
                        *usage += *cpu_usage;
                    })
                    .or_insert_with(|| *cpu_usage);
            }
        }
        if parent_location != location || is_network {
            // This operator is on the recipient
            let cpu_usage = if is_network {
                run_metadata.op_id_to_recv_cpu_usage.get(id)
            } else {
                run_metadata.op_id_to_cpu_usage.get(id)
            };

            if let Some(cpu_usage) = cpu_usage {
                prev_id_and_loc_to_recv_usage
                    .entry((parent_prev_id.unwrap(), location.clone()))
                    .and_modify(|usage| {
                        *usage += *cpu_usage;
                    })
                    .or_insert_with(|| *cpu_usage);
            }
        }
    }
    // C. Compare changes in send CPU usage
    for ((prev_id, location), cpu_usage) in prev_id_and_loc_to_send_usage {
        if let Some(prev_cardinality) = prev_run_metadata.op_id_to_cardinality.get(&prev_id)
            && let Some(prev_location) = prev_run_metadata.op_id_to_location.get(&prev_id)
        {
            compare_expected_values(
                cpu_usage,
                prev_run_metadata
                    .send_overhead
                    .get(prev_location)
                    .cloned()
                    .unwrap_or_default()
                    * *prev_cardinality as f64,
                location,
                prev_location,
                op_id_to_orig_id(prev_id, multi_run_metadata, iteration - 1).unwrap(),
                "decoupled send CPU usage",
            );
        }
    }
    // D. Compare changes in recv CPU usage
    for ((prev_id, location), cpu_usage) in prev_id_and_loc_to_recv_usage {
        if let Some(prev_cardinality) = prev_run_metadata.op_id_to_cardinality.get(&prev_id)
            && let Some(prev_location) = prev_run_metadata.op_id_to_location.get(&prev_id)
        {
            compare_expected_values(
                cpu_usage,
                prev_run_metadata
                    .recv_overhead
                    .get(prev_location)
                    .cloned()
                    .unwrap_or_default()
                    * *prev_cardinality as f64,
                &location,
                prev_location,
                op_id_to_orig_id(prev_id, multi_run_metadata, iteration - 1).unwrap(),
                "decoupled recv CPU usage",
            );
        }
    }

    // 3. Compare changes in unexplained CPU usage
    for (location, unexplained) in &run_metadata.unaccounted_perf {
        if let Some(old_unexplained) = prev_run_metadata.unaccounted_perf.get(location) {
            println!(
                "Location {:?}'s unexplained CPU usage changed from {} to {}",
                location, old_unexplained, unexplained
            );
        }
    }

    // 4. Compare changes in send/recv overheads
    for (location, send_overhead) in &run_metadata.send_overhead {
        if let Some(old_send_overhead) = prev_run_metadata.send_overhead.get(location) {
            println!(
                "Location {:?}'s send overhead changed from {} to {}",
                location, old_send_overhead, send_overhead
            );
        }
    }
    for (location, recv_overhead) in &run_metadata.recv_overhead {
        if let Some(old_recv_overhead) = prev_run_metadata.recv_overhead.get(location) {
            println!(
                "Location {:?}'s recv overhead changed from {} to {}",
                location, old_recv_overhead, recv_overhead
            );
        }
    }
}
