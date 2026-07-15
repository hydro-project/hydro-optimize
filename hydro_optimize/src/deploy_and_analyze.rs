use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use hydro_deploy::{Deployment, HYDRO_NET_CALIBRATE_ENV};
use hydro_lang::compile::built::BuiltFlow;
use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{HydroNode, HydroRoot, deep_clone, traverse_dfir};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Location, LocationKey, LocationType};
use hydro_lang::prelude::{Cluster, FlowBuilder, Process};
use hydro_lang::telemetry::Sidecar;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::decouple_analysis::{IlpInputs, Rewrite, find_optimal_budget};
use crate::deploy::{AWS_IO_TPS, AWS_NETWORK_BYTES_PER_SEC, HostType, ReusableHosts};
use crate::greedy_decouple_analysis::greedy_decouple_analysis;
use crate::parse_results::{
    ExhaustiveOptimizations, ExhaustiveSearchState, OptimizationState, RawIlpInputs, RunMetadata,
    analyze_cluster_results, decoupled_cluster_name, derive_per_op_load,
    find_bottleneck_across_runs, find_latest_iteration, find_workload_dirs, get_csvs_in_dir,
    load_json, load_raw_ilp_inputs, original_cluster_name,
};
use crate::reduce_pushdown::reduce_pushdown;
use crate::reduce_pushdown_analysis::reduce_pushdown_decision;
use crate::repair::{cycle_source_to_sink_parent, inject_id};
use crate::rewriter::apply_rewrite;
use crate::rewrites::{
    collection_kind_to_debug_type, get_network_op_ids, op_id_to_parents, save_id, tee_to_inner_id,
};

const METRIC_INTERVAL_SECS: u64 = 1;
const BATCH_PULL_LIMIT: usize = 10;
const CALIBRATION_DIR: &str = "benchmark_results";
const COUNTER_PREFIX: &str = "_optimize_counter";
const BYTE_SIZE_PREFIX: &str = "_optimize_byte_size";
const CPU_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_CPU:";
const SAR_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_SAR:";
const LATENCY_PREFIX: &str = "HYDRO_OPTIMIZE_LAT:";
const THROUGHPUT_PREFIX: &str = "HYDRO_OPTIMIZE_THR:";

pub const MAX_BUDGET_PER_CLUSTER: usize = 10;
const BIMODAL_CV_THRESHOLD: f64 = 0.3;

/// Returns true if min throughput in the measurement window is significantly lower than the max overall
fn run_was_unstable(throughputs: &[usize]) -> bool {
    if throughputs.len() <= MEASUREMENT_SECOND {
        println!("Throughput not fully recorded for run, cannot determine stability");
        return false;
    }
    let window = &throughputs[START_MEASUREMENT_SECOND..=MEASUREMENT_SECOND];
    let mut sorted = throughputs.to_vec();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    let second_highest = sorted.get(1).copied().unwrap_or(0) as f64;
    if second_highest == 0.0 {
        println!("2nd highest throughput was 0, cannot determine stability");
        return false;
    }
    let min_window = window.iter().copied().min().unwrap_or(0) as f64;
    (second_highest - min_window) / second_highest > BIMODAL_CV_THRESHOLD
}

// Note: Ensure edits to the match arms are consistent with infer_counter_from_parent
fn insert_counter_node(
    node: &mut HydroNode,
    next_stmt_id: &mut usize,
    duration: syn::Expr,
    exclude: &HashSet<LocationId>,
) {
    match node {
        HydroNode::Placeholder
        | HydroNode::Counter { .. } => {
            std::panic!("Unexpected {:?} found in insert_counter_node", node.print_root());
        }
        HydroNode::Source { metadata, .. }
        | HydroNode::CycleSource { metadata, .. }
        | HydroNode::Chain { metadata, .. } // Can technically be derived by summing parent cardinalities
        | HydroNode::ChainFirst { metadata, .. } // Can technically be derived by taking parent cardinality + 1
        | HydroNode::MergeOrdered { metadata, .. }
        | HydroNode::CrossSingleton { metadata, .. }
        | HydroNode::CrossProduct { metadata, .. } // Can technically be derived by multiplying parent cardinalities
        | HydroNode::Join { metadata, .. }
        | HydroNode::JoinHalf { metadata, .. }
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
        | HydroNode::Partition { metadata, .. }
         => {
            if exclude.contains(metadata.location_id.root()) {
                return;
            }

            // Use the original op_id from inject_id, not the traversal counter
            let Some(original_id) = metadata.op.id else {
                // If this node does not have an original id, it must have been added during the rewrite and we can ignore
                return;
            };
            let metadata = metadata.clone();
            let node_content = std::mem::replace(node, HydroNode::Placeholder);

            let counter = HydroNode::Counter {
                tag: original_id.to_string(),
                duration: duration.into(),
                prefix: COUNTER_PREFIX.to_string(),
                input: Box::new(node_content),
                metadata: metadata.clone(),
            };

            // when we emit this IR, the counter will bump the stmt id, so simulate that here
            *next_stmt_id += 1;

            *node = counter;
        }
        HydroNode::Tee { .. } // Do nothing, we will count the parent of the Tee
        | HydroNode::Map { .. } // Equal to parent cardinality
        | HydroNode::DeferTick { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::Sort { .. }
        | HydroNode::Cast { .. }
        | HydroNode::ObserveNonDet { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
         => {}
    }
}

fn insert_counters<'a>(built: BuiltFlow<'a>, exclude: &HashSet<LocationId>) -> BuiltFlow<'a> {
    let counter_output_duration: syn::Expr =
        syn::parse_quote!(std::time::Duration::from_secs(#METRIC_INTERVAL_SECS));
    built.optimize_with(|leaf| {
        traverse_dfir(
            leaf,
            |_, _| {},
            |node, next_stmt_id| {
                insert_counter_node(node, next_stmt_id, counter_output_duration.clone(), exclude);
            },
        );
    })
}

/// Returns true for node types whose cardinality equals their parent's and thus don't
/// get their own counter in `insert_counter_node`. Must be kept in sync with the
/// no-op match arm there.
fn inherits_parent_cardinality(node: &HydroNode) -> bool {
    matches!(
        node,
        HydroNode::Tee { .. }
            | HydroNode::Map { .. }
            | HydroNode::DeferTick { .. }
            | HydroNode::Enumerate { .. }
            | HydroNode::Inspect { .. }
            | HydroNode::Sort { .. }
            | HydroNode::Cast { .. }
            | HydroNode::ObserveNonDet { .. }
            | HydroNode::BeginAtomic { .. }
            | HydroNode::EndAtomic { .. }
            | HydroNode::Batch { .. }
            | HydroNode::YieldConcat { .. }
            | HydroNode::ResolveFutures { .. }
            | HydroNode::ResolveFuturesOrdered { .. }
            | HydroNode::ResolveFuturesBlocking { .. }
    )
}

/// Fills in counters for ops that inherit their parent's cardinality.
/// `op_id_to_parent` should be derived from the pre-rewrite IR so all parents have valid ids.
pub fn inject_inferred_counters(
    ir: &mut [HydroRoot],
    op_id_to_parent: &HashMap<usize, Vec<usize>>,
    counters: &mut HashMap<usize, usize>,
) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            if !inherits_parent_cardinality(node) {
                return;
            }
            let Some(original_id) = node.metadata().op.id else {
                return;
            };
            if let Some(parents) = op_id_to_parent.get(&original_id)
                && let Some(&parent_id) = parents.first()
                && let Some(&count) = counters.get(&parent_id)
            {
                counters.insert(original_id, count);
            }
        },
    );
}

/// Inserts an Inspect node that prints the serialized byte size,
/// as `BYTE_SIZE_PREFIX(original_op_id): size`.
/// `network_ops` contains original op_ids (before any insertions).
/// Inserted at:
/// - Ops where `network_ops` indicates a network boundary (the op executes at the sender
///   location and its output would be sent over the network, so measuring here captures
///   the serialized size that would cross the wire)
/// - After existing Network nodes
fn insert_byte_size_inspect(ir: &mut [HydroRoot], network_ops: &HashSet<usize>) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            // Use the original op_id from inject_id, not the traversal counter
            let original_id = node.metadata().op.id.unwrap();
            let is_network_boundary = network_ops.contains(&original_id);
            let is_existing_network = matches!(node, HydroNode::Network { .. });
            if !is_network_boundary && !is_existing_network {
                return;
            }

            let metadata = node.metadata().clone();
            let element_type: syn::Type =
                (*collection_kind_to_debug_type(&metadata.collection_kind).0).clone();
            let node_content = std::mem::replace(node, HydroNode::Placeholder);
            let tag = original_id.to_string();
            let prefix = BYTE_SIZE_PREFIX;

            let f: syn::Expr = syn::parse_quote!({
                move |item: &#element_type| {
                    let size = hydro_lang::runtime_support::bincode::serialize(item)
                        .map(|v| v.len() as u64)
                        .unwrap_or(0);
                    println!("{}({}): {}", #prefix, #tag, size);
                }
            });

            *node = HydroNode::Inspect {
                f: f.into(),
                input: Box::new(node_content),
                metadata,
            };
            *op_id += 1;
        },
    );
}

pub struct MetricLogs {
    pub throughputs: UnboundedReceiver<String>,
    pub latencies: UnboundedReceiver<String>,
    pub cpu: UnboundedReceiver<String>,
    pub sar: UnboundedReceiver<String>,
    pub counters: UnboundedReceiver<String>,
    pub byte_sizes: UnboundedReceiver<String>,
}

async fn track_process_metrics(process: &impl DeployCrateWrapper) -> MetricLogs {
    MetricLogs {
        throughputs: process.stdout_filter(THROUGHPUT_PREFIX),
        latencies: process.stdout_filter(LATENCY_PREFIX),
        cpu: process.stdout_filter(CPU_USAGE_PREFIX),
        sar: process.stdout_filter(SAR_USAGE_PREFIX),
        counters: process.stdout_filter(COUNTER_PREFIX),
        byte_sizes: process.stdout_filter(BYTE_SIZE_PREFIX),
    }
}

async fn track_cluster_metrics(
    nodes: &DeployResult<'_, HydroDeploy>,
) -> HashMap<(LocationId, String, usize), MetricLogs> {
    let mut cluster_to_metrics = HashMap::new();
    for (id, name, cluster) in nodes.get_all_clusters() {
        for (idx, node) in cluster.members().iter().enumerate() {
            let metrics = track_process_metrics(node).await;
            cluster_to_metrics.insert((id.clone(), name.to_string(), idx), metrics);
        }
    }
    for (id, name, process) in nodes.get_all_processes() {
        let metrics = track_process_metrics(process).await;
        cluster_to_metrics.insert((id.clone(), name.to_string(), 0), metrics);
    }
    cluster_to_metrics
}

pub struct ScriptSidecar {
    pub script: String,
    pub prefix: String,
}

impl Sidecar for ScriptSidecar {
    fn to_expr(
        &self,
        _flow_name: &str,
        location_key: LocationKey,
        _location_type: LocationType,
        _location_name: &str,
        _dfir_ident: &syn::Ident,
    ) -> syn::Expr {
        let script = &self.script;
        let prefix = &self.prefix;
        let location_key_str = format!("{:?}", location_key);

        syn::parse_quote! {
            async {
                use tokio::process::Command;
                use tokio::io::{BufReader, AsyncBufReadExt};
                use std::process::Stdio;

                let mut child = Command::new("sh")
                    .arg("-c")
                    .arg(#script)
                    .stdout(Stdio::piped())
                    .spawn()
                    .expect("Failed to spawn sidecar");

                let stdout = child.stdout.take().expect("Failed to open sidecar stdout");
                let mut reader = BufReader::new(stdout).lines();
                while let Some(line) = reader.next_line().await.expect("Failed to read line from sidecar") {
                    println!("{}{}: {}", #prefix, #location_key_str, line);
                }

                child.wait().await.expect("Failed to wait for sidecar");
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct ReusableClusters {
    pub named_clusters: Vec<(LocationId, String, usize)>,
}

impl ReusableClusters {
    pub fn with_cluster<C>(mut self, cluster: Cluster<'_, C>, num_members: usize) -> Self {
        self.named_clusters.push((
            cluster.id().clone(),
            std::any::type_name::<C>().to_string(),
            num_members,
        ));
        self
    }

    pub fn with_named(mut self, location: LocationId, name: String, num_members: usize) -> Self {
        self.named_clusters.push((location, name, num_members));
        self
    }

    pub fn set_num_members(&mut self, location: &LocationId, num_members: usize) {
        let Some((_, _, existing_num_members)) = self
            .named_clusters
            .iter_mut()
            .find(|(id, _, _)| location.key() == id.key())
        else {
            panic!("No reusable cluster found for location {:?}", location);
        };
        *existing_num_members = num_members;
    }

    pub fn from(named_clusters: Vec<(LocationId, String, usize)>) -> Self {
        Self { named_clusters }
    }

    pub fn find_by_name(&self, name: &str) -> Option<LocationId> {
        self.named_clusters
            .iter()
            .find(|(_, n, _)| n == name)
            .map(|(id, _, _)| id.clone())
    }

    pub fn location_name_and_num(&self, location: &LocationId) -> Option<(String, usize)> {
        self.named_clusters
            .iter()
            .find(|(id, _, _)| location.key() == id.key())
            .map(|(_, name, count)| (name.clone(), *count))
    }
}

#[derive(Default, Clone)]
pub struct ReusableProcesses {
    pub named_processes: Vec<(LocationId, String)>,
}

impl ReusableProcesses {
    pub fn with_process<P>(mut self, process: Process<'_, P>) -> Self {
        self.named_processes
            .push((process.id().clone(), std::any::type_name::<P>().to_string()));
        self
    }

    pub fn find_by_name(&self, name: &str) -> Option<LocationId> {
        self.named_processes
            .iter()
            .find(|(_, n)| n == name)
            .map(|(id, _)| id.clone())
    }
}

/// Mutually exclusive optimization strategies that rewrite the IR before deployment.
#[derive(Clone, Default)]
pub enum Optimization {
    /// No IR rewriting, no counters.
    #[default]
    None,
    /// No IR rewriting, but insert counters to measure per-op cardinality.
    Counters,
    /// Apply greedy decoupling, deploy decoupled system, gather per-operator SAR costs.
    /// Always inserts counters.
    BlowUpAnalysis,
    /// Insert size measuring nodes wherever decoupling is possible.
    SizeAnalysis,
    /// Deploy with `perf record` on non-excluded locations for flamegraph profiling.
    Perf,
    /// Run ILP to find optimal decoupling, save Rewrite to file, then exit without deploying.
    /// Automatically loads applied rewrites from the latest opt dir.
    BottleneckElimination,
    OptimizeWithLatencyBudget(usize),
    /// Load `{name}_exhaustive_optimizations.json` and benchmark every listed rewrite for a budget.
    ExhaustiveSearch(usize),
}

impl Optimization {
    pub fn label(&self) -> &'static str {
        match self {
            // Do not differentiate between None and BottleneckElimination, since both run the protocol without additional profiling
            Optimization::None
            | Optimization::BottleneckElimination
            | Optimization::OptimizeWithLatencyBudget(_)
            | Optimization::ExhaustiveSearch(_) => "none",
            Optimization::Counters => "counters",
            Optimization::BlowUpAnalysis => "blow_up",
            Optimization::SizeAnalysis => "size",
            Optimization::Perf => "perf",
        }
    }
}

/// Creates a flat output directory: `{CALIBRATION_DIR}/{name}_{workload}_{label}`.
/// Deletes and recreates if it already exists.
pub fn make_output_dir(name: &str, workload: &str, label: &str) -> std::path::PathBuf {
    let dir = Path::new(CALIBRATION_DIR).join(format!("{}_{}_{}", name, workload, label));
    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .unwrap_or_else(|e| panic!("Failed to remove {}: {}", dir.display(), e));
    }
    std::fs::create_dir_all(&dir)
        .unwrap_or_else(|e| panic!("Failed to create {}: {}", dir.display(), e));
    dir
}

/// Per-workload compiled artifacts. These depend on the compiled program's location IDs, so
/// they are produced per workload by the `compile` closure rather than shared in
/// `BenchmarkConfig`. All workloads of a protocol must produce an identical IR topology (same
/// op ids / clusters), differing only in runtime data.
#[derive(Clone, Default)]
pub struct CompiledProgram {
    pub clusters: ReusableClusters,
    pub processes: ReusableProcesses,
    /// Locations excluded from optimization (e.g. clients). Per-workload because it references
    /// the compiled program's location IDs.
    pub exclude: HashSet<LocationId>,
    pub location_id_to_cluster: HashMap<LocationId, String>,
}

impl CompiledProgram {
    pub fn new(
        clusters: ReusableClusters,
        processes: ReusableProcesses,
        location_id_to_cluster: HashMap<LocationId, String>,
    ) -> Self {
        Self {
            clusters,
            processes,
            exclude: HashSet::new(),
            location_id_to_cluster,
        }
    }

    /// Excludes a location (cluster/process) from optimization: it won't be decoupled,
    /// partitioned, profiled with `perf`, or chosen as a bottleneck.
    pub fn excluding(mut self, location: LocationId) -> Self {
        self.exclude.insert(location);
        self
    }
}

/// `stability_second`: The second in which the protocol is expected to be stable, and its performance can be used as the basis for optimization.
///
/// Any rewriting of the IR (loaded rewrites, blow-up analysis, size analysis) is expected to
/// have been performed by the caller already; this function only inserts counters, deploys,
/// waits, and parses metrics.
#[allow(clippy::too_many_arguments)]
async fn deploy_and_analyze<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    builder: BuiltFlow<'a>,
    program: &CompiledProgram,
    kind: &Optimization,
    num_seconds: Option<usize>,
) -> RunMetadata {
    // Measure network (-n DEV) and CPU (-u) usage. -P 0 measures only core 0, where the
    // hydro main thread is pinned (networking threads spin on other cores, so whole-machine
    // CPU is misleading).
    let sar_sidecar = ScriptSidecar {
        script: "sar -n DEV -u -P 0 -r -b 1".to_string(),
        prefix: SAR_USAGE_PREFIX.to_string(),
    };

    let use_perf = matches!(kind, Optimization::Perf);

    // Insert all clusters & processes
    let mut deployable = builder.into_deploy();
    let mut exhaustive_machine_idx = 0usize;
    for (cluster_id, name, num_hosts) in program.clusters.named_clusters.iter() {
        let excluded = program.exclude.contains(cluster_id);
        let hosts = if let Optimization::ExhaustiveSearch(budget) = kind
            && !excluded
        {
            // Don't want to keep relaunching machines during exhaustive search,
            // So make sure they always have the same host name
            (0..*num_hosts)
                .map(|_| {
                    let host_name = format!(
                        "exhaustive_budget{}_machine{}",
                        budget, exhaustive_machine_idx
                    );
                    exhaustive_machine_idx += 1;
                    reusable_hosts.get_process_host(deployment, host_name, use_perf)
                })
                .collect()
        } else {
            reusable_hosts.get_cluster_hosts(
                deployment,
                name.clone(),
                *num_hosts,
                use_perf && !excluded,
            )
        };
        deployable = deployable.with_cluster_erased(cluster_id.key(), hosts);
    }
    for (process_id, name) in program.processes.named_processes.iter() {
        let excluded = program.exclude.contains(process_id);
        let host = reusable_hosts.get_process_host(deployment, name.clone(), use_perf && !excluded);
        deployable = deployable.with_process_erased(process_id.key(), host);
    }
    for excluded_location in program.exclude.iter() {
        deployable = deployable.set_batch_limit_from(excluded_location.key(), BATCH_PULL_LIMIT);
    }
    deployable = deployable.with_sidecar_all(&sar_sidecar);

    let nodes = deployable.deploy(deployment);
    deployment.deploy().await.unwrap();
    let metrics = track_cluster_metrics(&nodes).await;

    // Wait for user to input a newline or timeout
    deployment
        .start_until(async {
            if let Some(seconds) = num_seconds {
                // Wait for some number of seconds
                tokio::time::sleep(Duration::from_secs(seconds as u64)).await;
            } else {
                // Wait for a new line
                eprintln!("Press enter to stop deployment and analyze results");
                let _ = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(
                    tokio::io::stdin(),
                ))
                .next_line()
                .await
                .unwrap();
            }
        })
        .await
        .unwrap();

    // Parse results to get metrics
    analyze_cluster_results(&nodes, metrics).await
}

pub struct BenchmarkArgs {
    pub gcp: Option<String>,
    pub aws: bool,
}

/// Static benchmark configuration, shared across all workloads of a protocol. Per-workload,
/// program-dependent data (clusters, processes, excluded ids, location map) lives in
/// `CompiledProgram`; the workload name comes from the `(params, name)` workload mapping.
#[derive(Clone)]
pub struct BenchmarkConfig {
    /// Base protocol name (e.g. "CAS"). Output dirs are `{name}[_optN]_{workload}_{label}`.
    pub name: String,
    /// Which optimization/analysis strategy to run. Static across workloads.
    pub kind: Optimization,
    pub num_physical_clients: usize,
    pub start_virtual_clients: usize,
    pub virtual_clients_step: usize,
    /// Number of successful runs to collect per virtual-client count.
    pub num_runs: usize,
    /// If set, enables network calibration mode: sets `HYDRO_NET_CALIBRATE=<size>`
    /// on all hosts and forces single-host deployment.
    pub calibrate_message_sizes: Option<Vec<usize>>,
}

pub const START_MEASUREMENT_SECOND: usize = 30;
pub const MEASUREMENT_SECOND: usize = 59;
pub const RUN_SECONDS: usize = 90;
pub const NUM_PHYSICAL_CLIENTS: usize = 10;
pub const NUM_VIRTUAL_CLIENTS_ENV: &str = "NUM_VIRTUAL_CLIENTS";
pub const NUM_RUNS_NO_THROUGHPUT: usize = 3;
pub const NO_IMPROVEMENT_LIMIT: usize = 3;

/// Applies a single `rewrite`, creating any new clusters it requires on `builder`. Records
/// which original op ids landed on which deployed location in `location_to_original_ops`.
fn apply_single_rewrite<'a>(
    ir: &mut [HydroRoot],
    builder: &mut FlowBuilder<'a>,
    clusters: &mut ReusableClusters,
    location_to_original_ops: &mut HashMap<LocationId, Vec<usize>>,
    location_id_to_cluster: &mut HashMap<LocationId, String>,
    rewrite: Rewrite,
    tee_to_inner: &HashMap<usize, usize>,
) {
    // Build locations_map: index 0 = original, index > 0 = new clusters.
    let mut locations_map = HashMap::from([(0, rewrite.original_location.clone())]);
    let original_name = location_id_to_cluster
        .get(&rewrite.original_location)
        .cloned()
        .unwrap_or_else(|| format!("{:?}", rewrite.original_location));
    if let Some(num_partitions) = rewrite.num_partitions.get(&0).copied() {
        clusters.set_num_members(
            &rewrite.original_location,
            rewrite.cluster_size * num_partitions.max(1),
        );
    }
    for loc_idx in rewrite.locations() {
        if loc_idx == 0 {
            continue;
        }
        let cluster = builder.cluster::<()>();
        let new_loc = cluster.id().clone();
        let name = decoupled_cluster_name(&original_name, loc_idx);
        locations_map.insert(loc_idx, new_loc.clone());
        location_id_to_cluster.insert(new_loc.clone(), name.clone());
        let num_partitions = rewrite
            .num_partitions
            .get(&loc_idx)
            .copied()
            .unwrap_or(1)
            .max(1);
        *clusters = std::mem::take(clusters).with_named(
            new_loc,
            name,
            rewrite.cluster_size * num_partitions,
        );
    }

    for (&op_id, &loc_idx) in &rewrite.op_to_loc {
        let deployed_loc = locations_map.get(&loc_idx).unwrap();
        location_to_original_ops
            .entry(deployed_loc.clone())
            .or_default()
            .push(op_id);
    }

    apply_rewrite(ir, &rewrite, &locations_map, tee_to_inner);
}

/// Applies every loaded rewrite (reduce pushdown + inject_id + apply_rewrite) in sequence.
pub fn apply_loaded_rewrites<'a>(
    built: BuiltFlow<'a>,
    clusters: &mut ReusableClusters,
    loaded_rewrites: &[Rewrite],
    location_id_to_cluster: &mut HashMap<LocationId, String>,
) -> BuiltFlow<'a> {
    if loaded_rewrites.is_empty() {
        return built;
    }

    let mut location_to_original_ops: HashMap<LocationId, Vec<usize>> = HashMap::new();
    let mut builder = FlowBuilder::from_built(&built);
    let mut ir = deep_clone(built.ir());
    let tee_to_inner = tee_to_inner_id(&mut ir);

    for (i, rewrite) in loaded_rewrites.iter().cloned().enumerate() {
        println!(
            "=== Applying rewrite {}/{}: cluster={:?} budget={} op_to_network={:?} ===",
            i + 1,
            loaded_rewrites.len(),
            rewrite.original_location,
            rewrite.budget,
            rewrite.op_to_network
        );
        apply_single_rewrite(
            &mut ir,
            &mut builder,
            clusters,
            &mut location_to_original_ops,
            location_id_to_cluster,
            rewrite,
            &tee_to_inner,
        );

        // Need to reinject ID after each rewrite, since the next rewrite will reference the latest op IDs
        inject_id(&mut ir);
    }

    builder.replace_ir(ir);
    builder.finalize()
}

/// Applies the greedy-decoupled rewrite (blow-up analysis). Returns the new IR, updated
/// clusters (with any newly created decouple clusters), location → original op ids map,
/// and the pre-rewrite parent map (for inferring counters after deployment).
#[allow(clippy::type_complexity)]
fn apply_blow_up_analysis<'a>(
    built: BuiltFlow<'a>,
    clusters: &mut ReusableClusters,
    exclude: &HashSet<LocationId>,
    location_id_to_cluster: &mut HashMap<LocationId, String>,
) -> (
    BuiltFlow<'a>,
    HashMap<LocationId, Vec<usize>>,
    HashMap<usize, Vec<usize>>,
) {
    let mut location_to_original_ops: HashMap<LocationId, Vec<usize>> = HashMap::new();

    let mut greedy_results = None;
    let built = built.optimize_with(|leaf| {
        greedy_results = Some(greedy_decouple_analysis(leaf, exclude, clusters));
    });
    let mut builder = FlowBuilder::from_built(&built);
    let mut ir = deep_clone(built.ir());

    // Compute parent map and tee map before rewrites mutate the IR
    let cycles = cycle_source_to_sink_parent(&mut ir);
    let pre_rewrite_parents = op_id_to_parents(&mut ir, None, &cycles);
    let tee_to_inner = tee_to_inner_id(&mut ir);

    for rewrite in greedy_results.unwrap() {
        apply_single_rewrite(
            &mut ir,
            &mut builder,
            clusters,
            &mut location_to_original_ops,
            location_id_to_cluster,
            rewrite,
            &tee_to_inner,
        );
    }

    builder.replace_ir(ir);

    // Insert counters after rewrites, otherwise rewrites won't work on an outdated graph
    // Note that counters still reference each nodes' original op_id
    let built = insert_counters(builder.finalize(), exclude);
    (built, location_to_original_ops, pre_rewrite_parents)
}

/// Scales virtual clients from `start` to a computed max, deploying and collecting
/// metrics at each step. Handles multi-run averaging, bimodal detection, zero-throughput
/// retries, and early termination on throughput plateau, CPU saturation, or size-analysis mode.
#[allow(clippy::too_many_arguments)]
async fn run_scaling_loop<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    built: &BuiltFlow<'a>,
    config: &BenchmarkConfig,
    program: &CompiledProgram,
    run_name: &str,
    workload: &str,
    kind: &Optimization,
    location_to_original_ops: &HashMap<LocationId, Vec<usize>>,
    pre_rewrite_parents: Option<&HashMap<usize, Vec<usize>>>,
    size_analysis_ops: &HashSet<usize>,
) -> RunMetadata {
    let output_dir = make_output_dir(run_name, workload, kind.label());
    let ir = built.ir();
    save_id(&mut deep_clone(ir), &output_dir.join("id.txt"));
    let mut final_run_metadata = RunMetadata::default();
    let mut best_throughput: usize = 0;
    let mut no_improvement_count: usize = 0;
    let mut num_virtual = config.start_virtual_clients;

    'outer: loop {
        let mut throughput_sum = 0;
        let mut successful_runs = 0;
        let mut zero_throughput_count = 0;
        let mut run = 0;
        while successful_runs < config.num_runs {
            println!(
                "Running {} ({}) with {} virtual clients (run {})",
                run_name,
                kind.label(),
                num_virtual,
                run
            );
            reusable_hosts.insert_env(NUM_VIRTUAL_CLIENTS_ENV.to_string(), num_virtual.to_string());

            let mut builder = FlowBuilder::from_built(built);
            builder.replace_ir(deep_clone(ir));
            let finalized = builder.finalize();

            let mut run_metadata = deploy_and_analyze(
                reusable_hosts,
                deployment,
                finalized,
                program,
                kind,
                Some(RUN_SECONDS),
            )
            .await;
            run_metadata.location_to_original_ops = location_to_original_ops.clone();

            let run_throughput = run_metadata.avg_throughput();
            println!(
                "Run throughput: {}, num_clients: {}",
                run_throughput, num_virtual
            );
            run_metadata.print_run_summary(&program.location_id_to_cluster, MEASUREMENT_SECOND);
            run_metadata.save_run_metadata(
                &program.location_id_to_cluster,
                &output_dir,
                config.num_physical_clients,
                num_virtual,
                run,
            );

            if matches!(kind, Optimization::BlowUpAnalysis) {
                run_metadata.save_blow_up_stats(
                    &output_dir,
                    &mut deep_clone(ir),
                    pre_rewrite_parents.unwrap(),
                    config.num_physical_clients,
                    num_virtual,
                    run,
                );
            }
            if !size_analysis_ops.is_empty() {
                run_metadata.save_size_analysis(&output_dir, size_analysis_ops);
            }
            if matches!(kind, Optimization::Counters) {
                run_metadata.save_counters(
                    &output_dir,
                    config.num_physical_clients,
                    num_virtual,
                    run,
                );
            }
            run_metadata.save_perf(&output_dir, config.num_physical_clients, num_virtual, run);

            let unstable_run = run_was_unstable(&run_metadata.throughputs);
            if run_throughput == 0 || unstable_run {
                zero_throughput_count += 1;
                if unstable_run {
                    println!(
                        "Unstable throughput detected ({}/{})",
                        zero_throughput_count, NUM_RUNS_NO_THROUGHPUT
                    );
                } else {
                    println!(
                        "Zero throughput detected ({}/{})",
                        zero_throughput_count, NUM_RUNS_NO_THROUGHPUT
                    );
                }
                if zero_throughput_count > NUM_RUNS_NO_THROUGHPUT {
                    return run_metadata;
                }
            } else {
                throughput_sum += run_throughput;
                successful_runs += 1;
            }
            run += 1;
            final_run_metadata = run_metadata;

            // Only run size analysis & network calibration once per config
            if matches!(kind, Optimization::SizeAnalysis)
                || config.calibrate_message_sizes.is_some()
            {
                break 'outer;
            }
        }

        let current_throughput = throughput_sum / successful_runs;
        println!(
            "clients={}, avg_throughput={}",
            num_virtual, current_throughput
        );

        if current_throughput > best_throughput {
            best_throughput = current_throughput;
            no_improvement_count = 0;
        } else {
            no_improvement_count += 1;
            println!(
                "No throughput improvement ({}/{})",
                no_improvement_count, NO_IMPROVEMENT_LIMIT
            );
            if no_improvement_count >= NO_IMPROVEMENT_LIMIT {
                println!(
                    "Throughput plateaued for {} consecutive iterations. Terminating.",
                    NO_IMPROVEMENT_LIMIT
                );
                break;
            }
        }

        // Only run exhaustive search on a specific config. But can run it multiple times
        if matches!(kind, Optimization::ExhaustiveSearch(_)) {
            break;
        }

        num_virtual += config.virtual_clients_step;
    }

    final_run_metadata
}

/// Runs a single analysis pass (counters, size, blow_up, or perf) on the given built flow.
/// Deploys with the appropriate instrumentation, scales virtual clients, and saves results.
#[allow(clippy::too_many_arguments)]
async fn run_analysis_pass<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    built: &BuiltFlow<'a>,
    config: &BenchmarkConfig,
    program: &mut CompiledProgram,
    run_name: &str,
    workload: &str,
    kind: Optimization,
) {
    println!("=== Running {} analysis ===", kind.label());

    let mut size_analysis_ops: HashSet<usize> = HashSet::new();
    let mut pre_rewrite_parents: Option<HashMap<usize, Vec<usize>>> = None;
    let mut location_to_original_ops: HashMap<LocationId, Vec<usize>> = HashMap::new();

    let mut builder = FlowBuilder::from_built(built);
    builder.replace_ir(deep_clone(built.ir()));
    let analysis_built = builder.finalize();

    let analysis_built = match &kind {
        Optimization::Counters => insert_counters(analysis_built, &program.exclude),
        Optimization::BlowUpAnalysis => {
            let (built, loc_ops, parents) = apply_blow_up_analysis(
                analysis_built,
                &mut program.clusters,
                &program.exclude,
                &mut program.location_id_to_cluster,
            );
            pre_rewrite_parents = Some(parents);
            location_to_original_ops = loc_ops;
            built
        }
        Optimization::SizeAnalysis => {
            let mut captured_ops = HashSet::new();
            let built = analysis_built.optimize_with(|leaf| {
                let per_loc_rewrites =
                    greedy_decouple_analysis(leaf, &program.exclude, &program.clusters);
                let network_ops: HashSet<usize> = per_loc_rewrites
                    .iter()
                    .flat_map(|r| r.op_to_network.keys().copied())
                    .collect();
                if !network_ops.is_empty() {
                    insert_byte_size_inspect(leaf, &network_ops);
                }
                captured_ops = network_ops;
            });
            size_analysis_ops = captured_ops;
            built
        }
        Optimization::Perf | Optimization::None | Optimization::ExhaustiveSearch(_) => {
            analysis_built
        }
        Optimization::BottleneckElimination | Optimization::OptimizeWithLatencyBudget(_) => {
            unreachable!()
        }
    };

    run_scaling_loop(
        reusable_hosts,
        deployment,
        &analysis_built,
        config,
        program,
        run_name,
        workload,
        &kind,
        &location_to_original_ops,
        pre_rewrite_parents.as_ref(),
        &size_analysis_ops,
    )
    .await;

    println!("=== {} analysis complete ===", kind.label());
}

/// Compiles and finalizes a workload's program (inject ids + reduce pushdown), returning the
/// optimized base `BuiltFlow` and its `CompiledProgram`.
fn build_workload<'a, W, F>(compile: &F, workload: &W) -> (BuiltFlow<'a>, CompiledProgram)
where
    F: Fn(&W) -> (FlowBuilder<'a>, CompiledProgram),
{
    let (builder, program) = compile(workload);
    let built = builder.finalize().optimize_with(|leaf| {
        inject_id(leaf);
        let decision = reduce_pushdown_decision(leaf, &program.exclude);
        reduce_pushdown(leaf, decision);
    });
    (built, program)
}

/// Applies the rewrites currently recorded in `state` to a fresh copy of `base_built`,
/// updating `program`'s clusters/loc-map with any new decoupled clusters. Returns `None` if
/// there are no prior rewrites to apply (so callers fall back to `base_built`).
fn apply_prior_rewrites<'a>(
    base_built: &BuiltFlow<'a>,
    state: &OptimizationState,
    program: &mut CompiledProgram,
) -> Option<BuiltFlow<'a>> {
    if state.applied.is_empty() {
        return None;
    }
    let rewrites = state.rewrites_to_apply();
    let mut builder = FlowBuilder::from_built(base_built);
    builder.replace_ir(deep_clone(base_built.ir()));
    Some(apply_loaded_rewrites(
        builder.finalize(),
        &mut program.clusters,
        &rewrites,
        &mut program.location_id_to_cluster,
    ))
}

/// Plans the next optimization iteration across all workloads and returns `(run_name, rewrites)`
/// for the caller to benchmark. Workloads share an identical IR (they differ only in runtime
/// data), so stats are op-id-keyed and merged with element-wise maxes across workloads.
/// Exits the process when optimization is complete (exhausted / no improvement).
///
/// Phases:
/// 1. First-ever run: no rewrites yet — return the base name and an empty rewrite set.
/// 2. Find the bottleneck across all workloads' latest `_none` runs (max saturation per cluster).
/// 3. If no cached rewrites: run perf/counters/size analyses for *each* workload (gated per
///    workload on disk), then compute the ILP ONCE on the max-merged stats.
/// 4. Apply the next budget once and return its rewrites.
async fn prepare_bottleneck_rewrites<'a, W, F>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    config: &BenchmarkConfig,
    workloads: &[(W, String)],
    compile: &F,
) -> (String, Vec<Rewrite>)
where
    F: Fn(&W) -> (FlowBuilder<'a>, CompiledProgram),
{
    assert!(
        !workloads.is_empty(),
        "No workloads provided for optimization"
    );

    let base = Path::new(CALIBRATION_DIR);
    let base_name = config.name.clone();
    let opt_base_name = match config.kind {
        Optimization::OptimizeWithLatencyBudget(budget) => format!("{}_lat{}", base_name, budget),
        _ => base_name.clone(),
    };
    let state_path = base.join(format!("{}_optimization_state.json", opt_base_name));
    let mut state = OptimizationState::load(&state_path);

    // === Phase 1: first-ever run — no rewrites yet; benchmark the base program. ===
    let Some(latest_iter) = find_latest_iteration(base, &base_name, &opt_base_name) else {
        return (base_name, Vec::new());
    };
    let iteration = latest_iter + 1;
    let prev_name = if latest_iter == 0 {
        base_name.clone()
    } else {
        format!("{}_opt{}", opt_base_name, latest_iter)
    };

    // === Phase 2: find the bottleneck across all workloads' latest `_none` runs. ===
    // Excluded ids depend on the compiled program, so build one workload to resolve names.
    // (Use `build_workload` rather than `compile` directly: an unfinalized `FlowBuilder`
    // panics on drop, so the builder must be finalized even though we discard the result.)
    let (_first_built, first_program) = build_workload(compile, &workloads[0].0);
    let excluded_names: HashSet<String> = first_program
        .exclude
        .iter()
        .filter_map(|loc| first_program.location_id_to_cluster.get(loc).cloned())
        .collect();
    let prev_run_dirs = find_workload_dirs(base, &prev_name, Optimization::None.label());
    assert!(
        !prev_run_dirs.is_empty(),
        "No _none run found for '{}'",
        prev_name
    );
    let bottleneck_name = find_bottleneck_across_runs(
        &prev_run_dirs,
        AWS_NETWORK_BYTES_PER_SEC,
        AWS_IO_TPS,
        &excluded_names,
    );
    let bottleneck_cluster = original_cluster_name(&bottleneck_name).to_string();

    if state.is_exhausted(&bottleneck_cluster) {
        println!(
            "Bottleneck '{}' exhausted all budgets. Stopping.",
            bottleneck_cluster
        );
        std::process::exit(0);
    }

    // === Phase 3: if no cached rewrites, analyze every workload then compute the ILP once. ===
    if !state.cluster_rewrites.contains_key(&bottleneck_cluster) {
        let analysis_name = if state.applied.is_empty() {
            base_name.clone()
        } else {
            format!("{}_opt{}", opt_base_name, iteration)
        };

        // Run perf/counters/size for each workload, gated on that workload's own dir so every
        // workload gets analyzed (not just the first).
        for (params, workload) in workloads {
            let (base_built, mut program) = build_workload(compile, params);
            let analysis_built = apply_prior_rewrites(&base_built, &state, &mut program);
            let run_built = analysis_built.as_ref().unwrap_or(&base_built);
            for kind in &[
                Optimization::Perf,
                Optimization::Counters,
                Optimization::SizeAnalysis,
            ] {
                // Skip if this workload's analysis already completed. Build the path directly —
                // do NOT use `make_output_dir`, which deletes and recreates the directory. Gate
                // on CSV presence so a previously-created-but-empty dir is re-run.
                let dir = Path::new(CALIBRATION_DIR).join(format!(
                    "{}_{}_{}",
                    analysis_name,
                    workload,
                    kind.label()
                ));
                if dir.is_dir() && !get_csvs_in_dir(&dir).is_empty() {
                    continue;
                }
                run_analysis_pass(
                    reusable_hosts,
                    deployment,
                    run_built,
                    config,
                    &mut program,
                    &analysis_name,
                    workload,
                    kind.clone(),
                )
                .await;
            }
        }

        // Barrier: every workload is analyzed. Merge stats (max across workloads) and solve once.
        let raw_ilp_inputs = load_raw_ilp_inputs(base, &analysis_name);

        // Canonical IR for the ILP: any workload's program (they share structure), with prior
        // rewrites applied so the op ids and locations match the analyzed program.
        let (canon_base, mut canon_program) = build_workload(compile, &workloads[0].0);
        let canon_built =
            apply_prior_rewrites(&canon_base, &state, &mut canon_program).unwrap_or(canon_base);
        let bottleneck_loc = canon_program
            .location_id_to_cluster
            .iter()
            .find(|(_, n)| *n == &bottleneck_name)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| panic!("Bottleneck cluster '{}' not found", bottleneck_name));
        let cluster_size = canon_program
            .clusters
            .location_name_and_num(&bottleneck_loc)
            .map(|(_, n)| n)
            .unwrap_or(1);

        let RawIlpInputs {
            per_op_blow_up,
            op_output_sizes,
            mut op_counts,
            perf,
            network_cost_table,
        } = raw_ilp_inputs;

        let mut ir = deep_clone(canon_built.ir());
        let network_op_ids = get_network_op_ids(&mut ir);
        let cycles = cycle_source_to_sink_parent(&mut ir);
        let op_parents = op_id_to_parents(&mut ir, Some(&bottleneck_loc), &cycles);
        inject_inferred_counters(&mut ir, &op_parents, &mut op_counts);

        let per_op_load = derive_per_op_load(&perf, &network_op_ids, &per_op_blow_up);
        let latency_budget = match config.kind {
            Optimization::OptimizeWithLatencyBudget(budget) => Some(budget),
            _ => None,
        };
        let ilp_inputs = IlpInputs {
            op_counts,
            op_output_sizes,
            network_cost_table,
            per_op_load,
            consider_partitioning: true,
            cluster_size,
            latency_budget,
        };

        let computed_rewrites = find_optimal_budget(&mut ir, &bottleneck_loc, &ilp_inputs, &cycles);
        state
            .cluster_rewrites
            .insert(bottleneck_cluster.clone(), computed_rewrites);
    }

    // === Phase 4: apply the next budget once and return its rewrites for the caller to run. ===
    let budget = state.next_budget(&bottleneck_cluster);
    println!(
        "Bottleneck '{}': applying budget {}.",
        bottleneck_cluster, budget
    );
    state.applied.insert(bottleneck_cluster, budget);
    state.save(&state_path);

    let opt_name = format!("{}_opt{}", opt_base_name, iteration);
    (opt_name, state.rewrites_to_apply())
}

async fn run_exhaustive_search<'a, W, F>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    config: &BenchmarkConfig,
    workloads: &[(W, String)],
    compile: &F,
) -> (Vec<HydroRoot>, RunMetadata)
where
    F: Fn(&W) -> (FlowBuilder<'a>, CompiledProgram),
{
    let base = Path::new(CALIBRATION_DIR);
    let optimizations_path = base.join(format!("{}_exhaustive_optimizations.json", config.name));
    assert!(
        optimizations_path.exists(),
        "Exhaustive search file not found: {}",
        optimizations_path.display()
    );

    let budget = match config.kind {
        Optimization::ExhaustiveSearch(budget) => budget,
        _ => unreachable!(),
    };
    let candidates = load_json::<ExhaustiveOptimizations>(&optimizations_path)
        .into_candidates_for_budget(budget);
    let state_path = base.join(format!(
        "{}_exhaustive_budget{}_state.json",
        config.name, budget
    ));
    let mut state = ExhaustiveSearchState::load(&state_path);
    let mut last = (Vec::new(), RunMetadata::default());

    while state.next_index < candidates.len() {
        let candidate = candidates[state.next_index].clone();
        let run_name = format!("{}_{}", config.name, candidate.name);
        println!(
            "=== Running exhaustive candidate {}/{}: {} ===",
            state.next_index + 1,
            candidates.len(),
            candidate.name
        );

        for (params, workload) in workloads {
            let (base_built, mut program) = build_workload(compile, params);
            let built = apply_loaded_rewrites(
                base_built,
                &mut program.clusters,
                std::slice::from_ref(&candidate.rewrite),
                &mut program.location_id_to_cluster,
            );
            let meta = run_scaling_loop(
                reusable_hosts,
                deployment,
                &built,
                config,
                &program,
                &run_name,
                workload,
                &config.kind,
                &HashMap::new(),
                None,
                &HashSet::new(),
            )
            .await;
            last = (deep_clone(built.ir()), meta);
        }

        state.executed.push(candidate.name);
        state.next_index += 1;
        state.save(&state_path);
    }

    println!("=== Exhaustive search complete ===");
    last
}

/// Runs the benchmark/optimization flow over one or more workloads. Each workload is built by
/// `compile(&params)` and must produce an identical IR structure (op-id topology), differing
/// only in runtime data (e.g. write ratio), since stats are merged across workloads by op id.
/// Each workload's name (the second tuple element) must be a unique, stable label — it
/// discriminates output directories on disk.
pub async fn benchmark_protocol<'a, W>(
    args: BenchmarkArgs,
    config: BenchmarkConfig,
    workloads: &[(W, String)],
    compile: impl Fn(&W) -> (FlowBuilder<'a>, CompiledProgram),
) -> (Vec<HydroRoot>, RunMetadata) {
    assert!(!workloads.is_empty(), "Must provide at least one workload");
    assert!(
        config.num_runs > 0,
        "Must run at least one iteration of the benchmark"
    );
    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp.clone() {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };
    let mut reusable_hosts = ReusableHosts::new(&host_type);

    if config.calibrate_message_sizes.is_some() {
        reusable_hosts.set_single_host(true);
    }

    if matches!(config.kind, Optimization::ExhaustiveSearch(_)) {
        return run_exhaustive_search(
            &mut reusable_hosts,
            &mut deployment,
            &config,
            workloads,
            &compile,
        )
        .await;
    }

    // Decide what to benchmark: bottleneck elimination first runs the analyses + ILP and returns
    // the rewrites + run name; every other mode benchmarks the base program (no rewrites).
    let (run_name, rewrites) = match config.kind {
        Optimization::BottleneckElimination | Optimization::OptimizeWithLatencyBudget(_) => {
            prepare_bottleneck_rewrites(
                &mut reusable_hosts,
                &mut deployment,
                &config,
                workloads,
                &compile,
            )
            .await
        }
        _ => (config.name.clone(), Vec::new()),
    };

    // Benchmark the (possibly rewritten) program for each workload, with optional calibration.
    let mut last = (Vec::new(), RunMetadata::default());
    for (params, workload) in workloads {
        let (base_built, mut program) = build_workload(&compile, params);
        let built = apply_loaded_rewrites(
            base_built,
            &mut program.clusters,
            &rewrites,
            &mut program.location_id_to_cluster,
        );
        // One deploy per calibration size (under a `{run_name}_{size}b` name), or a single
        // deploy under `run_name` when not calibrating.
        let scaling_runs: Vec<(String, Option<usize>)> = match &config.calibrate_message_sizes {
            Some(sizes) => sizes
                .iter()
                .map(|&size| (format!("{}_{}b", run_name, size), Some(size)))
                .collect(),
            None => vec![(run_name.clone(), None)],
        };
        let mut meta = RunMetadata::default();
        for (scaling_name, calibrate_size) in &scaling_runs {
            if let Some(size) = calibrate_size {
                reusable_hosts.insert_env(HYDRO_NET_CALIBRATE_ENV.to_string(), size.to_string());
            }
            meta = run_scaling_loop(
                &mut reusable_hosts,
                &mut deployment,
                &built,
                &config,
                &program,
                scaling_name,
                workload,
                &config.kind,
                &HashMap::new(),
                None,
                &HashSet::new(),
            )
            .await;
        }
        last = (deep_clone(built.ir()), meta);
    }
    last
}
