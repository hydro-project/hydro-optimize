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

use crate::decouple_analysis::{IlpInputs, Rewrite, find_optimal_budget, project_total_cpu};
use crate::deploy::{AWS_IO_TPS, AWS_NETWORK_BYTES_PER_SEC, HostType, ReusableHosts};
use crate::greedy_decouple_analysis::greedy_decouple_analysis;
use crate::parse_results::{
    OptimizationState, RawIlpInputs, RunMetadata, analyze_cluster_results, decoupled_cluster_name,
    derive_per_op_load, find_bottleneck_from_run, find_latest_iteration, find_workload_dirs,
    load_raw_ilp_inputs, max_throughput_for, original_cluster_name,
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

pub const MAX_BUDGET_PER_CLUSTER: usize = 7;
pub const MAX_OPTIMIZATION_ITERATIONS_PAST_NO_IMPROVEMENT: usize = 2;
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
pub enum OptimizationKind {
    /// No IR rewriting, no counters.
    #[default]
    None,
    /// No IR rewriting, but insert counters to measure per-op cardinality.
    CountersOnly,
    /// Apply greedy decoupling, deploy decoupled system, gather per-operator SAR costs.
    /// Always inserts counters.
    BlowUpAnalysis,
    /// Insert size measuring nodes wherever decoupling is possible.
    SizeAnalysis,
    /// Deploy with `perf record` on non-excluded locations for flamegraph profiling.
    PerfOnly,
    /// Run ILP to find optimal decoupling, save Rewrite to file, then exit without deploying.
    /// Automatically loads applied rewrites from the latest opt dir.
    BottleneckElimination,
}

impl OptimizationKind {
    pub fn label(&self) -> &'static str {
        match self {
            // Do not differentiate between None and BottleneckElimination, since both run the protocol without additional profiling
            OptimizationKind::None | OptimizationKind::BottleneckElimination => "none",
            OptimizationKind::CountersOnly => "counters",
            OptimizationKind::BlowUpAnalysis => "blow_up",
            OptimizationKind::SizeAnalysis => "size",
            OptimizationKind::PerfOnly => "perf",
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

#[derive(Clone, Default)]
pub struct Optimizations {
    pub decoupling: bool,
    pub partitioning: bool,
    pub kind: OptimizationKind,
    pub exclude: HashSet<LocationId>,
}

impl Optimizations {
    pub fn with_decoupling(mut self) -> Self {
        self.decoupling = true;
        self
    }

    pub fn with_partitioning(mut self) -> Self {
        self.partitioning = true;
        self
    }

    /// Insert counters only (no IR rewriting).
    pub fn with_counters_only(mut self) -> Self {
        self.set_kind(OptimizationKind::CountersOnly);
        self
    }

    /// Insert size measuring nodes wherever decoupling is possible
    pub fn with_size_analysis(mut self) -> Self {
        self.set_kind(OptimizationKind::SizeAnalysis);
        self
    }

    /// Apply greedy decoupling, deploy decoupled system, gather per-operator SAR costs
    pub fn with_blow_up_analysis(mut self) -> Self {
        self.set_kind(OptimizationKind::BlowUpAnalysis);
        self
    }

    /// Deploy with `perf record` on non-excluded locations for flamegraph profiling.
    pub fn with_perf_only(mut self) -> Self {
        self.set_kind(OptimizationKind::PerfOnly);
        self
    }

    /// Run ILP-based bottleneck elimination (offline, no deployment).
    /// Automatically finds the latest run directory for bottleneck detection
    /// and loads applied rewrites from the latest opt dir.
    pub fn with_bottleneck_elimination(mut self) -> Self {
        self.set_kind(OptimizationKind::BottleneckElimination);
        self
    }

    pub fn excluding(mut self, location: LocationId) -> Self {
        self.exclude.insert(location);
        self
    }

    fn set_kind(&mut self, kind: OptimizationKind) {
        assert!(
            matches!(self.kind, OptimizationKind::None),
            "blow_up_analysis, size_analysis, and loaded rewrites are mutually exclusive"
        );
        self.kind = kind;
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
    clusters: &ReusableClusters,
    processes: &ReusableProcesses,
    optimizations: &Optimizations,
    num_seconds: Option<usize>,
) -> RunMetadata {
    // Measure network (-n DEV) and CPU (-u) usage. -P 0 measures only core 0, where the
    // hydro main thread is pinned (networking threads spin on other cores, so whole-machine
    // CPU is misleading).
    let sar_sidecar = ScriptSidecar {
        script: "sar -n DEV -u -P 0 -r -b 1".to_string(),
        prefix: SAR_USAGE_PREFIX.to_string(),
    };

    let use_perf = matches!(optimizations.kind, OptimizationKind::PerfOnly);

    // Insert all clusters & processes
    let mut deployable = builder.into_deploy();
    for (cluster_id, name, num_hosts) in clusters.named_clusters.iter() {
        let excluded = optimizations.exclude.contains(cluster_id);
        let hosts = reusable_hosts.get_cluster_hosts(
            deployment,
            name.clone(),
            *num_hosts,
            use_perf && !excluded,
        );
        deployable = deployable.with_cluster_erased(cluster_id.key(), hosts);
    }
    for (process_id, name) in processes.named_processes.iter() {
        let excluded = optimizations.exclude.contains(process_id);
        let host = reusable_hosts.get_process_host(deployment, name.clone(), use_perf && !excluded);
        deployable = deployable.with_process_erased(process_id.key(), host);
    }
    for excluded_location in optimizations.exclude.iter() {
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

#[derive(Clone)]
pub struct BenchmarkConfig {
    pub name: String,
    /// Workload variant (e.g. "read_heavy"). Determines output subdirectory name.
    pub workload: String,
    pub clusters: ReusableClusters,
    pub processes: ReusableProcesses,
    pub optimizations: Optimizations,
    pub location_id_to_cluster: HashMap<LocationId, String>,
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
    optimizations: &Optimizations,
    location_to_original_ops: &HashMap<LocationId, Vec<usize>>,
    pre_rewrite_parents: Option<&HashMap<usize, Vec<usize>>>,
    size_analysis_ops: &HashSet<usize>,
) -> RunMetadata {
    let output_dir = make_output_dir(&config.name, &config.workload, optimizations.kind.label());
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
                config.name,
                optimizations.kind.label(),
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
                &config.clusters,
                &config.processes,
                optimizations,
                Some(RUN_SECONDS),
            )
            .await;
            run_metadata.location_to_original_ops = location_to_original_ops.clone();

            let run_throughput = run_metadata.avg_throughput();
            println!(
                "Run throughput: {}, num_clients: {}",
                run_throughput, num_virtual
            );
            run_metadata.print_run_summary(&config.location_id_to_cluster, MEASUREMENT_SECOND);
            run_metadata.save_run_metadata(
                &config.location_id_to_cluster,
                &output_dir,
                config.num_physical_clients,
                num_virtual,
                run,
            );

            if matches!(optimizations.kind, OptimizationKind::BlowUpAnalysis) {
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
            if matches!(optimizations.kind, OptimizationKind::CountersOnly) {
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

            if matches!(optimizations.kind, OptimizationKind::SizeAnalysis)
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

        num_virtual += config.virtual_clients_step;
    }

    final_run_metadata
}

/// Runs a single analysis pass (counters, size, blow_up, or perf) on the given built flow.
/// Deploys with the appropriate instrumentation, scales virtual clients, and saves results.
async fn run_analysis_pass<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    built: &BuiltFlow<'a>,
    config: &mut BenchmarkConfig,
    kind: OptimizationKind,
) {
    println!("=== Running {} analysis ===", kind.label());

    let mut size_analysis_ops: HashSet<usize> = HashSet::new();
    let mut pre_rewrite_parents: Option<HashMap<usize, Vec<usize>>> = None;
    let mut location_to_original_ops: HashMap<LocationId, Vec<usize>> = HashMap::new();

    let mut builder = FlowBuilder::from_built(built);
    builder.replace_ir(deep_clone(built.ir()));
    let analysis_built = builder.finalize();

    let analysis_built = match &kind {
        OptimizationKind::CountersOnly => {
            insert_counters(analysis_built, &config.optimizations.exclude)
        }
        OptimizationKind::BlowUpAnalysis => {
            let (built, loc_ops, parents) = apply_blow_up_analysis(
                analysis_built,
                &mut config.clusters,
                &config.optimizations.exclude,
                &mut config.location_id_to_cluster,
            );
            pre_rewrite_parents = Some(parents);
            location_to_original_ops = loc_ops;
            built
        }
        OptimizationKind::SizeAnalysis => {
            let mut captured_ops = HashSet::new();
            let built = analysis_built.optimize_with(|leaf| {
                let per_loc_rewrites =
                    greedy_decouple_analysis(leaf, &config.optimizations.exclude, &config.clusters);
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
        OptimizationKind::PerfOnly | OptimizationKind::None => analysis_built,
        OptimizationKind::BottleneckElimination => unreachable!(),
    };

    let optimizations = Optimizations {
        kind: kind.clone(),
        exclude: config.optimizations.exclude.clone(),
        ..Default::default()
    };

    run_scaling_loop(
        reusable_hosts,
        deployment,
        &analysis_built,
        config,
        &optimizations,
        &location_to_original_ops,
        pre_rewrite_parents.as_ref(),
        &size_analysis_ops,
    )
    .await;

    println!("=== {} analysis complete ===", kind.label());
}

/// Runs the ILP-based bottleneck elimination workflow.
///
/// 1. Apply prior rewrites from state
/// 2. Find bottleneck from latest `_none` run
/// 3. If no cached ILP rewrites for bottleneck → run analyses, compute ILP
/// 4. Apply next budget for bottleneck cluster
async fn run_bottleneck_elimination<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    built: BuiltFlow<'a>,
    config: &mut BenchmarkConfig,
) -> BuiltFlow<'a> {
    let base = Path::new(CALIBRATION_DIR);
    let base_name = config.name.clone();
    let state_path = base.join(format!("{}_optimization_state.json", base_name));
    let mut state = OptimizationState::load(&state_path);

    // Check if a previous _none run exists
    let Some(latest_iter) = find_latest_iteration(base, &base_name) else {
        return built; // First ever run — just benchmark the base
    };
    let iteration = latest_iter + 1;
    let prev_name = if latest_iter == 0 {
        base_name.clone()
    } else {
        format!("{}_opt{}", base_name, latest_iter)
    };

    // Early exit if last optimization didn't improve throughput
    if latest_iter >= 2 {
        let thr_prev = max_throughput_for(base, &prev_name);
        let thr_prev_prev =
            max_throughput_for(base, &format!("{}_opt{}", base_name, latest_iter - 1));
        assert!(
            thr_prev_prev > 0 && thr_prev > 0,
            "Previous run(s) had zero throughput, cannot compare for improvement"
        );
        if thr_prev <= thr_prev_prev {
            println!(
                "No improvement from opt{} ({}) to opt{} ({}). Stopping.",
                latest_iter - 1,
                thr_prev_prev,
                latest_iter,
                thr_prev
            );
            std::process::exit(0);
        }
    }

    // Find bottleneck from the most recent _none run
    let prev_run_dir = find_workload_dirs(base, &prev_name, OptimizationKind::None.label())
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("No _none run found for '{}'", prev_name));
    let bottleneck_name =
        find_bottleneck_from_run(&prev_run_dir, AWS_NETWORK_BYTES_PER_SEC, AWS_IO_TPS);
    let bottleneck_cluster = original_cluster_name(&bottleneck_name).to_string();

    if state.is_exhausted(&bottleneck_cluster) {
        println!(
            "Bottleneck '{}' exhausted all budgets. Stopping.",
            bottleneck_cluster
        );
        std::process::exit(0);
    }

    // If no cached rewrites for this cluster, run analyses and compute ILP
    if !state.cluster_rewrites.contains_key(&bottleneck_cluster) {
        let analysis_name;
        let analysis_built;
        if state.applied.is_empty() {
            analysis_name = base_name.clone();
            analysis_built = None;
        } else {
            let name = format!("{}_opt{}", base_name, iteration);
            config.name = name.clone();
            analysis_name = name;

            let rewrites = state.rewrites_to_apply();
            let mut analysis_clusters = config.clusters.clone();
            let mut analysis_loc_map = config.location_id_to_cluster.clone();
            let mut builder = FlowBuilder::from_built(&built);
            builder.replace_ir(deep_clone(built.ir()));
            analysis_built = Some(apply_loaded_rewrites(
                builder.finalize(),
                &mut analysis_clusters,
                &rewrites,
                &mut analysis_loc_map,
            ));
        }

        for kind in &[
            OptimizationKind::PerfOnly,
            OptimizationKind::CountersOnly,
            OptimizationKind::SizeAnalysis,
            // OptimizationKind::BlowUpAnalysis,
            // TODO: Reenable blow up analysis once vcpu limit has been addressed
        ] {
            if find_workload_dirs(base, &analysis_name, kind.label()).is_empty() {
                let run_built = analysis_built.as_ref().unwrap_or(&built);
                run_analysis_pass(reusable_hosts, deployment, run_built, config, kind.clone())
                    .await;
            }
        }

        let raw_ilp_inputs = load_raw_ilp_inputs(base, &analysis_name);
        let bottleneck_loc = config
            .location_id_to_cluster
            .iter()
            .find(|(_, n)| *n == &bottleneck_name)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| panic!("Bottleneck cluster '{}' not found", bottleneck_name));
        let cluster_size = config
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

        let mut ir = if let Some(ref ab) = analysis_built {
            deep_clone(ab.ir())
        } else {
            deep_clone(built.ir())
        };
        let network_op_ids = get_network_op_ids(&mut ir);
        let cycles = cycle_source_to_sink_parent(&mut ir);
        let op_parents = op_id_to_parents(&mut ir, Some(&bottleneck_loc), &cycles);
        inject_inferred_counters(&mut ir, &op_parents, &mut op_counts);

        let per_op_load = derive_per_op_load(&perf, &network_op_ids, &per_op_blow_up);
        let ilp_inputs = IlpInputs {
            op_counts,
            op_output_sizes,
            network_cost_table,
            per_op_load,
            consider_partitioning: true,
            cluster_size,
        };

        let computed_rewrites = find_optimal_budget(&mut ir, &bottleneck_loc, &ilp_inputs, &cycles);
        state
            .cluster_rewrites
            .insert(bottleneck_cluster.clone(), computed_rewrites);
    }

    // Apply next budget for the bottleneck cluster
    let budget = state.next_budget(&bottleneck_cluster);
    println!(
        "Bottleneck '{}': applying budget {}.",
        bottleneck_cluster, budget
    );
    state.applied.insert(bottleneck_cluster, budget);
    let built = apply_loaded_rewrites(
        built,
        &mut config.clusters,
        &state.rewrites_to_apply(),
        &mut config.location_id_to_cluster,
    );

    state.save(&state_path);
    config.name = format!("{}_opt{}", base_name, iteration);
    built
}

pub async fn benchmark_protocol<'a>(
    args: BenchmarkArgs,
    run_benchmark: impl Fn() -> (FlowBuilder<'a>, BenchmarkConfig),
) -> (Vec<HydroRoot>, RunMetadata) {
    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp.clone() {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };
    let mut reusable_hosts = ReusableHosts::new(&host_type);

    let (builder, mut config) = run_benchmark();
    assert!(
        config.num_runs > 0,
        "Must run at least one iteration of the benchmark"
    );

    if config.calibrate_message_sizes.is_some() {
        reusable_hosts.set_single_host(true);
    }

    // Apply the selected optimization strategy exactly once.
    let exclude = config.optimizations.exclude.clone();
    let built = builder.finalize().optimize_with(|leaf| {
        inject_id(leaf);
        let decision = reduce_pushdown_decision(leaf, &exclude);
        reduce_pushdown(leaf, decision);
    });

    let built = match &config.optimizations.kind {
        OptimizationKind::None => built,
        OptimizationKind::BottleneckElimination => {
            run_bottleneck_elimination(&mut reusable_hosts, &mut deployment, built, &mut config)
                .await
        }
        _ => {
            println!("Warning: Explicit optimization kinds are no longer allowed as parameters");
            built
        }
    };

    // Run a loop for each network calibration size, if provided
    let final_run_metadata = if let Some(ref sizes) = config.calibrate_message_sizes {
        let mut last_metadata = RunMetadata::default();
        for &size in sizes {
            reusable_hosts.insert_env(HYDRO_NET_CALIBRATE_ENV.to_string(), size.to_string());
            let mut iter_config = config.clone();
            iter_config.name = format!("{}_{}b", config.name, size);
            last_metadata = run_scaling_loop(
                &mut reusable_hosts,
                &mut deployment,
                &built,
                &iter_config,
                &config.optimizations,
                &HashMap::new(),
                None,
                &HashSet::new(),
            )
            .await;
        }
        last_metadata
    } else {
        run_scaling_loop(
            &mut reusable_hosts,
            &mut deployment,
            &built,
            &config,
            &config.optimizations,
            &HashMap::new(),
            None,
            &HashSet::new(),
        )
        .await
    };

    (deep_clone(built.ir()), final_run_metadata)
}
