use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Local;
use hydro_deploy::Deployment;
use hydro_lang::compile::built::BuiltFlow;
use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::compile::ir::{HydroNode, HydroRoot, deep_clone, traverse_dfir};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::deploy::deploy_graph::DeployCrateWrapper;
use hydro_lang::location::Location;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::prelude::{Cluster, FlowBuilder, Process};
use hydro_lang::telemetry::Sidecar;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::decouple_analysis::decouple_analysis;
use crate::decoupler::decouple;
use crate::deploy::{HostType, ReusableHosts};
use crate::parse_results::{RunMetadata, analyze_cluster_results};
use crate::repair::{cycle_source_to_sink_input, inject_id, remove_counter};
use crate::rewrites::Rewrite;

/// Special line printed by a scenario subprocess after it finishes its measurement phase.
pub const SCENARIO_FINISHED_PREFIX: &str = "HYDRO_OPTIMIZE_SCENARIO_FINISHED:";

pub(crate) const METRIC_INTERVAL_SECS: u64 = 1;
const COUNTER_PREFIX: &str = "_optimize_counter";
pub(crate) const CPU_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_CPU:";
pub(crate) const SAR_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_SAR:";
pub(crate) const LATENCY_PREFIX: &str = "HYDRO_OPTIMIZE_LAT:";
pub(crate) const THROUGHPUT_PREFIX: &str = "HYDRO_OPTIMIZE_THR:";

// Note: Ensure edits to the match arms are consistent with inject_count_node
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
        | HydroNode::Partition { metadata, .. }
         => {
            if exclude.contains(metadata.location_id.root()) {
                return;
            }

            let metadata = metadata.clone();
            let node_content = std::mem::replace(node, HydroNode::Placeholder);

            let counter = HydroNode::Counter {
                tag: next_stmt_id.to_string(),
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

fn insert_counter(ir: &mut [HydroRoot], duration: &syn::Expr, exclude: &HashSet<LocationId>) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            insert_counter_node(node, next_stmt_id, duration.clone(), exclude);
        },
    );
}

pub struct MetricLogs {
    pub throughputs: UnboundedReceiver<String>,
    pub latencies: UnboundedReceiver<String>,
    pub cpu: UnboundedReceiver<String>,
    pub sar: UnboundedReceiver<String>,
    pub counters: UnboundedReceiver<String>,
}

async fn track_process_metrics(process: &impl DeployCrateWrapper) -> MetricLogs {
    MetricLogs {
        throughputs: process.stdout_filter(THROUGHPUT_PREFIX),
        latencies: process.stdout_filter(LATENCY_PREFIX),
        cpu: process.stdout_filter(CPU_USAGE_PREFIX),
        sar: process.stdout_filter(SAR_USAGE_PREFIX),
        counters: process.stdout_filter(COUNTER_PREFIX),
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

struct ScriptSidecar {
    script: String,
    prefix: String,
}

impl Sidecar for ScriptSidecar {
    fn to_expr(
        &self,
        _flow_name: &str,
        location_key: hydro_lang::location::LocationKey,
        _location_type: hydro_lang::location::LocationType,
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
    named_clusters: Vec<(LocationId, String, usize)>,
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

    fn location_name_and_num(&self, location: &LocationId) -> Option<(String, usize)> {
        self.named_clusters
            .iter()
            .find(|(id, _, _)| location.key() == id.key())
            .map(|(_, name, count)| (name.clone(), *count))
    }
}

#[derive(Default, Clone)]
pub struct ReusableProcesses {
    named_processes: Vec<(LocationId, String)>,
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

    fn location_name(&self, location: &LocationId) -> Option<String> {
        self.named_processes
            .iter()
            .find(|(id, _)| location.key() == id.key())
            .map(|(_, name)| name.clone())
    }
}

#[derive(Clone)]
pub struct Optimizations {
    decoupling: bool,
    partitioning: bool,
    pub exclude: HashSet<LocationId>,
    iterations: usize, // Must be at least 1
}

impl Default for Optimizations {
    fn default() -> Self {
        Self {
            decoupling: false,
            partitioning: false,
            exclude: HashSet::new(),
            iterations: 1,
        }
    }
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

    pub fn excluding(mut self, location: LocationId) -> Self {
        self.exclude.insert(location);
        self
    }

    pub fn with_iterations(mut self, iterations: usize) -> Self {
        assert!(iterations > 0);
        self.iterations = iterations;
        self
    }
}

/// Persistent per-bottleneck state that lets successive apply_optimizations calls
/// remember which clusters have already been marked partitionable and how many
/// partitions they should now use.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct BottleneckState {
    /// Maps a bottleneck name to its current partition count (>= 2 once partitioned).
    pub partitions: HashMap<String, usize>,
}

impl BottleneckState {
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(s) => serde_json::from_str(&s).expect("failed to deserialize BottleneckState"),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self, path: &Path) {
        let json = serde_json::to_string_pretty(self).unwrap();
        std::fs::write(path, json).expect("failed to write BottleneckState");
    }
}

/// Result of `apply_optimizations` for a single bottleneck.
pub enum ApplyResult {
    /// A new decoupling rewrite was produced.
    Decoupled(Rewrite),
    /// The bottleneck is (re)partitioned to `num_partitions` total partitions.
    Partitioned(Rewrite),
    /// Neither decoupling nor partitioning is possible/useful. Caller should skip.
    Skip,
}

/// `stability_second`: The second in which the protocol is expected to be stable, and its performance can be used as the basis for optimization.
#[allow(clippy::too_many_arguments)]
pub async fn deploy_and_optimize<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    mut builder: BuiltFlow<'a>,
    mut clusters: ReusableClusters,
    processes: ReusableProcesses,
    optimizations: Optimizations,
    client_id: &LocationId,
    num_clients_per_node: usize,
    num_seconds: Option<usize>,
    stability_second: Option<usize>,
    analyze: bool,
) -> RunMetadata {
    assert!(
        optimizations.iterations < 1 || num_seconds.is_some(),
        "Cannot specify multiple iterations without bounding run time"
    );
    if let Some(num_seconds) = num_seconds {
        assert!(
            stability_second.is_some_and(|stability_time| stability_time < num_seconds),
            "Invariant: stability_second < num_seconds"
        );
    }
    if optimizations.decoupling || optimizations.partitioning {
        assert!(
            stability_second.is_some() && analyze,
            "Must select stability_second and enable analysis with optimizations"
        );
    }
    assert!(
        num_clients_per_node > 0,
        "Must have at least 1 client per node"
    );

    let counter_output_duration =
        syn::parse_quote!(std::time::Duration::from_secs(#METRIC_INTERVAL_SECS));
    let mut run_metadata = RunMetadata::default();
    // Measure network (-n DEV) and CPU (-u) usage. -P ALL measures all CPUs on the machine
    let sar_sidecar = ScriptSidecar {
        script: "sar -n DEV -u -P ALL -r -b 1".to_string(),
        prefix: SAR_USAGE_PREFIX.to_string(),
    };

    for iteration in 0..optimizations.iterations {
        // Rewrite with counter tracking
        let mut post_rewrite_builder = FlowBuilder::from_built(&builder);
        let optimized = builder.optimize_with(|leaf| {
            inject_id(leaf);
            insert_counter(leaf, &counter_output_duration, &optimizations.exclude);
        });
        let mut ir = deep_clone(optimized.ir());

        // Insert all clusters & processes
        let mut deployable = optimized.into_deploy();
        for (cluster_id, name, num_hosts) in clusters.named_clusters.iter() {
            let excluded = optimizations.exclude.contains(cluster_id);
            if cluster_id.key() == client_id.key() {
                let mut client_hosts = vec![];
                for i in 0..num_clients_per_node {
                    let pin_to_core = i % reusable_hosts.num_cores();
                    client_hosts.push(reusable_hosts.get_cluster_hosts(
                        deployment,
                        name.clone(),
                        *num_hosts,
                        pin_to_core,
                        false,
                    ));
                }
                deployable =
                    deployable.with_cluster_erased(cluster_id.key(), client_hosts.concat());
            } else {
                deployable = deployable.with_cluster_erased(
                    cluster_id.key(),
                    reusable_hosts.get_cluster_hosts(
                        deployment,
                        name.clone(),
                        *num_hosts,
                        0,
                        !excluded,
                    ),
                );
                if !excluded {
                    deployable = deployable.with_sidecar_internal(cluster_id.key(), &sar_sidecar);
                }
            }
        }
        for (process_id, name) in processes.named_processes.iter() {
            let excluded = optimizations.exclude.contains(process_id);
            let host = if excluded {
                reusable_hosts.get_no_perf_process_hosts(deployment, name.clone(), 0)
            } else {
                reusable_hosts.get_process_hosts(deployment, name.clone(), 0)
            };
            deployable = deployable.with_process_erased(process_id.key(), host);
            if !excluded {
                deployable = deployable.with_sidecar_internal(process_id.key(), &sar_sidecar);
            }
        }

        let nodes = deployable.deploy(deployment);
        deployment.deploy().await.unwrap();
        let metrics = track_cluster_metrics(&nodes).await;

        // Wait for user to input a newline
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

        if analyze {
            // Parse results to get metrics
            run_metadata = analyze_cluster_results(&nodes, &mut ir, metrics, &optimizations).await;
            // Remove HydroNode::Counter (since we don't want to consider decoupling those)
            remove_counter(&mut ir);

            if optimizations.decoupling {
                // Preserve historical behavior: auto-optimize the first non-excluded cluster.
                let bottleneck = clusters
                    .named_clusters
                    .iter()
                    .map(|(id, _, _)| id)
                    .find(|id| !optimizations.exclude.contains(id))
                    .cloned()
                    .expect("No bottleneck cluster available");
                let mut state = BottleneckState::default();
                apply_optimizations(
                    &mut ir,
                    &bottleneck,
                    &mut clusters,
                    &processes,
                    &mut post_rewrite_builder,
                    &mut state,
                    iteration,
                );
            }
        }

        post_rewrite_builder.replace_ir(ir);
        builder = post_rewrite_builder.finalize();
    }

    run_metadata
}

/// Applies an optimization for the given `bottleneck` location.
///
/// Returns the rewrite that was produced (to be saved/replayed), or `Skip` if the scenario
/// should not be explored further (no new decoupling AND the cluster is not partitionable).
pub fn apply_optimizations(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    clusters: &mut ReusableClusters,
    processes: &ReusableProcesses,
    post_rewrite_builder: &mut FlowBuilder<'_>,
    state: &mut BottleneckState,
    iteration: usize,
) -> ApplyResult {
    let cycle_source_to_sink = cycle_source_to_sink_input(ir);

    let (bottleneck_name, bottleneck_num_nodes) = processes
        .location_name(bottleneck)
        .map(|name| (name, 1))
        .unwrap_or_else(|| clusters.location_name_and_num(bottleneck).unwrap());

    // Run decouple_analysis with partitioning considered. The result encodes both
    // the decoupling placement and per-location partitionability + field choices.
    let possible_rewrite =
        decouple_analysis(ir, bottleneck, 0.01, 0.01, 2, &cycle_source_to_sink, true);

    // Determine partition count: if already partitioned, double; otherwise use 2 if any
    // location is partitionable; otherwise 0.
    let any_partitionable = !possible_rewrite.stateless_partitionable.is_empty()
        || !possible_rewrite.field_partitionable.is_empty();
    let num_partitions = if let Some(count) = state.partitions.get(&bottleneck_name).copied() {
        let new_count = count * 2;
        state.partitions.insert(bottleneck_name.clone(), new_count);
        new_count
    } else if any_partitionable {
        state.partitions.insert(bottleneck_name.clone(), 2);
        2
    } else {
        0
    };

    let has_decoupling = possible_rewrite.num_locations() > 1;
    if !has_decoupling && num_partitions == 0 {
        return ApplyResult::Skip;
    }

    // Build locations_map: index 0 = bottleneck; create fresh clusters for other indices.
    let mut locations_map = HashMap::new();
    locations_map.insert(0, bottleneck.clone());
    for loc_idx in possible_rewrite.locations() {
        if loc_idx == 0 {
            continue;
        }
        let cluster = post_rewrite_builder.cluster::<()>();
        locations_map.insert(loc_idx, cluster.id().clone());
        clusters.named_clusters.push((
            cluster.id().clone(),
            format!("{}-decouple-{}", bottleneck_name, iteration),
            bottleneck_num_nodes,
        ));
    }

    let rewrite = Rewrite {
        possible_rewrite,
        num_partitions,
        original_node: bottleneck.clone(),
        cluster_size: bottleneck_num_nodes,
    };
    decouple(ir, &rewrite, &locations_map);

    if has_decoupling {
        ApplyResult::Decoupled(rewrite)
    } else {
        ApplyResult::Partitioned(rewrite)
    }
}

pub struct BenchmarkArgs {
    pub gcp: Option<String>,
    pub aws: bool,
}

pub struct BenchmarkConfig<'a> {
    pub name: String,
    pub builder: FlowBuilder<'a>,
    pub clusters: ReusableClusters,
    pub processes: ReusableProcesses,
    pub client_id: LocationId,
    pub optimizations: Optimizations,
    pub location_id_to_cluster: HashMap<LocationId, String>,
    /// Name of a cargo example (in `hydro_optimize_examples`) that can rerun this benchmark
    /// after applying one new optimization for a specific bottleneck. When set, after the
    /// normal benchmark sweep `benchmark_protocol` will iteratively launch scenarios (one
    /// subprocess per non-excluded bottleneck) and converge on the highest-throughput config.
    pub scenario_binary: Option<String>,
}

pub const START_MEASUREMENT_SECOND: usize = 30;
pub const MEASUREMENT_SECOND: usize = 59;
pub const RUN_SECONDS: usize = 90;
pub const PHYSICAL_CLIENTS: usize = 10;
pub const VIRTUAL_CLIENTS_MAX: usize = 50 * PHYSICAL_CLIENTS; // Based on manual testing, an 8-core m5.2xlarge's CPU saturates around 50 clients.
pub const VIRTUAL_CLIENTS_STEP: usize = 50; // Can tweak to get finer-grained numbers
pub const NUM_RUNS_NO_THROUGHPUT: usize = 3;

pub async fn benchmark_protocol<'a>(
    args: BenchmarkArgs,
    num_runs: usize,
    run_benchmark: fn(
        usize, // PHYSICAL_CLIENTS
    ) -> BenchmarkConfig<'a>,
) {
    assert!(
        num_runs > 0,
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

    let benchmark_config = run_benchmark(PHYSICAL_CLIENTS);
    let config_name = benchmark_config.name.clone();
    let config_clusters = benchmark_config.clusters.clone();
    let config_processes = benchmark_config.processes.clone();
    let config_optimizations = benchmark_config.optimizations.clone();
    let config_client_id = benchmark_config.client_id.clone();
    let config_location_id_to_cluster = benchmark_config.location_id_to_cluster.clone();
    let scenario_binary = benchmark_config.scenario_binary.clone();
    let config_exclude = benchmark_config.optimizations.exclude.clone();
    // Set up FlowBuilder for cloning
    let built = benchmark_config.builder.finalize();
    let ir = built.ir();
    let output_dir = Path::new("benchmark_results").join(format!(
        "{}_{}",
        config_name,
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));

    let mut best_profiling: Option<PathBuf> = None;
    let mut best_throughput: usize = 0;

    for num_virtual in (1..=VIRTUAL_CLIENTS_MAX).step_by(VIRTUAL_CLIENTS_STEP) {
        let mut throughput_sum = 0;
        let mut successful_runs = 0;
        let mut zero_throughput_count = 0;
        let mut run = 0;
        let mut aborted = false;
        while successful_runs < num_runs {
            println!(
                "Running {} with {} clients (run {})",
                config_name, num_virtual, run
            );

            let mut builder = FlowBuilder::from_built(&built);
            builder.replace_ir(deep_clone(ir));
            let run_metadata = deploy_and_optimize(
                &mut reusable_hosts,
                &mut deployment,
                builder.finalize(),
                config_clusters.clone(),
                config_processes.clone(),
                config_optimizations.clone(),
                &config_client_id,
                std::cmp::max(1, num_virtual / PHYSICAL_CLIENTS), // clients per node
                Some(RUN_SECONDS),
                Some(MEASUREMENT_SECOND),
                true,
            )
            .await;

            let run_throughput =
                run_metadata.avg_throughput(START_MEASUREMENT_SECOND, MEASUREMENT_SECOND);
            run_metadata.print_run_summary(&config_location_id_to_cluster, MEASUREMENT_SECOND);
            run_metadata.save_run_metadata(
                &config_location_id_to_cluster,
                &output_dir,
                PHYSICAL_CLIENTS,
                num_virtual,
                run,
            );

            if run_throughput == 0 {
                zero_throughput_count += 1;
                println!(
                    "Zero throughput detected ({}/{})",
                    zero_throughput_count, NUM_RUNS_NO_THROUGHPUT
                );
                if zero_throughput_count > NUM_RUNS_NO_THROUGHPUT {
                    println!(
                        "Exceeded {} zero-throughput runs for this config. Terminating benchmark.",
                        NUM_RUNS_NO_THROUGHPUT
                    );
                    aborted = true;
                    break;
                }
            } else {
                throughput_sum += run_throughput;
                successful_runs += 1;
            }
            run += 1;
        }

        if successful_runs > 0 {
            let current_throughput = throughput_sum / successful_runs;
            println!(
                "clients={}, avg_throughput={}",
                num_virtual, current_throughput
            );
            if current_throughput > best_throughput {
                best_throughput = current_throughput;
                best_profiling = Some(output_dir.join(format!(
                    "profiling_{}c_{}vc_r0.json",
                    PHYSICAL_CLIENTS, num_virtual
                )));
            }
        }

        if aborted {
            break;
        }
    }

    // Post-sweep: iteratively try each non-excluded cluster/process as the bottleneck in parallel.
    if let Some(binary) = scenario_binary
        && let Some(profiling_path) = best_profiling
    {
        run_scenarios_loop(
            &args,
            &binary,
            &output_dir,
            &profiling_path,
            &config_clusters,
            &config_processes,
            &config_exclude,
            best_throughput,
        )
        .await;
    }
}

/// Returns the list of bottleneck names from clusters + processes that are not excluded.
pub fn non_excluded_bottlenecks(
    clusters: &ReusableClusters,
    processes: &ReusableProcesses,
    exclude: &HashSet<LocationId>,
) -> Vec<String> {
    let mut names = vec![];
    for (id, name, _) in &clusters.named_clusters {
        if !exclude.contains(id) {
            names.push(name.clone());
        }
    }
    for (id, name) in &processes.named_processes {
        if !exclude.contains(id) {
            names.push(name.clone());
        }
    }
    names
}

/// Spawns `scenario_binary` (a cargo example) as a subprocess for the given bottleneck.
/// Pipes its stdout to `log_path` and concurrently scans for the finished marker and
/// throughput line. Returns the final parsed throughput once the subprocess prints
/// SCENARIO_FINISHED_PREFIX. The subprocess keeps running to completion (handling its own
/// terraform teardown) in a detached background task; we just don't block on it.
async fn run_one_scenario(
    binary: &str,
    args: &BenchmarkArgs,
    bottleneck: &str,
    rewrites_in: Option<&Path>,
    state_in: Option<&Path>,
    rewrites_out: &Path,
    state_out: &Path,
    profiling: &Path,
    log_path: &Path,
) -> Option<usize> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;
    use tokio::sync::oneshot;

    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--release")
        .arg("--example")
        .arg(binary)
        .arg("--")
        .arg("--bottleneck")
        .arg(bottleneck)
        .arg("--profiling")
        .arg(profiling)
        .arg("--rewrites-out")
        .arg(rewrites_out)
        .arg("--state-out")
        .arg(state_out);
    if let Some(path) = rewrites_in {
        cmd.arg("--rewrites-in").arg(path);
    }
    if let Some(path) = state_in {
        cmd.arg("--state-in").arg(path);
    }
    if let Some(project) = &args.gcp {
        cmd.arg("--gcp").arg(project);
    } else if args.aws {
        cmd.arg("--aws");
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    // Default tokio behavior is kill_on_drop = false, but be explicit: we want the child to
    // keep running after this task finishes so it can finish terraform teardown.
    cmd.kill_on_drop(false);

    let mut child = cmd.spawn().expect("failed to spawn scenario subprocess");
    let stdout = child.stdout.take().unwrap();
    let log_file = tokio::fs::File::create(log_path)
        .await
        .expect("failed to create log file");

    let (tx, rx) = oneshot::channel::<Option<usize>>();

    // Continuously drain stdout to the log file (so the child's pipe never fills) and
    // signal the parent via `tx` the first time we see the finished marker. The task keeps
    // draining until EOF (child exit), then awaits the child to fully reap it.
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        let mut log_file = log_file;
        let mut tx = Some(tx);
        let mut throughput: Option<usize> = None;
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = log_file.write_all(line.as_bytes()).await;
            let _ = log_file.write_all(b"\n").await;
            if let Some(rest) = line.strip_prefix(SCENARIO_FINISHED_PREFIX) {
                throughput = rest.trim().parse::<usize>().ok();
                if let Some(sender) = tx.take() {
                    let _ = sender.send(throughput);
                }
            }
        }
        // EOF: if we never saw the marker, still unblock the parent.
        if let Some(sender) = tx.take() {
            let _ = sender.send(throughput);
        }
        // Reap the child so it doesn't become a zombie.
        let _ = child.wait().await;
    });

    rx.await.ok().flatten()
}

async fn run_scenarios_loop(
    args: &BenchmarkArgs,
    binary: &str,
    output_dir: &Path,
    initial_profiling: &Path,
    clusters: &ReusableClusters,
    processes: &ReusableProcesses,
    exclude: &HashSet<LocationId>,
    initial_throughput: usize,
) {
    let scenarios_dir = output_dir.join("scenarios");
    std::fs::create_dir_all(&scenarios_dir).ok();

    let mut current_profiling = initial_profiling.to_path_buf();
    let mut current_rewrites: Option<PathBuf> = None;
    let mut current_state: Option<PathBuf> = None;
    let mut current_throughput = initial_throughput;
    let mut iteration = 0usize;

    loop {
        let names = non_excluded_bottlenecks(clusters, processes, exclude);
        if names.is_empty() {
            println!("No non-excluded bottlenecks to explore; halting.");
            break;
        }

        // Launch each scenario in parallel.
        let mut handles = vec![];
        for name in &names {
            let out_rewrites =
                scenarios_dir.join(format!("rewrites_iter{}_{}.json", iteration, name));
            let out_state = scenarios_dir.join(format!("state_iter{}_{}.json", iteration, name));
            let log_path = scenarios_dir.join(format!("log_iter{}_{}.txt", iteration, name));

            let binary = binary.to_string();
            let args_clone = BenchmarkArgs {
                gcp: args.gcp.clone(),
                aws: args.aws,
            };
            let bottleneck = name.clone();
            let rewrites_in = current_rewrites.clone();
            let state_in = current_state.clone();
            let profiling = current_profiling.clone();
            let out_rewrites_cl = out_rewrites.clone();
            let out_state_cl = out_state.clone();
            let handle = tokio::spawn(async move {
                let thr = run_one_scenario(
                    &binary,
                    &args_clone,
                    &bottleneck,
                    rewrites_in.as_deref(),
                    state_in.as_deref(),
                    &out_rewrites_cl,
                    &out_state_cl,
                    &profiling,
                    &log_path,
                )
                .await;
                (bottleneck, thr, out_rewrites_cl, out_state_cl)
            });
            handles.push(handle);
        }

        let mut results = vec![];
        for h in handles {
            if let Ok(r) = h.await {
                results.push(r);
            }
        }

        // Pick highest-throughput scenario.
        let best = results
            .iter()
            .filter_map(|(n, thr, rw, st)| thr.map(|t| (n.clone(), t, rw.clone(), st.clone())))
            .max_by_key(|(_, t, _, _)| *t);

        let Some((best_name, best_thr, best_rewrites, best_state)) = best else {
            println!("All scenarios failed or were skipped; halting.");
            break;
        };

        println!(
            "Iteration {}: best bottleneck '{}' throughput {} (previous {})",
            iteration, best_name, best_thr, current_throughput
        );

        if best_thr <= current_throughput {
            println!(
                "No improvement. Halting. Best config: {}",
                best_rewrites.display()
            );
            break;
        }

        current_throughput = best_thr;
        current_rewrites = Some(best_rewrites);
        current_state = Some(best_state);
        // The subprocess also produced a new profiling file next to its rewrites.
        current_profiling =
            scenarios_dir.join(format!("profiling_iter{}_{}.json", iteration, best_name));
        iteration += 1;
    }
}
