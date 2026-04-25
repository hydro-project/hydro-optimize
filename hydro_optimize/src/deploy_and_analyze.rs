use std::collections::{HashMap, HashSet};
use std::path::Path;
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
use tokio::sync::mpsc::UnboundedReceiver;

use crate::deploy::{HostType, ReusableHosts};
use crate::parse_results::{RunMetadata, analyze_cluster_results};
use crate::reduce_pushdown::reduce_pushdown;
use crate::reduce_pushdown_analysis::reduce_pushdown_decision;
use crate::repair::inject_id;
use crate::rewriter::apply_rewrite;
use crate::rewrites::Rewrite;

const METRIC_INTERVAL_SECS: u64 = 1;
const BYTE_SIZE_SAMPLE_EVERY_N: usize = 10000; // Sample every 10000th element for byte size to avoid excessive overhead
const COUNTER_PREFIX: &str = "_optimize_counter";
const BYTE_SIZE_PREFIX: &str = "_optimize_byte_size";
const CPU_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_CPU:";
const SAR_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_SAR:";
const LATENCY_PREFIX: &str = "HYDRO_OPTIMIZE_LAT:";
const THROUGHPUT_PREFIX: &str = "HYDRO_OPTIMIZE_THR:";

/// Applies reduce pushdown for all locations in the IR except excluded ones.
fn apply_reduce_pushdown(ir: &mut [HydroRoot], exclude: &HashSet<LocationId>) {
    // TODO: This way of getting locations is wrong, again
    let locations: HashSet<_> = ir
        .iter()
        .map(|root| root.input_metadata().location_id.root().clone())
        .filter(|loc| !exclude.contains(loc))
        .collect();
    for loc in locations {
        let decision = reduce_pushdown_decision(ir, &loc);
        reduce_pushdown(ir, decision);
    }
}

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

            // Use the original op_id from inject_id, not the traversal counter
            let original_id = metadata.op.id.unwrap();
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

fn insert_counter(ir: &mut [HydroRoot], duration: &syn::Expr, exclude: &HashSet<LocationId>) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            insert_counter_node(node, next_stmt_id, duration.clone(), exclude);
        },
    );
}

/// Inserts an Inspect node that samples every Nth element's serialized byte size,
/// printing it as `BYTE_SIZE_PREFIX(original_op_id): size`.
/// `network_ops` contains original op_ids (before any insertions).
/// Inserted at:
/// - Ops where `network_ops` indicates a network boundary
/// - After existing Network nodes
fn insert_byte_size_inspect(
    ir: &mut [HydroRoot],
    network_ops: &HashSet<usize>,
) {
    use crate::rewrites::collection_kind_to_debug_type;

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
            let sample_every_n = BYTE_SIZE_SAMPLE_EVERY_N;

            let f: syn::Expr = syn::parse_quote!({
                let mut __byte_size_counter: usize = 0;
                move |item: &#element_type| {
                    __byte_size_counter += 1;
                    if __byte_size_counter % #sample_every_n == 0 {
                        let size = bincode::serialized_size(item).unwrap_or(0);
                        println!("{}({}): {}", #prefix, #tag, size);
                    }
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

    pub fn location_name_and_num(&self, location: &LocationId) -> Option<(String, usize)> {
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
}

#[derive(Clone, Default)]
pub struct Optimizations {
    decoupling: bool,
    partitioning: bool,
    no_counters: bool,
    /// Insert size measuring nodes wherever decoupling is possible
    size_analysis: bool,
    /// Apply greedy decoupling, deploy decoupled system, gather per-operator SAR costs
    blow_up_analysis: bool,
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

    /// Do not insert counters. Used to establish highest-performance config
    pub fn with_no_counters(mut self) -> Self {
        self.no_counters = true;
        self
    }

    /// Insert size measuring nodes wherever decoupling is possible
    pub fn with_size_analysis(mut self) -> Self {
        self.size_analysis = true;
        self
    }

    /// Apply greedy decoupling, deploy decoupled system, gather per-operator SAR costs
    pub fn with_blow_up_analysis(mut self) -> Self {
        self.blow_up_analysis = true;
        self
    }

    pub fn excluding(mut self, location: LocationId) -> Self {
        self.exclude.insert(location);
        self
    }
}

/// `stability_second`: The second in which the protocol is expected to be stable, and its performance can be used as the basis for optimization.
#[allow(clippy::too_many_arguments)]
pub async fn deploy_and_analyze<'a>(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    builder: BuiltFlow<'a>,
    clusters: &ReusableClusters,
    processes: &ReusableProcesses,
    optimizations: &Optimizations,
    client_id: &LocationId,
    num_clients_per_node: usize,
    num_seconds: Option<usize>,
    stability_second: Option<usize>,
) -> RunMetadata {
    if let Some(num_seconds) = num_seconds {
        assert!(
            stability_second.is_some_and(|stability_time| stability_time < num_seconds),
            "Invariant: stability_second < num_seconds"
        );
    }
    assert!(
        num_clients_per_node > 0,
        "Must have at least 1 client per node"
    );

    let counter_output_duration =
        syn::parse_quote!(std::time::Duration::from_secs(#METRIC_INTERVAL_SECS));
    // Measure network (-n DEV) and CPU (-u) usage. -P ALL measures all CPUs on the machine
    let sar_sidecar = ScriptSidecar {
        script: "sar -n DEV -u -P ALL -r -b 1".to_string(),
        prefix: SAR_USAGE_PREFIX.to_string(),
    };

    // Rewrite with optional decoupled deployment (blow_up_analysis) or size inspection (size_analysis)
    let mut extra_clusters: Vec<(LocationId, String, usize)> = vec![];
    // Maps new LocationId → list of original op_ids assigned to that location
    let mut location_to_original_ops: HashMap<LocationId, Vec<usize>> = HashMap::new();

    // Always reduce pushdown and inject IDs first
    let built = builder.optimize_with(|leaf| {
        apply_reduce_pushdown(leaf, &optimizations.exclude);
        inject_id(leaf);
    });

    let optimized = if optimizations.blow_up_analysis {
        let mut greedy_results = None;
        let built = built.optimize_with(|leaf| {
            greedy_results = Some(crate::greedy_decouple_analysis::greedy_decouple_analysis(
                leaf,
                &optimizations.exclude,
            ));
        });

        let mut builder = FlowBuilder::from_built(&built);
        let mut ir = deep_clone(built.ir());

        for (orig_loc, possible_rewrite) in greedy_results.unwrap() {
            let cluster_size = clusters
                .location_name_and_num(&orig_loc)
                .map(|(_, n)| n)
                .unwrap_or(1); // Is process

            // Build locations_map: index 0 = original, index > 0 = new clusters
            let mut locations_map = HashMap::new();
            locations_map.insert(0, orig_loc.clone());
            for loc_idx in possible_rewrite.locations() {
                if loc_idx == 0 {
                    continue;
                }
                let cluster = builder.cluster::<()>();
                let new_loc = cluster.id().clone();
                locations_map.insert(loc_idx, new_loc.clone());
                extra_clusters.push((new_loc, format!("decouple_{}", loc_idx), cluster_size));
            }

            // Record which original ops end up at which deployed location so we can reassociate metrics with the ops
            for (&op_id, &loc_idx) in &possible_rewrite.op_to_loc {
                let deployed_loc = locations_map.get(&loc_idx).unwrap();
                location_to_original_ops
                    .entry(deployed_loc.clone())
                    .or_default()
                    .push(op_id);
            }

            let rewrite = Rewrite {
                possible_rewrite,
                num_partitions: 0,
                original_node: orig_loc,
                cluster_size,
            };
            apply_rewrite(&mut ir, &rewrite, &locations_map);
        }

        builder.replace_ir(ir);
        builder.finalize()
    } else if optimizations.size_analysis {
        // Insert byte-size inspects at network boundaries without applying decoupling
        built.optimize_with(|leaf| {
            let per_loc_rewrites = crate::greedy_decouple_analysis::greedy_decouple_analysis(
                leaf,
                &optimizations.exclude,
            );
            let network_ops: HashSet<usize> = per_loc_rewrites
                .values()
                .flat_map(|r| r.op_to_network.keys().copied())
                .collect();
            if !network_ops.is_empty() {
                insert_byte_size_inspect(leaf, &network_ops);
            }
        })
    } else {
        built
    };

    // Insert counters
    let optimized = if !optimizations.no_counters {
        optimized.optimize_with(|leaf| {
            insert_counter(leaf, &counter_output_duration, &optimizations.exclude);
        })
    } else {
        optimized
    };

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
            deployable = deployable.with_cluster_erased(cluster_id.key(), client_hosts.concat());
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
        }
        if !excluded {
            deployable = deployable.with_sidecar_internal(cluster_id.key(), &sar_sidecar);
        }
    }
    // Deploy extra clusters created by greedy decouple analysis
    for (cluster_id, name, num_hosts) in extra_clusters.iter() {
        deployable = deployable.with_cluster_erased(
            cluster_id.key(),
            reusable_hosts.get_cluster_hosts(deployment, name.clone(), *num_hosts, 0, true),
        );
        deployable = deployable.with_sidecar_internal(cluster_id.key(), &sar_sidecar);
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
    let mut run_metadata = analyze_cluster_results(&nodes, metrics, optimizations).await;
    run_metadata.location_to_original_ops = location_to_original_ops;
    run_metadata
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
    start_virtual_clients: usize,
    num_runs: usize,
    run_benchmark: impl Fn(usize) -> BenchmarkConfig<'a>,
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

    let BenchmarkConfig {
        name: config_name,
        builder,
        clusters: config_clusters,
        processes: config_processes,
        optimizations: config_optimizations,
        client_id: config_client_id,
        location_id_to_cluster: config_location_id_to_cluster,
    } = run_benchmark(PHYSICAL_CLIENTS);
    // Set up FlowBuilder for cloning
    let built = builder.finalize();
    let ir = built.ir();
    let output_dir = Path::new("benchmark_results").join(format!(
        "{}_{}",
        config_name,
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));

    for num_virtual in (start_virtual_clients..=VIRTUAL_CLIENTS_MAX).step_by(VIRTUAL_CLIENTS_STEP) {
        let mut throughput_sum = 0;
        let mut successful_runs = 0;
        let mut zero_throughput_count = 0;
        let mut run = 0;
        while successful_runs < num_runs {
            println!(
                "Running {} with {} clients (run {})",
                config_name, num_virtual, run
            );

            let mut builder = FlowBuilder::from_built(&built);
            builder.replace_ir(deep_clone(ir));
            let run_metadata = deploy_and_analyze(
                &mut reusable_hosts,
                &mut deployment,
                builder.finalize(),
                &config_clusters,
                &config_processes,
                &config_optimizations,
                &config_client_id,
                std::cmp::max(1, num_virtual / PHYSICAL_CLIENTS), // clients per node
                Some(RUN_SECONDS),
                Some(MEASUREMENT_SECOND),
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
                    return;
                }
            } else {
                throughput_sum += run_throughput;
                successful_runs += 1;
            }
            run += 1;
        }

        let current_throughput = throughput_sum / successful_runs;
        println!(
            "clients={}, avg_throughput={}",
            num_virtual, current_throughput
        );
    }
}
