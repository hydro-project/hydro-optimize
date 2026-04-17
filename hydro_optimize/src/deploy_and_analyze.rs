use std::collections::{HashMap, HashSet};
use std::fs;
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
use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::prelude::{Cluster, FlowBuilder, Process};
use hydro_lang::telemetry::Sidecar;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::decouple_analysis::decouple_analysis;
use crate::decoupler;
use crate::deploy::{HostType, ReusableHosts};
use crate::parse_results::{RunMetadata, analyze_cluster_results};
use crate::repair::{cycle_source_to_sink_input, inject_id, remove_counter};

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
    exclude: &HashSet<LocationKey>,
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
            if exclude.contains(&metadata.location_id.root().key()) {
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

fn insert_counter(ir: &mut [HydroRoot], duration: &syn::Expr, exclude: &HashSet<LocationKey>) {
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
    named_clusters: Vec<(LocationKey, String, usize)>,
}

impl ReusableClusters {
    pub fn with_cluster<C>(mut self, cluster: Cluster<'_, C>, num_members: usize) -> Self {
        self.named_clusters.push((
            cluster.id().key(),
            std::any::type_name::<C>().to_string(),
            num_members,
        ));
        self
    }

    pub fn from(named_clusters: Vec<(LocationKey, String, usize)>) -> Self {
        Self { named_clusters }
    }

    fn location_name_and_num(&self, location: &LocationId) -> Option<(String, usize)> {
        self.named_clusters
            .iter()
            .find(|(key, _, _)| location.key() == *key)
            .map(|(_, name, count)| (name.clone(), *count))
    }
}

#[derive(Default, Clone)]
pub struct ReusableProcesses {
    named_processes: Vec<(LocationKey, String)>,
}

impl ReusableProcesses {
    pub fn with_process<P>(mut self, process: Process<'_, P>) -> Self {
        self.named_processes
            .push((process.id().key(), std::any::type_name::<P>().to_string()));
        self
    }

    fn location_name(&self, location: &LocationId) -> Option<String> {
        self.named_processes
            .iter()
            .find(|(key, _)| &location.key() == key)
            .map(|(_, name)| name.clone())
    }
}

#[derive(Clone)]
pub struct Optimizations {
    decoupling: bool,
    partitioning: bool,
    pub exclude: HashSet<LocationKey>,
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
        self.exclude.insert(location.key());
        self
    }

    pub fn with_iterations(mut self, iterations: usize) -> Self {
        assert!(iterations > 0);
        self.iterations = iterations;
        self
    }
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
            if *cluster_id == client_id.key() {
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
                deployable = deployable.with_cluster_erased(*cluster_id, client_hosts.concat());
            } else {
                deployable = deployable.with_cluster_erased(
                    *cluster_id,
                    reusable_hosts.get_cluster_hosts(
                        deployment,
                        name.clone(),
                        *num_hosts,
                        0,
                        !excluded,
                    ),
                );
                if !excluded {
                    deployable = deployable.with_sidecar_internal(*cluster_id, &sar_sidecar);
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
            deployable = deployable.with_process_erased(*process_id, host);
            if !excluded {
                deployable = deployable.with_sidecar_internal(*process_id, &sar_sidecar);
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
            run_metadata =
                analyze_cluster_results(&nodes, &mut ir, metrics, stability_second, &optimizations)
                    .await;
            // Remove HydroNode::Counter (since we don't want to consider decoupling those)
            remove_counter(&mut ir);

            apply_optimizations(
                &mut ir,
                &optimizations,
                &mut clusters,
                &processes,
                &mut post_rewrite_builder,
                iteration,
            );
        }

        post_rewrite_builder.replace_ir(ir);
        builder = post_rewrite_builder.finalize();
    }

    run_metadata
}

/// Applies optimizations (decoupling) to the IR based on profiling data.
pub fn apply_optimizations(
    ir: &mut [HydroRoot],
    optimizations: &Optimizations,
    clusters: &mut ReusableClusters,
    processes: &ReusableProcesses,
    post_rewrite_builder: &mut FlowBuilder<'_>,
    iteration: usize,
) {
    let cycle_source_to_sink = cycle_source_to_sink_input(ir);

    if optimizations.decoupling {
        // TODO: Re-enable and apply to all nodes in the program
        // let (bottleneck_name, bottleneck_num_nodes) = processes
        //     .location_name(bottleneck)
        //     .map(|name| (name, 1))
        //     .unwrap_or_else(|| clusters.location_name_and_num(bottleneck).unwrap());
        //
        // let decision = decouple_analysis(
        //     ir,
        //     bottleneck,
        //     0.01,
        //     0.01,
        //     &cycle_source_to_sink,
        // );
        //
        // let new_clusters =
        //     decoupler::decouple(ir, decision, bottleneck, post_rewrite_builder);
        // for cluster in new_clusters {
        //     clusters.named_clusters.push((
        //         cluster.id().key(),
        //         format!("{}-decouple-{}", bottleneck_name, iteration),
        //         bottleneck_num_nodes,
        //     ));
        // }
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
}

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
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };

    let mut reusable_hosts = ReusableHosts::new(&host_type);

    const START_MEASUREMENT_SECOND: usize = 30;
    const MEASUREMENT_SECOND: usize = 59;
    const RUN_SECONDS: usize = 90;
    const PHYSICAL_CLIENTS: usize = 10;
    const VIRTUAL_CLIENTS_MAX: usize = 50 * PHYSICAL_CLIENTS; // Based on manual testing, an 8-core m5.2xlarge's CPU saturates around 50 clients.
    const VIRTUAL_CLIENTS_STEP: usize = 50; // Can tweak to get finer-grained numbers
    const NUM_RUNS_NO_THROUGHPUT: usize = 3;

    let benchmark_config = run_benchmark(PHYSICAL_CLIENTS);
    // Set up FlowBuilder for cloning
    let built = benchmark_config.builder.finalize();
    let ir = built.ir();
    let output_dir = Path::new("benchmark_results").join(format!(
        "{}_{}",
        benchmark_config.name,
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));

    for num_virtual in (1..=VIRTUAL_CLIENTS_MAX).step_by(VIRTUAL_CLIENTS_STEP) {
        let mut throughput_sum = 0;
        let mut successful_runs = 0;
        let mut zero_throughput_count = 0;
        let mut run = 0;
        while successful_runs < num_runs {
            println!(
                "Running {} with {} clients (run {})",
                benchmark_config.name, num_virtual, run
            );

            let mut builder = FlowBuilder::from_built(&built);
            builder.replace_ir(deep_clone(ir));
            let run_metadata = deploy_and_optimize(
                &mut reusable_hosts,
                &mut deployment,
                builder.finalize(),
                benchmark_config.clusters.clone(),
                benchmark_config.processes.clone(),
                benchmark_config.optimizations.clone(),
                &benchmark_config.client_id.clone(),
                std::cmp::max(1, num_virtual / PHYSICAL_CLIENTS), // clients per node
                Some(RUN_SECONDS),
                Some(MEASUREMENT_SECOND),
                true,
            )
            .await;

            let run_throughput =
                run_metadata.avg_throughput(START_MEASUREMENT_SECOND, MEASUREMENT_SECOND);
            run_metadata
                .print_run_summary(&benchmark_config.location_id_to_cluster, MEASUREMENT_SECOND);
            run_metadata.save_run_metadata(
                &benchmark_config.location_id_to_cluster,
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

        let current_throughput = throughput_sum / num_runs;
        println!(
            "clients={}, avg_throughput={}",
            num_virtual, current_throughput
        );
    }
}
