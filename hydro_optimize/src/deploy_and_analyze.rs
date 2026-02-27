use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

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

pub const NUM_CLIENTS_PER_NODE_ENV: &str = "NUM_CLIENTS_PER_NODE";

// Note: Ensure edits to the match arms are consistent with inject_count_node
fn insert_counter_node(node: &mut HydroNode, next_stmt_id: &mut usize, duration: syn::Expr) {
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
         => {
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
         => {}
    }
}

fn insert_counter(ir: &mut [HydroRoot], duration: &syn::Expr) {
    traverse_dfir(ir,
    |_, _| {},
    |node, next_stmt_id| {
        insert_counter_node(node, next_stmt_id, duration.clone());
    },);
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

pub struct Optimizations {
    decoupling: bool,
    partitioning: bool,
    exclude: Vec<String>,
    iterations: usize, // Must be at least 1
}

impl Default for Optimizations {
    fn default() -> Self {
        Self {
            decoupling: false,
            partitioning: false,
            exclude: vec![],
            iterations: 1,
        }
    }
}

impl Clone for Optimizations {
    fn clone(&self) -> Self {
        Self {
            decoupling: self.decoupling,
            partitioning: self.partitioning,
            exclude: self.exclude.clone(),
            iterations: self.iterations,
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

    pub fn excluding<T>(mut self) -> Self {
        self.exclude.push(std::any::type_name::<T>().to_string());
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
    num_seconds: Option<usize>,
    stability_second: Option<usize>,
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
            stability_second.is_some(),
            "Must select stability_second with optimizations"
        );
    }

    let counter_output_duration =
        syn::parse_quote!(std::time::Duration::from_secs(#METRIC_INTERVAL_SECS));
    let mut run_metadata = RunMetadata::default();

    for iteration in 0..optimizations.iterations {
        // Rewrite with counter tracking
        let mut post_rewrite_builder = FlowBuilder::from_built(&builder);
        let optimized = builder.optimize_with(|leaf| {
            inject_id(leaf);
            insert_counter(leaf, &counter_output_duration);
        });
        let mut ir = deep_clone(optimized.ir());

        // Insert all clusters & processes
        let mut deployable = optimized.into_deploy();
        for (cluster_id, name, num_hosts) in clusters.named_clusters.iter() {
            deployable = deployable.with_cluster_erased(
                *cluster_id,
                reusable_hosts.get_cluster_hosts(deployment, name.clone(), *num_hosts),
            );
        }
        for (process_id, name) in processes.named_processes.iter() {
            deployable = deployable.with_process_erased(
                *process_id,
                reusable_hosts.get_no_perf_process_hosts(deployment, name.clone()),
            );
        }

        // Measure network (-n DEV) and CPU (-u) usage
        let sar_sidecar = ScriptSidecar {
            script: "sar -n DEV -u 1".to_string(),
            prefix: SAR_USAGE_PREFIX.to_string(),
        };
        let nodes = deployable
            .with_sidecar_all(&sar_sidecar) // Measure network usage
            .deploy(deployment);
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

        // Parse results to get metrics
        run_metadata = analyze_cluster_results(&nodes, &mut ir, metrics).await;
        // Remove HydroNode::Counter (since we don't want to consider decoupling those)
        remove_counter(&mut ir);

        // Create a mapping from each CycleSink to its corresponding CycleSource
        let cycle_source_to_sink_input = cycle_source_to_sink_input(&mut ir);

        if optimizations.decoupling {
            let bottleneck = run_metadata.cpu_bottleneck(stability_second.unwrap());
            // The bottleneck is either a process or cluster. Panic otherwise.
            let (bottleneck_name, bottleneck_num_nodes) = processes
                .location_name(&bottleneck)
                .map(|name| (name, 1))
                .unwrap_or_else(|| clusters.location_name_and_num(&bottleneck).unwrap());

            let decision = decouple_analysis(
                &mut ir,
                &bottleneck,
                0.01, // TODO: Deprecate use of send/recv overheads, since they are inaccurate anyway
                0.01,
                &cycle_source_to_sink_input,
            );

            // Apply decoupling — creates new clusters as needed
            let new_clusters =
                decoupler::decouple(&mut ir, decision, &bottleneck, &mut post_rewrite_builder);
            for cluster in new_clusters {
                clusters.named_clusters.push((
                    cluster.id().key(),
                    format!("{}-decouple-{}", bottleneck_name, iteration),
                    bottleneck_num_nodes,
                ));
            }
            // TODO: Save decoupling decision to file
        }

        post_rewrite_builder.replace_ir(ir);
        builder = post_rewrite_builder.finalize();
    }

    run_metadata
}

/// Writes per-second metrics CSV for each location, combining sar stats with
/// the shared throughput and latency time series.
fn write_metrics_csv(
    output_dir: &Path,
    run_metadata: &RunMetadata,
    location_id_to_cluster: &HashMap<LocationId, String>,
    num_clients: usize,
    num_clients_per_node: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;

    for (location, stats) in &run_metadata.sar_stats {
        if stats.is_empty() {
            continue;
        }

        let location_name = location_id_to_cluster.get(location).unwrap();
        let filename = output_dir.join(format!(
            "{}_{}c_{}vc.csv",
            location_name, num_clients, num_clients_per_node
        ));
        let num_rows = stats
            .len()
            .max(run_metadata.throughputs.len())
            .max(run_metadata.latencies.len());

        let mut file = File::create(&filename)?;
        writeln!(
            file,
            "time_s,cpu_user,cpu_system,cpu_idle,\
             network_tx_packets_per_sec,network_rx_packets_per_sec,\
             network_tx_bytes_per_sec,network_rx_bytes_per_sec,\
             throughput_rps,latency_p50_ms,latency_p99_ms,latency_p999_ms,latency_samples",
        )?;

        for i in 0..num_rows {
            let sar = stats.get(i).copied().unwrap_or_default();
            let thr = run_metadata.throughputs.get(i).copied().unwrap_or(0);
            let (p50, p99, p999, count) =
                run_metadata.latencies.get(i).copied().unwrap_or_default();
            writeln!(
                file,
                "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{:.3},{:.3},{:.3},{}",
                i,
                sar.cpu.user,
                sar.cpu.system,
                sar.cpu.idle,
                sar.network.tx_packets_per_sec,
                sar.network.rx_packets_per_sec,
                sar.network.tx_bytes_per_sec,
                sar.network.rx_bytes_per_sec,
                thr,
                p50,
                p99,
                p999,
                count,
            )?;
        }

        println!("Generated CSV: {}", filename.display());
    }

    Ok(())
}

async fn output_metrics(
    run_metadata: RunMetadata,
    location_id_to_cluster: &HashMap<LocationId, String>,
    output_dir: &Path,
    num_clients: usize,
    num_clients_per_node: usize,
    measurement_second: usize,
) {
    if let Some(&throughput) = run_metadata.throughputs.get(measurement_second) {
        println!(
            "Throughput @{}s: {} requests/s",
            measurement_second + 1,
            throughput
        );
    }
    if let Some(&(p50, p99, p999, samples)) = run_metadata.latencies.get(measurement_second) {
        println!(
            "Latency @{}s: p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({} samples)",
            measurement_second + 1,
            p50,
            p99,
            p999,
            samples
        );
    }
    run_metadata
        .sar_stats
        .iter()
        .for_each(|(location, sar_stats)| {
            println!(
                "{} CPU: {:.2}%",
                location_id_to_cluster.get(location).unwrap(),
                sar_stats
                    .last()
                    .map(|stats| stats.cpu.user + stats.cpu.system)
                    .unwrap_or_default()
            )
        });

    if let Err(e) = write_metrics_csv(
        output_dir,
        &run_metadata,
        location_id_to_cluster,
        num_clients,
        num_clients_per_node,
    ) {
        eprintln!("Failed to write CSV: {}", e);
    }
}

pub struct BenchmarkArgs {
    pub gcp: Option<String>,
    pub aws: bool,
}

pub struct BenchmarkConfig<'a> {
    pub builder: FlowBuilder<'a>,
    pub clusters: ReusableClusters,
    pub processes: ReusableProcesses,
    pub optimizations: Optimizations,
    pub location_id_to_cluster: HashMap<LocationId, String>,
    pub output_dir: PathBuf,
}

pub async fn benchmark_protocol<'a>(
    args: BenchmarkArgs,
    run_benchmark: fn(usize) -> BenchmarkConfig<'a>,
) {
    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };

    let mut reusable_hosts = ReusableHosts::new(host_type);

    const MEASUREMENT_SECOND: usize = 59;
    const RUN_SECONDS: usize = 90;
    const PHYSICAL_CLIENTS_MIN: usize = 1;
    const PHYSICAL_CLIENTS_MAX: usize = 10;
    const VIRTUAL_CLIENTS_STEP: usize = 100;
    const STALL_PATIENCE: usize = 3;

    let mut best_throughput: usize = 0;
    let mut best_config: (usize, usize) = (0, 0);
    let mut output_dir = None;

    for num_clients in (PHYSICAL_CLIENTS_MIN..=PHYSICAL_CLIENTS_MAX).step_by(1) {
        let BenchmarkConfig {
            builder,
            clusters,
            processes,
            optimizations,
            location_id_to_cluster,
            output_dir: config_output_dir,
        } = run_benchmark(num_clients);
        let built = builder.finalize();

        let mut round_best_throughput = 0;
        let mut round_best_virtual = 0;
        let mut stall_count = 0;

        // Start at the per-node count that yields roughly the same total
        // virtual clients as the previous round's best throughput config,
        // rounded down to the nearest step (minimum 1).
        let mut num_virtual = std::cmp::max(1, best_config.0 * best_config.1 / num_clients);

        while stall_count <= STALL_PATIENCE {
            let iteration_built = FlowBuilder::from_built(&built).finalize();
            // Set the number of virtual clients
            reusable_hosts.insert_env(NUM_CLIENTS_PER_NODE_ENV.to_string(), num_virtual.to_string());

            let run_metadata = deploy_and_optimize(
                &mut reusable_hosts,
                &mut deployment,
                iteration_built,
                clusters.clone(),
                processes.clone(),
                optimizations.clone(),
                Some(RUN_SECONDS),
                Some(MEASUREMENT_SECOND),
            )
            .await;

            let output_dir = output_dir.get_or_insert(config_output_dir.clone());

            let current_throughput = run_metadata.throughputs[MEASUREMENT_SECOND];
            println!(
                "physical_clients={}, virtual_clients={}, throughput@{}s={}",
                num_clients,
                num_virtual,
                MEASUREMENT_SECOND + 1,
                current_throughput
            );

            output_metrics(
                run_metadata,
                &location_id_to_cluster,
                output_dir,
                num_clients,
                num_virtual,
                MEASUREMENT_SECOND,
            )
            .await;

            if current_throughput > round_best_throughput {
                round_best_throughput = current_throughput;
                round_best_virtual = num_virtual;
                stall_count = 0;
            } else {
                stall_count += 1;
            }

            num_virtual += VIRTUAL_CLIENTS_STEP;
        }

        println!(
            "Throughput stalled for {} configs at {} physical clients \
                     (best={} at {}vc). Moving to next physical client count.",
            STALL_PATIENCE, num_clients, round_best_throughput, round_best_virtual
        );

        if round_best_throughput > best_throughput {
            best_throughput = round_best_throughput;
            best_config = (num_clients, round_best_virtual);
        } else {
            println!(
                "Throughput still saturated after increasing physical clients \
                 (prior best={}, current_peak={}). Stopping search.",
                best_throughput, round_best_throughput
            );
            break;
        }
    }

    println!("\n=== Benchmark Summary ===");
    println!(
        "Best throughput: {} rps with {} physical clients and {} virtual clients",
        best_throughput, best_config.0, best_config.1
    );
}
