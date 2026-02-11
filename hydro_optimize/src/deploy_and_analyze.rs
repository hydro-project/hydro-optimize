use std::collections::HashMap;
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
use crate::decoupler::{self, Decoupler};
use crate::deploy::ReusableHosts;
use crate::parse_results::{RunMetadata, analyze_cluster_results, analyze_send_recv_overheads};
use crate::repair::{cycle_source_to_sink_input, inject_id, remove_counter};

pub(crate) const METRIC_INTERVAL_SECS: u64 = 1;
const COUNTER_PREFIX: &str = "_optimize_counter";
pub(crate) const CPU_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_CPU:";
pub(crate) const SAR_USAGE_PREFIX: &str = "HYDRO_OPTIMIZE_SAR:";
pub(crate) const LATENCY_PREFIX: &str = "HYDRO_OPTIMIZE_LAT:";
pub(crate) const THROUGHPUT_PREFIX: &str = "HYDRO_OPTIMIZE_THR:";

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
    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            insert_counter_node(node, next_stmt_id, duration.clone());
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

#[derive(Default)]
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

    fn location_name_and_num(&self, location: &LocationId) -> Option<(String, usize)> {
        self.named_clusters
            .iter()
            .find(|(key, _, _)| location.key() == *key)
            .map(|(_, name, count)| (name.clone(), *count))
    }
}

#[derive(Default)]
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
            decoupling: true,
            partitioning: true,
            exclude: vec![],
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
                .and_then(|name| Some((name, 1)))
                .unwrap_or_else(|| clusters.location_name_and_num(&bottleneck).unwrap());

            let decision = decouple_analysis(
                &mut ir,
                &bottleneck,
                0.01, // TODO: Deprecate use of send/recv overheads, since they are inaccurate anyway
                0.01,
                &cycle_source_to_sink_input,
            );

            // Apply decoupling
            let new_cluster = post_rewrite_builder.cluster::<()>();
            let decouple_with_location = Decoupler {
                decision,
                orig_location: bottleneck,
                decoupled_location: new_cluster.id().clone(),
            };
            decoupler::decouple(&mut ir, &decouple_with_location);
            clusters.named_clusters.push((
                new_cluster.id().key(),
                format!("{}-decouple-{}", bottleneck_name, iteration),
                bottleneck_num_nodes,
            ));
            // TODO: Save decoupling decision to file
        }

        post_rewrite_builder.replace_ir(ir);
        builder = post_rewrite_builder.finalize();
    }

    run_metadata
}
