use std::collections::HashMap;

use hydro_optimize_examples::chain_replication::chain_replication_bench::chain_replication_bench;
use hydro_optimize_examples::chain_replication::cas::MockCAS;
use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
};
use stageleft::q;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct Args {
    #[command(flatten)]
    graph: GraphConfig,

    /// Use Gcp for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use Aws, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

/// Runs a single chain replication benchmark with the given parameters
fn run_benchmark<'a>(num_clients: usize) -> BenchmarkConfig<'a> {
    let num_replicas = 6;
    let heartbeat_output_millis = 1000;
    let heartbeat_check_millis = 10000;
    let heartbeat_expired_millis = 15000;
    let print_result_frequency = 1000;

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let replicas = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let mock_cas = builder.process();

    let location_id_to_cluster = HashMap::from([
        (replicas.id(), "replica".to_string()),
        (clients.id(), "client".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
        (mock_cas.id(), "mock_cas".to_string()),
    ]);

    let client_id = clients.id();

    chain_replication_bench(
        &replicas,
        num_replicas,
        MockCAS { process: &mock_cas },
        &clients,
        clients.singleton(q!(1usize)),
        &client_aggregator,
        heartbeat_output_millis,
        heartbeat_check_millis,
        heartbeat_expired_millis,
        print_result_frequency,
    );

    let clusters = ReusableClusters::default()
        .with_cluster(replicas, num_replicas)
        .with_cluster(clients, num_clients);
    let processes = ReusableProcesses::default()
        .with_process(client_aggregator)
        .with_process(mock_cas);
    let optimizations = Optimizations::default();

    BenchmarkConfig {
        name: "Chain_Replication".to_string(),
        builder, 
        clusters,
        processes,
        client_id,
        optimizations,
        location_id_to_cluster,
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let num_runs = 1;
    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp,
            aws: args.aws,
        },
        num_runs,
        run_benchmark,
    )
    .await;
}
