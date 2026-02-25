use std::collections::HashMap;
use std::path::Path;

use chrono::Local;
use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_lang::{viz::config::GraphConfig};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
};
use hydro_optimize_examples::network_calibrator::network_calibrator;

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

fn run_benchmark<'a>(num_clients: usize, num_clients_per_node: usize) -> BenchmarkConfig<'a> {
    let message_size = 8;
    let print_result_frequency = 1000;

    println!(
        "Running network calibrator with {} clients and {} virtual clients per node",
        num_clients, num_clients_per_node,
    );

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let server = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let location_id_to_cluster = HashMap::from([
        (server.id(), "server".to_string()),
        (clients.id(), "client".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
    ]);

    network_calibrator(
        num_clients_per_node,
        message_size,
        &server,
        &clients,
        &client_aggregator,
        print_result_frequency,
    );

    let clusters = ReusableClusters::default()
        .with_cluster(server, 1)
        .with_cluster(clients, num_clients);
    let processes = ReusableProcesses::default().with_process(client_aggregator);
    let optimizations = Optimizations::default();

    let output_dir = Path::new("benchmark_results").join(format!(
        "network_{}",
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));

    BenchmarkConfig {
        builder,
        clusters,
        processes,
        optimizations,
        location_id_to_cluster,
        output_dir,
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let num_runs = 3;
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
