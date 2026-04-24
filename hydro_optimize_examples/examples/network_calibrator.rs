use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses, VIRTUAL_CLIENTS_MAX, benchmark_protocol
};
use hydro_optimize_examples::network_calibrator::network_calibrator;

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

    /// Message size in bytes
    #[arg(long, default_value = "8")]
    message_size: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let message_size = args.message_size;
    let num_clients = VIRTUAL_CLIENTS_MAX;
    let num_runs = 1;

    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp,
            aws: args.aws,
        },
        num_clients,
        num_runs,
        move |num_clients| {
            let print_result_frequency = 1000;

            let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
            let server = builder.cluster();
            let clients = builder.cluster();
            let client_aggregator = builder.process();

            let client_id = clients.id();
            let aggregator_id = client_aggregator.id();

            let location_id_to_cluster = HashMap::from([
                (server.id(), "server".to_string()),
                (client_id.clone(), "client".to_string()),
                (aggregator_id.clone(), "client_aggregator".to_string()),
            ]);

            network_calibrator(
                message_size,
                &server,
                &clients,
                clients.singleton(q!(1usize)),
                &client_aggregator,
                print_result_frequency,
            );

            let clusters = ReusableClusters::default()
                .with_cluster(server, 1)
                .with_cluster(clients, num_clients);
            let processes = ReusableProcesses::default().with_process(client_aggregator);
            let optimizations = Optimizations::default()
                .excluding(client_id.clone())
                .excluding(aggregator_id)
                .with_no_counters();

            BenchmarkConfig {
                name: "Network".to_string(),
                builder,
                clusters,
                processes,
                client_id,
                optimizations,
                location_id_to_cluster,
            }
        },
    )
    .await;
}
