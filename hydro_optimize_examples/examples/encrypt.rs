use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::{location::Location, prelude::FlowBuilder};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, CompiledProgram, NUM_PHYSICAL_CLIENTS, Optimization,
    ReusableClusters, ReusableProcesses, benchmark_protocol,
};
use hydro_optimize_examples::encrypt_bench::{Aggregator, Client, Server, encrypt_bench};
use stageleft::q;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct Args {
    /// Use Gcp for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use Aws, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = BenchmarkConfig {
        name: "Encrypt".to_string(),
        kind: Optimization::None,
        num_physical_clients: 1,
        start_virtual_clients: 1,
        virtual_clients_step: 1,
        num_runs: 1,
        calibrate_message_sizes: None,
    };

    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp.clone(),
            aws: args.aws,
        },
        config,
        &[((), "default".to_string())],
        move |_: &()| {
            let print_result_frequency = 1000;
            let mut builder = FlowBuilder::new();
            let server = builder.cluster::<Server>();
            let clients = builder.cluster::<Client>();
            let client_aggregator = builder.process::<Aggregator>();

            let client_id = clients.id();
            let client_aggregator_id = client_aggregator.id();

            let location_id_to_cluster = HashMap::from([
                (server.id(), "server".to_string()),
                (client_id.clone(), "client".to_string()),
                (
                    client_aggregator_id.clone(),
                    "client_aggregator".to_string(),
                ),
            ]);

            encrypt_bench(
                &server,
                &clients,
                clients.singleton(q!({
                    std::env::var("NUM_VIRTUAL_CLIENTS")
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                })),
                &client_aggregator,
                print_result_frequency,
            );

            let clusters = ReusableClusters::default()
                .with_cluster(server, 1)
                .with_cluster(clients, NUM_PHYSICAL_CLIENTS);
            let processes = ReusableProcesses::default().with_process(client_aggregator);
            let program = CompiledProgram::new(clusters, processes, location_id_to_cluster)
                .excluding(client_id);

            (builder, program)
        },
    )
    .await;

    println!("=== Calibration complete ===");
}
