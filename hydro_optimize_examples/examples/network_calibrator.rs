use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::{location::Location, prelude::FlowBuilder};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
};
use hydro_optimize_examples::network_calibrator::network_calibrator;
use stageleft::q;

// Message sizes between 1 and 64 have similar costs
const CALIBRATION_SIZES: &[usize] = &[1, 64, 128, 256, 512, 1024, 2048, 4096];
// Saturating only starts at 4 cliens for 1B messages
const NUM_PHYSICAL_CLIENTS_TO_TEST: &[usize] = &[4, 10];

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

    for &size in CALIBRATION_SIZES {
        for &num_physical in NUM_PHYSICAL_CLIENTS_TO_TEST {
            println!("=== Calibrating message_size={}, physical_clients={} ===", size, num_physical);

            benchmark_protocol(
                BenchmarkArgs {
                    gcp: args.gcp.clone(),
                    aws: args.aws,
                },
                move || {
                    let mut builder = FlowBuilder::new();
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
                        size,
                        &server,
                        &clients,
                        clients.singleton(q!({
                            std::env::var("NUM_VIRTUAL_CLIENTS")
                                .unwrap()
                                .parse::<usize>()
                                .unwrap()
                        })),
                        &client_aggregator,
                        1000,
                    );

                    let clusters = ReusableClusters::default()
                        .with_cluster(server, 1)
                        .with_cluster(clients, num_physical);
                    let processes = ReusableProcesses::default().with_process(client_aggregator);
                    let optimizations = Optimizations::default()
                        .excluding(client_id.clone())
                        .excluding(aggregator_id);

                    (builder, BenchmarkConfig {
                        name: format!("Network_{}b_{}c", size, num_physical),
                        workload: "default".to_string(),
                        clusters,
                        processes,
                        optimizations,
                        location_id_to_cluster,
                        num_physical_clients: num_physical,
                        start_virtual_clients: 100, // No need to test non-saturated regimes
                        virtual_clients_step: 10, // irrelevant
                        num_runs: 3,
                        fix_virtual_clients: Some(100),
                    })
                },
            )
            .await;
        }
    }

    println!("=== Calibration complete ===");
}
