use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol_with_reusable_machines, VIRTUAL_CLIENTS_MAX,
};
use hydro_optimize_examples::network_calibrator::network_calibrator;

use stageleft::q;

const CALIBRATION_SIZES: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];

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

    let mut deployment = Deployment::new();
    let host_type = if let Some(project) = args.gcp.clone() {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };
    let mut reusable_hosts = ReusableHosts::new(&host_type);

    for &size in CALIBRATION_SIZES {
        println!("=== Calibrating message_size={} ===", size);

        benchmark_protocol_with_reusable_machines(
            &mut reusable_hosts,
            &mut deployment,
            move |num_clients| {
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
                    size,
                    &server,
                    &clients,
                    clients.singleton(q!(1usize)),
                    &client_aggregator,
                    1000,
                );

                let clusters = ReusableClusters::default()
                    .with_cluster(server, 1)
                    .with_cluster(clients, num_clients);
                let processes = ReusableProcesses::default().with_process(client_aggregator);
                let optimizations = Optimizations::default()
                    .excluding(client_id.clone())
                    .excluding(aggregator_id);

                BenchmarkConfig {
                    name: format!("NetworkCalibration_{}b", size),
                    builder,
                    clusters,
                    processes,
                    client_id,
                    optimizations,
                    location_id_to_cluster,
                    start_virtual_clients: VIRTUAL_CLIENTS_MAX,
                    num_runs: 1,
                }
            },
        )
        .await;
    }

    println!("=== Calibration complete ===");
}
