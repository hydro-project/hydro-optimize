use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::{
    location::Location,
    prelude::{FlowBuilder, TCP},
};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
};
use hydro_optimize_examples::network_calibrator::{Client, Server};
use stageleft::q;

const CALIBRATION_SIZES: &[usize] = &[64, 128, 256, 512, 1024, 2048, 4096];

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

    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp.clone(),
            aws: args.aws,
        },
        move || {
            let mut builder = FlowBuilder::new();
            let server = builder.process::<Server>();
            let clients = builder.cluster::<Client>();
            let client_id = clients.id();

            let location_id_to_cluster = HashMap::from([
                (server.id(), "server".to_string()),
                (client_id.clone(), "client".to_string()),
            ]);

            // Dummy code. Just need to make sure that the server receives and sends.
            // The actual message generation is done by the Hydro IR.
            // Use a cluster of clients (not process) since that has higher overhead
            clients
                .source_iter(q!(vec![Vec::<u8>::new()]))
                .send(&server, TCP.fail_stop().bincode())
                .demux(&clients, TCP.fail_stop().bincode());

            let clusters = ReusableClusters::default().with_cluster(clients, 1);
            let processes = ReusableProcesses::default().with_process(server);
            let optimizations = Optimizations::default()
                .excluding(client_id);

            (
                builder,
                BenchmarkConfig {
                    name: "Network".to_string(),
                    workload: "default".to_string(),
                    clusters,
                    processes,
                    optimizations,
                    location_id_to_cluster,
                    num_physical_clients: 1,
                    start_virtual_clients: 1,
                    virtual_clients_step: 1,
                    num_runs: 1,
                    calibrate_message_sizes: Some(CALIBRATION_SIZES.to_vec()),
                },
            )
        },
    )
    .await;

    println!("=== Calibration complete ===");
}
