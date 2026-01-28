use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use hydro_deploy::Deployment;
use hydro_deploy::gcp::GcpNetwork;
use hydro_lang::location::Location;
use hydro_lang::prelude::FlowBuilder;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy::ReusableHosts;
use hydro_optimize::deploy_and_analyze::deploy_and_optimize;
use hydro_optimize_examples::network_calibrator::{Aggregator, Client, Server, network_calibrator};
use tokio::sync::RwLock;

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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };
    let mut reusable_hosts = ReusableHosts::new(host_type);

    let num_clients = 10; // >1 clients so it doesn't become the bottleneck
    let num_clients_per_node = 1000;
    let message_sizes = vec![1, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];
    let num_seconds_to_profile = Some(60);
    let multi_run_metadata = RefCell::new(vec![]);

    for message_size in message_sizes {
        let builder = FlowBuilder::new();
        let server = builder.cluster();
        let clients = builder.cluster();
        let client_aggregator = builder.process();

        let clusters = vec![
            (
                server.id().key(),
                std::any::type_name::<Server>().to_string(),
                1,
            ),
            (
                clients.id().key(),
                std::any::type_name::<Client>().to_string(),
                num_clients,
            ),
        ];
        let processes = vec![(
            client_aggregator.id().key(),
            std::any::type_name::<Aggregator>().to_string(),
        )];

        println!(
            "Running network calibrator with message size: {} bytes, num clients: {}",
            message_size, num_clients
        );
        network_calibrator(
            num_clients_per_node,
            message_size,
            &server,
            &clients,
            &client_aggregator,
        );

        let (rewritten_ir_builder, ir, _, _, _) = deploy_and_optimize(
            &mut reusable_hosts,
            &mut deployment,
            builder.finalize(),
            &clusters,
            &processes,
            vec![
                std::any::type_name::<Client>().to_string(),
                std::any::type_name::<Aggregator>().to_string(),
            ],
            num_seconds_to_profile,
            &multi_run_metadata,
            0, // Set to 0 to turn off comparisons between iterations
        )
        .await;

        let built = rewritten_ir_builder.build_with(|_| ir).finalize();

        // Generate graphs if requested
        _ = built.generate_graph_with_config(&args.graph, None);
    }
}
