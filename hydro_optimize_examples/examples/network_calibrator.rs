use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::prelude::FlowBuilder;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, deploy_and_optimize,
};
use hydro_optimize_examples::network_calibrator::{Aggregator, Client, network_calibrator};

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
    let interval_millis = 1000;

    for message_size in message_sizes {
        let mut builder = FlowBuilder::new();
        let server = builder.cluster();
        let clients = builder.cluster();
        let client_aggregator = builder.process();

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
            interval_millis,
        );

        deploy_and_optimize(
            &mut reusable_hosts,
            &mut deployment,
            builder.finalize(),
            ReusableClusters::default()
                .with_cluster(server, 1)
                .with_cluster(clients, num_clients),
            ReusableProcesses::default().with_process(client_aggregator),
            Optimizations::default()
                .excluding::<Client>()
                .excluding::<Aggregator>(),
            num_seconds_to_profile,
            None,
        )
        .await;
    }
}
