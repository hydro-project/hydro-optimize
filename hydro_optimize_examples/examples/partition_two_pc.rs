
use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, deploy_and_optimize,
};
use hydro_test::cluster::two_pc_bench::{Aggregator, Client};

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

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let num_participants = 3;
    let num_clients = 3;
    let num_clients_per_node = 100;
    let print_result_frequency = 1000; // Millis

    let coordinator = builder.process();
    let participants = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();

    hydro_test::cluster::two_pc_bench::two_pc_bench(
        num_clients_per_node,
        &coordinator,
        &participants,
        num_participants,
        &clients,
        &client_aggregator,
        print_result_frequency,
        print_parseable_bench_results,
    );

    deploy_and_optimize(
        &mut reusable_hosts,
        &mut deployment,
        builder.finalize(),
        ReusableClusters::new()
            .with_cluster(participants, num_participants)
            .with_cluster(clients, num_clients),
        ReusableProcesses::new()
            .with_process(coordinator)
            .with_process(client_aggregator),
        Optimizations::new()
            .with_partitioning()
            .excluding::<Client>()
            .excluding::<Aggregator>(),
        None,
    )
    .await;
}
