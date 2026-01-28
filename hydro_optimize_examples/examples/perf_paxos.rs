use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, deploy_and_optimize,
};
use hydro_test::cluster::kv_replica::Replica;
use hydro_test::cluster::paxos::{Acceptor, CorePaxos, PaxosConfig, Proposer};
use hydro_test::cluster::paxos_bench::{Aggregator, Client};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct PerfPaxosArgs {
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
    let args = PerfPaxosArgs::parse();

    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let f = 1;
    let num_clients = 3;
    let num_clients_per_node = 500; // Change based on experiment between 1, 50, 100.
    let checkpoint_frequency = 1000; // Num log entries
    let i_am_leader_send_timeout = 5; // Sec
    let i_am_leader_check_timeout = 10; // Sec
    let i_am_leader_check_timeout_delay_multiplier = 15;

    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let replicas = builder.cluster();

    hydro_test::cluster::paxos_bench::paxos_bench(
        num_clients_per_node,
        checkpoint_frequency,
        f,
        f + 1,
        CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config: PaxosConfig {
                f,
                i_am_leader_send_timeout,
                i_am_leader_check_timeout,
                i_am_leader_check_timeout_delay_multiplier,
            },
        },
        &clients,
        &client_aggregator,
        &replicas,
    );

    // Deploy
    let mut reusable_hosts = ReusableHosts::new(host_type);
    let num_times_to_optimize = 2;
    let run_seconds = 30;

    deploy_and_optimize(
        &mut reusable_hosts,
        &mut deployment,
        builder.finalize(),
        ReusableClusters::new()
            .with_cluster(proposers, f + 1)
            .with_cluster(acceptors, 2 * f + 1)
            .with_cluster(clients, num_clients)
            .with_cluster(replicas, f + 1),
        ReusableProcesses::new().with_process(client_aggregator),
        Optimizations::new()
            .with_decoupling()
            .excluding::<Client>()
            .excluding::<Aggregator>()
            .with_iterations(num_times_to_optimize),
        Some(run_seconds),
    )
    .await;
}
