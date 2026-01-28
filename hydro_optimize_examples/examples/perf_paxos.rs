use std::cell::RefCell;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::decoupler;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{Optimizations, deploy_and_optimize};
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

    let mut clusters = vec![
        (
            proposers.id().key(),
            std::any::type_name::<Proposer>().to_string(),
            f + 1,
        ),
        (
            acceptors.id().key(),
            std::any::type_name::<Acceptor>().to_string(),
            2 * f + 1,
        ),
        (
            clients.id().key(),
            std::any::type_name::<Client>().to_string(),
            num_clients,
        ),
        (
            replicas.id().key(),
            std::any::type_name::<Replica>().to_string(),
            f + 1,
        ),
    ];
    let processes = vec![(
        client_aggregator.id().key(),
        std::any::type_name::<Aggregator>().to_string(),
    )];

    // Deploy
    let mut reusable_hosts = ReusableHosts::new(host_type);

    let multi_run_metadata = RefCell::new(vec![]);
    let num_times_to_optimize = 2;

    for i in 0..num_times_to_optimize {
        let new_builder = deploy_and_optimize(
            &mut reusable_hosts,
            &mut deployment,
            builder.finalize(),
            &mut clusters,
            &processes,
            vec![
                std::any::type_name::<Client>().to_string(),
                std::any::type_name::<Aggregator>().to_string(),
            ],
         Optimizations::new().with_decoupling(),
            None,
            &multi_run_metadata,
            i,
        )
        .await;

        builder = new_builder;
    }

    let built = builder.finalize();

    // Generate graphs if requested
    _ = built.generate_graph_with_config(&args.graph, None);
}
