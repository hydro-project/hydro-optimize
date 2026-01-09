use std::cell::RefCell;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::deploy_and_analyze;
use hydro_test::cluster::kv_replica::Replica;
use hydro_test::cluster::paxos::{Acceptor, CorePaxos, PaxosConfig, Proposer};
use hydro_test::cluster::paxos_bench::{Aggregator, Client};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct BenchmarkArgs {
    #[command(flatten)]
    graph: hydro_lang::viz::config::GraphConfig,

    /// Use GCP for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use AWS, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

#[tokio::main]
async fn main() {
    let args = BenchmarkArgs::parse();

    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::GCP { project }
    } else if args.aws {
        HostType::AWS
    } else {
        HostType::Localhost
    };

    let mut reusable_hosts = ReusableHosts::new(host_type);

    let f = 1;
    let checkpoint_frequency = 1000; // Num log entries
    let i_am_leader_send_timeout = 5; // Sec
    let i_am_leader_check_timeout = 10; // Sec
    let i_am_leader_check_timeout_delay_multiplier = 15;

    // Benchmark parameters
    let num_clients = [1, 2];
    let num_clients_per_node = vec![1, 500, 1000, 2000, 3000];
    let run_seconds = 60;

    let multi_run_metadata = RefCell::new(vec![]);
    let mut iteration = 0;
    let max_num_clients_per_node = num_clients_per_node.iter().max().unwrap();
    for (i, num_clients) in num_clients.iter().enumerate() {
        // For the 1st client, test a variable number of virtual clients. For the rest, use the max number.
        let virtual_clients = if i == 0 {
            &num_clients_per_node
        } else {
            &vec![*max_num_clients_per_node]
        };

        for num_clients_per_node in virtual_clients {
            println!(
                "Running Paxos with {} clients and {} virtual clients per node for {} seconds",
                num_clients, num_clients_per_node, run_seconds
            );

            let builder = hydro_lang::compile::builder::FlowBuilder::new();
            let proposers = builder.cluster();
            let acceptors = builder.cluster();
            let clients = builder.cluster();
            let client_aggregator = builder.process();
            let replicas = builder.cluster();

            hydro_test::cluster::paxos_bench::paxos_bench(
                *num_clients_per_node,
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

            let clusters = vec![
                (
                    proposers.id().raw_id(),
                    std::any::type_name::<Proposer>().to_string(),
                    f + 1,
                ),
                (
                    acceptors.id().raw_id(),
                    std::any::type_name::<Acceptor>().to_string(),
                    2 * f + 1,
                ),
                (
                    clients.id().raw_id(),
                    std::any::type_name::<Client>().to_string(),
                    *num_clients,
                ),
                (
                    replicas.id().raw_id(),
                    std::any::type_name::<Replica>().to_string(),
                    f + 1,
                ),
            ];
            let processes = vec![(
                client_aggregator.id().raw_id(),
                std::any::type_name::<Aggregator>().to_string(),
            )];

            let (rewritten_ir_builder, ir, _, _, _) = deploy_and_analyze(
                &mut reusable_hosts,
                &mut deployment,
                builder.finalize(),
                &clusters,
                &processes,
                vec![],
                Some(run_seconds),
                &multi_run_metadata,
                iteration,
            )
            .await;

            // Cleanup and generate graphs if requested
            let built = rewritten_ir_builder.build_with(|_| ir).finalize();
            _ = built.generate_graph_with_config(&args.graph, None);

            iteration += 1;
        }
    }
}
