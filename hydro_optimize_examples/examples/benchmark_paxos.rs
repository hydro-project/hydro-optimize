use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, deploy_and_optimize,
};
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct BenchmarkArgs {
    #[command(flatten)]
    graph: hydro_lang::viz::config::GraphConfig,

    /// Use Gcp for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use Aws, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

#[tokio::main]
async fn main() {
    let args = BenchmarkArgs::parse();

    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };

    let mut reusable_hosts = ReusableHosts::new(host_type);

    let f = 1;
    let checkpoint_frequency = 1000; // Num log entries
    let i_am_leader_send_timeout = 5; // Sec
    let i_am_leader_check_timeout = 10; // Sec
    let i_am_leader_check_timeout_delay_multiplier = 15;
    let print_result_frequency = 1000; // Millis

    // Benchmark parameters
    let num_clients = [1, 2, 3, 4, 5];
    let num_clients_per_node = vec![1, 50, 100];
    let run_seconds = 30;

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

            let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
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
                print_result_frequency,
                hydro_std::bench_client::pretty_print_bench_results,
            );

            let run_metadata = deploy_and_optimize(
                &mut reusable_hosts,
                &mut deployment,
                builder.finalize(),
                ReusableClusters::new()
                    .with_cluster(proposers, f + 1)
                    .with_cluster(acceptors, 2 * f + 1)
                    .with_cluster(clients, *num_clients)
                    .with_cluster(replicas, f + 1),
                ReusableProcesses::new().with_process(client_aggregator),
                Optimizations::new(),
                Some(run_seconds),
            )
            .await;
        }
    }
}
