use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, NUM_PHYSICAL_CLIENTS, Optimizations, ReusableClusters,
    ReusableProcesses, benchmark_protocol,
};
use hydro_optimize_examples::print_parseable_bench_results;
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
use stageleft::q;

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

    /// Run ILP-based bottleneck elimination (auto-runs missing analyses)
    #[arg(long, action = ArgAction::SetTrue)]
    optimize: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let optimize = args.optimize;

    let (_ir, _final_run_metadata) = benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp,
            aws: args.aws,
        },
        move || {
            let num_clients = NUM_PHYSICAL_CLIENTS;
            let f = 1;
            let checkpoint_frequency = 1000;
            let i_am_leader_send_timeout = 5;
            let i_am_leader_check_timeout = 10;
            let i_am_leader_check_timeout_delay_multiplier = 15;
            let print_result_frequency = 1000;

            let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
            let proposers = builder.cluster();
            let acceptors = builder.cluster();
            let clients = builder.cluster();
            let client_aggregator = builder.process();
            let replicas = builder.cluster();
            let location_id_to_cluster = HashMap::from([
                (proposers.id(), "proposer".to_string()),
                (acceptors.id(), "acceptor".to_string()),
                (clients.id(), "client".to_string()),
                (replicas.id(), "replica".to_string()),
                (client_aggregator.id(), "client_aggregator".to_string()),
            ]);
            let client_id = clients.id();
            let client_aggregator_id = client_aggregator.id();

            hydro_test::cluster::paxos_bench::paxos_bench(
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
                clients.singleton(q!({
                    std::env::var("NUM_VIRTUAL_CLIENTS")
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                })),
                &client_aggregator,
                &replicas,
                print_result_frequency / 10,
                print_result_frequency,
                print_parseable_bench_results,
            );

            let clusters = ReusableClusters::default()
                .with_cluster(proposers, f + 1)
                .with_cluster(acceptors, 2 * f + 1)
                .with_cluster(clients, num_clients)
                .with_cluster(replicas, f + 1);
            let processes = ReusableProcesses::default().with_process(client_aggregator);

            let mut optimizations = Optimizations::default()
                .excluding(client_id.clone())
                .excluding(client_aggregator_id);
            if optimize {
                optimizations = optimizations.with_bottleneck_elimination();
            }

            (builder, BenchmarkConfig {
                name: "Paxos".to_string(),
                workload: "default".to_string(),
                clusters,
                processes,
                optimizations,
                location_id_to_cluster,
                num_physical_clients: num_clients,
                start_virtual_clients: 1,
                virtual_clients_step: 10,
                num_runs: 1,
                fix_virtual_clients: None,
            })
        },
    )
    .await;
}
