use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
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
}

/// Runs a single Paxos benchmark with the given parameters
fn run_benchmark<'a>(num_clients: usize) -> BenchmarkConfig<'a> {
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
        clients.singleton(q!(1usize)),
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
    let optimizations = Optimizations::default()
        .excluding(client_id.clone())
        .excluding(client_aggregator_id);

    BenchmarkConfig {
        name: "Paxos".to_string(),
        builder,
        clusters,
        processes,
        client_id,
        optimizations,
        location_id_to_cluster,
        scenario_binary: Some("paxos_optimizations".to_string()),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let num_runs = 1;
    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp,
            aws: args.aws,
        },
        num_runs,
        run_benchmark,
    )
    .await;
}
