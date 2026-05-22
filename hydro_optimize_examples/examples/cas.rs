use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::viz::config::GraphConfig;
use hydro_lang::{location::Location, prelude::FlowBuilder};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, NUM_PHYSICAL_CLIENTS, Optimizations, ReusableClusters,
    ReusableProcesses, VIRTUAL_CLIENTS_STEP, benchmark_protocol,
};
use hydro_optimize_examples::compare_and_swap::cas_bench::cas_bench;

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

    /// Run ILP-based bottleneck elimination (auto-runs missing analyses)
    #[arg(long, action = ArgAction::SetTrue)]
    optimize: bool,
}

const WRITE_RATIOS: &[usize] = &[10, 100];

fn run_benchmark<'a>(write_ratio: usize, optimize: bool) -> (FlowBuilder<'a>, BenchmarkConfig) {
    let f = 1;
    let retry_timeout = 5000;
    let print_result_frequency = 1000;
    let num_clients = NUM_PHYSICAL_CLIENTS;

    let mut builder = FlowBuilder::new();
    let replicas = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let location_id_to_cluster = HashMap::from([
        (replicas.id(), "replicas".to_string()),
        (clients.id(), "client".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
    ]);
    let client_id = clients.id();
    let client_aggregator_id = client_aggregator.id();

    cas_bench(
        &replicas,
        f,
        write_ratio,
        &clients,
        &client_aggregator,
        retry_timeout,
        print_result_frequency,
    );

    let clusters = ReusableClusters::default()
        .with_cluster(replicas, 2 * f + 1)
        .with_cluster(clients, num_clients);
    let processes = ReusableProcesses::default().with_process(client_aggregator);

    let mut optimizations = Optimizations::default()
        .excluding(client_id)
        .excluding(client_aggregator_id);
    if optimize {
        optimizations = optimizations.with_bottleneck_elimination();
    }

    (
        builder,
        BenchmarkConfig {
            name: "CAS".to_string(),
            workload: format!("write{write_ratio}"),
            clusters,
            processes,
            optimizations,
            location_id_to_cluster,
            num_physical_clients: num_clients,
            start_virtual_clients: 1,
            virtual_clients_step: VIRTUAL_CLIENTS_STEP,
            num_runs: 1,
            calibrate_message_size: None,
        },
    )
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    for &write_ratio in WRITE_RATIOS {
        benchmark_protocol(
            BenchmarkArgs {
                gcp: args.gcp.clone(),
                aws: args.aws,
            },
            || run_benchmark(write_ratio, args.optimize),
        )
        .await;
    }
}
