use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
};
use hydro_optimize_examples::compare_and_swap::cas_write_bench::cas_write_bench;

use stageleft::q;

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

fn run_benchmark<'a>(num_clients: usize) -> BenchmarkConfig<'a> {
    let f = 1;
    let retry_timeout = 5000;
    let print_result_frequency = 1000;

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let replicas = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let location_id_to_cluster = HashMap::from([
        (replicas.id(), "replicas".to_string()),
        (clients.id(), "client".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
    ]);
    let client_id = clients.id();

    cas_write_bench(
        &replicas,
        f,
        &clients,
        clients.singleton(q!(1usize)),
        &client_aggregator,
        retry_timeout,
        print_result_frequency,
    );

    let clusters = ReusableClusters::default()
        .with_cluster(replicas, 2*f+1)
        .with_cluster(clients, num_clients);
    let processes = ReusableProcesses::default().with_process(client_aggregator);
    let optimizations = Optimizations::default();

    BenchmarkConfig {
        name: "CAS_Write".to_string(),
        builder,
        clusters,
        processes,
        client_id,
        optimizations,
        location_id_to_cluster,
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
