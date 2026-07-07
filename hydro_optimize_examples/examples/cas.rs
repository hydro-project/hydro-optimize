use std::collections::HashMap;

use clap::{ArgAction, Parser};
use hydro_lang::{location::Location, prelude::FlowBuilder};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, CompiledProgram, NUM_PHYSICAL_CLIENTS, Optimization,
    ReusableClusters, ReusableProcesses, benchmark_protocol,
};
use hydro_optimize_examples::compare_and_swap::cas_bench::cas_bench;

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

    /// Optimize with a given latency budget
    #[arg(long)]
    latency: Option<usize>,
}

// const WRITE_RATIOS: &[usize] = &[0, 100];
const WRITE_RATIOS: &[usize] = &[100, 0];

/// Compiles the CAS program for a given write ratio. All write ratios produce an identical IR
/// (the ratio is a runtime constant inside the client), so they share one optimization run.
fn compile<'a>(write_ratio: usize) -> (FlowBuilder<'a>, CompiledProgram) {
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

    let program = CompiledProgram::new(clusters, processes, location_id_to_cluster)
        .excluding(client_id)
        .excluding(client_aggregator_id);

    (builder, program)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = BenchmarkConfig {
        name: "CAS".to_string(),
        kind: if args.optimize {
            if let Some(latency_budget) = args.latency {
                Optimization::OptimizeWithLatencyBudget(latency_budget)
            } else {
                Optimization::BottleneckElimination
            }
        } else {
            Optimization::None
        },
        num_physical_clients: NUM_PHYSICAL_CLIENTS,
        start_virtual_clients: 1,
        virtual_clients_step: 10,
        num_runs: 1,
        calibrate_message_sizes: None,
    };
    // (params, workload name) — the name discriminates output directories on disk.
    let workloads: Vec<(usize, String)> = WRITE_RATIOS
        .iter()
        .map(|&ratio| (ratio, format!("write{ratio}")))
        .collect();

    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp.clone(),
            aws: args.aws,
        },
        config,
        &workloads,
        |&write_ratio| compile(write_ratio),
    )
    .await;
}
