use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser};
use hydro_lang::location::Location;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    VIRTUAL_CLIENTS_STEP, benchmark_protocol,
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

    /// Disable counter instrumentation (for baseline runs)
    #[arg(long, action = ArgAction::SetTrue)]
    counters_only: bool,

    /// Insert byte-size inspection at network boundaries
    #[arg(long, action = ArgAction::SetTrue)]
    size_analysis: bool,

    /// Apply greedy decoupling, deploy decoupled system, gather per-operator SAR costs
    #[arg(long, action = ArgAction::SetTrue)]
    blow_up_analysis: bool,

    /// Path(s) to JSON file(s) containing `PossibleRewrite`s to apply before deploying.
    /// Rewrites are applied in the order given.
    #[arg(long = "rewrite")]
    rewrite: Vec<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let counters_only = args.counters_only;
    let size_analysis = args.size_analysis;
    let blow_up_analysis = args.blow_up_analysis;
    let rewrite_paths = args.rewrite.clone();

    let (_ir, _final_run_metadata) = benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp,
            aws: args.aws,
        },
        move |num_clients| {
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

            let mut optimizations = Optimizations::default()
                .excluding(client_id.clone())
                .excluding(client_aggregator_id);
            if counters_only {
                optimizations = optimizations.with_counters_only();
            }
            if size_analysis {
                optimizations = optimizations.with_size_analysis();
            }
            if blow_up_analysis {
                optimizations = optimizations.with_blow_up_analysis();
            }
            for p in &rewrite_paths {
                optimizations = optimizations.load_rewrite(Path::new(p));
            }

            BenchmarkConfig {
                name: "Paxos".to_string(),
                builder,
                clusters,
                processes,
                client_id,
                optimizations,
                location_id_to_cluster,
                start_virtual_clients: 1,
                virtual_clients_step: VIRTUAL_CLIENTS_STEP,
                num_runs: 1,
            }
        },
    )
    .await;
}
