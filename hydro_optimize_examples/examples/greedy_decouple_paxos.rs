use std::collections::HashMap;
use std::path::Path;

use chrono::Local;
use clap::{ArgAction, Parser};
use hydro_lang::compile::ir::deep_clone;
use hydro_lang::location::Location;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses, benchmark_protocol
};
use hydro_optimize::greedy_decouple_analysis::greedy_decouple_analysis;
use hydro_optimize::repair::inject_id;
use hydro_optimize::rewrites::{Rewrite, RewriteMetadata, replay};
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
    #[command(flatten)]
    graph: GraphConfig,

    /// Use Gcp for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use Aws, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}


/// Runs a single Paxos benchmark with the given parameters
fn run_benchmark<'a>(num_clients: usize, num_virtual_clients_env: String) -> BenchmarkConfig<'a> {
    let f = 1;
    let checkpoint_frequency = 1000;
    let i_am_leader_send_timeout = 5;
    let i_am_leader_check_timeout = 10;
    let i_am_leader_check_timeout_delay_multiplier = 15;
    let print_result_frequency = 1000;

    println!(
        "Running Greedy Decouple Paxos with {} clients",
        num_clients,
    );

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let replicas = builder.cluster();

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
        clients.singleton(q!(std::env::var(NUM_CLIENTS_PER_NODE_ENV.to_string())
            .unwrap()
            .parse::<usize>()
            .unwrap())),
        &client_aggregator,
        &replicas,
        print_result_frequency / 10,
        print_result_frequency,
        print_parseable_bench_results,
    );

    // Decouple first
    let built = builder.optimize_with(|ir| {
        inject_id(ir);
    });
    let mut ir = deep_clone(built.ir());
    let decision = greedy_decouple_analysis(&mut ir, &proposers.id());
    let decouple = Rewrite::Decouple {
        decision,
        orig_location: proposers.id(),
    };
    let rewrite_metadata = RewriteMetadata {
        node: proposers.id(),
        num_nodes: f + 1,
        rewrite: decouple,
    };
    let (new_proposer_nodes, new_builder) = replay(vec![rewrite_metadata], built);
    let new_proposer_ids_with_name_num = new_proposer_nodes
        .iter()
        .enumerate()
        .map(|(i, (cluster, num))| (cluster.id(), format!("decouple-{}", i), *num))
        .collect::<Vec<_>>();

    let new_proposer_ids_with_names = new_proposer_ids_with_name_num 
        .iter()
        .map(|(id, name, _)| (id.clone(), name.clone()))
        .collect::<HashMap<LocationId, String>>();
    let mut location_id_to_cluster = HashMap::from([
        (proposers.id(), "proposer".to_string()),
        (acceptors.id(), "acceptor".to_string()),
        (clients.id(), "client".to_string()),
        (replicas.id(), "replica".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
    ]);
    location_id_to_cluster.extend(new_proposer_ids_with_names);
    
    let new_proposer_keys_with_name_num = new_proposer_ids_with_name_num 
        .into_iter()
        .map(|(id, name, num)| (id.key(), name, num))
        .collect();
    let clusters = ReusableClusters::from(new_proposer_keys_with_name_num)
        .with_cluster(proposers, f + 1)
        .with_cluster(acceptors, 2 * f + 1)
        .with_cluster(clients, num_clients)
        .with_cluster(replicas, f + 1);
    let processes = ReusableProcesses::default().with_process(client_aggregator);
    let optimizations = Optimizations::default();

    let output_dir = Path::new("benchmark_results").join(format!(
        "greedy_decouple_paxos_{}",
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));

    BenchmarkConfig {
        builder: new_builder,
        clusters,
        processes,
        optimizations,
        location_id_to_cluster,
        output_dir,
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    benchmark_protocol(
        BenchmarkArgs {
            gcp: args.gcp,
            aws: args.aws,
        },
        run_benchmark,
    )
    .await;
}
