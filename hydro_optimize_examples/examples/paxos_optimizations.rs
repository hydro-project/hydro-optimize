use std::path::Path;

use clap::Parser;
use hydro_lang::compile::ir::deep_clone;
use hydro_lang::location::Location;
use hydro_lang::prelude::FlowBuilder;
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, apply_optimizations,
};
use hydro_optimize::parse_results::RunMetadata;
use hydro_optimize::repair::{inject_id, remove_counter};
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
use stageleft::q;

#[derive(Parser, Debug)]
struct Args {
    /// Path to profiling JSON file (from benchmark_protocol output)
    #[arg(long)]
    profiling: String,
}

fn main() {
    let args = Args::parse();

    let f = 1;
    let mut builder = FlowBuilder::new();
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let replicas = builder.cluster();

    let client_id = clients.id();
    let client_aggregator_id = client_aggregator.id();

    hydro_test::cluster::paxos_bench::paxos_bench(
        1000, // checkpoint_frequency
        f,
        f + 1,
        CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config: PaxosConfig {
                f,
                i_am_leader_send_timeout: 5,
                i_am_leader_check_timeout: 10,
                i_am_leader_check_timeout_delay_multiplier: 15,
            },
        },
        &clients,
        clients.singleton(q!(1usize)),
        &client_aggregator,
        &replicas,
        100,  // print_result_frequency / 10
        1000, // print_result_frequency
        |_| {}, // no-op bench results printer
    );

    let clusters = ReusableClusters::default()
        .with_cluster(proposers, f + 1)
        .with_cluster(acceptors, 2 * f + 1)
        .with_cluster(clients, 1)
        .with_cluster(replicas, f + 1);
    let processes = ReusableProcesses::default().with_process(client_aggregator);
    let optimizations = Optimizations::default()
        .with_decoupling()
        .excluding(client_id)
        .excluding(client_aggregator_id);

    let built = builder.optimize_with(|ir| {
        inject_id(ir);
    });

    let mut ir = deep_clone(built.ir());
    inject_id(&mut ir);
    remove_counter(&mut ir);

    // Print original IR
    println!("=== Original IR ===");
    hydro_lang::compile::ir::dbg_dedup_tee(|| {
        println!("{:#?}", ir);
    });

    // Load profiling data and inject counters
    let _profiling = RunMetadata::load_and_inject(Path::new(&args.profiling), &mut ir);

    // Apply optimizations
    let mut rewrite_builder = FlowBuilder::from_built(&built);
    rewrite_builder.replace_ir(deep_clone(&ir));
    apply_optimizations(
        &mut ir,
        &optimizations,
        &mut clusters.clone(),
        &processes,
        &mut rewrite_builder,
        0,
    );

    // Print rewritten IR
    println!("\n=== Rewritten IR ===");
    hydro_lang::compile::ir::dbg_dedup_tee(|| {
        println!("{:#?}", ir);
    });
}
