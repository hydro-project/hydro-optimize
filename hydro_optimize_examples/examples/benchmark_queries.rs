use hydro_lang::location::{Location, MemberId};
use hydro_optimize::deploy_and_analyze::{
    BenchmarkArgs, BenchmarkConfig, Optimizations, ReusableClusters, ReusableProcesses,
    benchmark_protocol,
};
use hydro_optimize_examples::flink_queries::{
    flink_queries::{Bid, Queries, q1},
    flink_queries_bench::{Client, queries_bench_single},
    flink_workload_generators::bid_workload_generator,
};
use stageleft::q;
use std::collections::HashMap;

/// Runs a single Query benchmark with the given parameters
fn run_benchmark<'a>(num_clients: usize) -> BenchmarkConfig<'a> {
    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let query_sys: hydro_lang::prelude::Process<'_, Queries> = builder.process();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let interval_millis = 1000;
    let location_id_to_cluster = HashMap::from([
        (query_sys.id(), "query_sys".to_string()),
        (clients.id(), "client".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
    ]);
    let client_id = clients.id();

    // Output Type depends on which query you're running
    queries_bench_single::<Bid, Bid>(
        &query_sys,
        &clients,
        clients.singleton(q!(1usize)),
        &client_aggregator,
        interval_millis,
        // Replace query below
        q1::<(MemberId<Client>, u32)>,
        bid_workload_generator,
    );

    let clusters = ReusableClusters::default().with_cluster(clients, num_clients);
    let processes = ReusableProcesses::default().with_process(client_aggregator);
    let optimizations = Optimizations::default();

    BenchmarkConfig {
        name: "Queries".to_string(),
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
    let num_runs = 1;
    benchmark_protocol(
        BenchmarkArgs {
            gcp: None,
            aws: false,
        },
        num_runs,
        run_benchmark,
    )
    .await;
}
