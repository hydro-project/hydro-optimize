use hydro_lang::{prelude::{Cluster, Process}, nondet::nondet};
use hydro_std::bench_client::{bench_client, print_bench_results};

use hydro_test::cluster::paxos_bench::inc_u32_workload_generator;
use stageleft::q;
use crate::simple_graphs::{Client, GraphFunction, Server, map_h_map_h_parallel_no_union};
pub struct Aggregator;

pub fn simple_graphs_bench<'a>(
    num_clients_per_node: usize,
    server: &Cluster<'a, Server>,
    clients: &Cluster<'a, Client>,
    client_aggregator: &Process<'a, Aggregator>,
    graph: impl GraphFunction<'a>,
) {
    let bench_results = bench_client(
        clients,
        inc_u32_workload_generator,
        |payloads| {
            graph(
                server,
                payloads
                    .broadcast_bincode(server, nondet!(/** Test */))
                    .into(),
            )
            .demux_bincode(clients)
            .values()
        },
        num_clients_per_node,
        nondet!(/** bench */),
    );

    print_bench_results(bench_results, client_aggregator, clients);
}

pub fn simple_graphs_bench_no_union<'a>(
    num_clients_per_node: usize,
    server: &Cluster<'a, Server>,
    clients: &Cluster<'a, Client>,
    client_aggregator: &Process<'a, Aggregator>,
) {
    let bench_results = bench_client(
        clients,
        inc_u32_workload_generator,
        |payloads| {
            let payloads1 = payloads.clone().filter(q!(|(virt_client_id, _)| virt_client_id % 2 == 0));
            let payloads2 = payloads.filter(q!(|(virt_client_id, _)| virt_client_id % 2 == 1));
            let (batch0, batch1) = map_h_map_h_parallel_no_union(
                server,
                payloads1
                    .broadcast_bincode(server, nondet!(/** Test */))
                    .into(),
                payloads2
                    .broadcast_bincode(server, nondet!(/** Test */))
                    .into(),
            );
            let clients_batch0 = batch0
                .demux_bincode(clients)
                .values();
            batch1
                .demux_bincode(clients)
                .values()
                .interleave(clients_batch0)
        },
        num_clients_per_node,
        nondet!(/** bench */),
    );

    print_bench_results(bench_results, client_aggregator, clients);
}