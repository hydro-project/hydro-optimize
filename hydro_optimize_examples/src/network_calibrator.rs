use hydro_lang::{
    live_collections::stream::NoOrder,
    nondet::nondet,
    prelude::{Cluster, KeyedStream, Process, TCP, Unbounded},
};
use hydro_optimize::parse_results::print_parseable_bench_results;
use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};

use stageleft::q;

pub struct Client;
pub struct Server;
pub struct Aggregator;

pub fn network_calibrator<'a>(
    num_clients_per_node: usize,
    message_size: usize,
    server: &Cluster<'a, Server>,
    clients: &Cluster<'a, Client>,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
) {
    let latencies = bench_client(
        clients,
        num_clients_per_node,
        |ids_and_prev_payloads| {
            size_based_workload_generator(message_size, ids_and_prev_payloads)
        },
        |payloads| {
            // Server just echoes the payload
            payloads
                .entries()
                .broadcast(server, TCP.bincode(), nondet!(/** Test */))
                .demux(clients, TCP.bincode())
                .values()
                .into_keyed()
        },
    ).values().map(q!(|(_client_id, latency)| latency));

    let bench_results = compute_throughput_latency(clients, latencies, nondet!(/** bench */));
    let aggregate_results = aggregate_bench_results(bench_results, client_aggregator, clients, interval_millis);
    print_parseable_bench_results(aggregate_results, interval_millis);
}

/// Generates an incrementing u32 for each virtual client ID, starting at 0
pub fn size_based_workload_generator<'a, Client>(
    message_size: usize,
    ids_and_prev_payloads: KeyedStream<
        u32,
        Option<Vec<u8>>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Vec<u8>, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev_payloads.map(q!(move |payload| {
        if let Some(mut payload) = payload {
            if let Some(last) = payload.last_mut() {
                *last += 1;
                return payload;
            }
        }

        // Temp fix for macro stuff that isn't supported by stageleft I guess
        let msg_size = message_size;
        vec![0; msg_size]
    }))
}
