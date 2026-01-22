use hydro_lang::{
    live_collections::stream::NoOrder,
    nondet::nondet,
    prelude::{Cluster, Process, Stream, TCP, Unbounded},
};
use hydro_std::bench_client::{bench_client, print_bench_results};

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
) {
    let bench_results = bench_client(
        clients,
        |_client, payload_request| size_based_workload_generator(message_size, payload_request),
        |payloads| {
            // Server just echoes the payload
            payloads
                .broadcast(server, TCP.bincode(), nondet!(/** Test */))
                .demux(clients, TCP.bincode())
                .values()
        },
        num_clients_per_node,
        nondet!(/** bench */),
    );

    print_bench_results(bench_results, client_aggregator, clients);
}

/// Generates an incrementing u32 for each virtual client ID, starting at 0
pub fn size_based_workload_generator<'a, Client>(
    message_size: usize,
    payload_request: Stream<(u32, Option<Vec<u8>>), Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<(u32, Vec<u8>), Cluster<'a, Client>, Unbounded, NoOrder> {
    payload_request.map(q!(move |(virtual_id, payload)| {
        if let Some(mut payload) = payload {
            if let Some(last) = payload.last_mut() {
                *last += 1;
                return (virtual_id, payload);
            }
        }

        // Temp fix for macro stuff that isn't supported by stageleft I guess
        let msg_size = message_size;
        (virtual_id, vec![0; msg_size])
    }))
}
