use hydro_lang::{
    live_collections::{
        sliced::sliced,
        stream::{NoOrder, TotalOrder},
    },
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{Bounded, Cluster, Process, Singleton, Stream, TCP, Unbounded},
};
use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use hydro_test::cluster::paxos_bench::inc_i32_workload_generator;
use serde::{Deserialize, Serialize};
use stageleft::q;

use crate::{
    chain_replication::{Configuration, cas_like::CASLike, chain_replication},
    print_parseable_bench_results,
};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct Replica;
pub struct Client;
pub struct Aggregator;

#[expect(clippy::too_many_arguments, reason = "benchmark code")]
pub fn chain_replication_bench<'a>(
    replicas: &Cluster<'a, Replica>,
    num_replicas: usize,
    cas: impl CASLike<'a, Configuration<Replica>, MemberId<Replica>, Replica>,
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    client_aggregator: &Process<'a, Aggregator>,
    heartbeat_output_millis: u64,
    heartbeat_check_millis: u64,
    heartbeat_expired_millis: u64,
    interval_millis: u64,
) {
    let latencies = bench_client(
        clients,
        num_clients_per_node,
        inc_i32_workload_generator,
        |input| {
            let (payload_destination_complete, payload_destination) = clients.forward_ref::<Stream<
                MemberId<Replica>,
                Cluster<'a, Client>,
                Unbounded,
                TotalOrder,
            >>();
            let destination = payload_destination
                .inspect(q!(|new_acceptor| println!("Client learning of new acceptor: {:?}", new_acceptor)))
                .last();

            let nondet_send_payload =
                nondet!(/** Will send the paylod to whoever the leader is at the time */);
            let payloads = sliced! {
                let input = use(input.entries(), nondet_send_payload);
                let destination = use(destination, nondet_send_payload);
                let mut unsent_payloads = use::state_null::<Stream<_, _, _, NoOrder>>();

                let sending = unsent_payloads
                    .chain(input.clone())
                    .cross_singleton(destination.clone());
                unsent_payloads = input.filter_if_none(destination);
                sending
            };
            let sent_payloads = payloads
                .map(q!(|(payload, dest)| (dest, payload)))
                .demux(replicas, TCP.fail_stop().bincode())
                .entries();

            let (just_became_acceptor, output) = chain_replication(
                replicas,
                num_replicas,
                sent_payloads,
                heartbeat_output_millis,
                heartbeat_check_millis,
                heartbeat_expired_millis,
                cas,
            );

            let notify_clients_of_new_acceptor = just_became_acceptor
                .broadcast(
                    clients,
                    TCP.fail_stop().bincode(),
                    nondet!(/** Client membership does not change during benchmarking*/),
                )
                .keys()
            .assume_ordering(nondet!(/** Doesn't matter who we send to if there are multiple leader contenders */));
            payload_destination_complete.complete(notify_clients_of_new_acceptor);

            // NOTE: Even though the slot number is returned, since this benchmark doesn't include replicas, it is discarded
            output
                .into_keyed()
                .values()
                .demux(clients, TCP.fail_stop().bincode())
                .values()
                .into_keyed()
        },
    )
    .values()
    .map(q!(|(_value, latency)| latency));

    let bench_results = compute_throughput_latency(
        clients,
        latencies,
        interval_millis / 10,
        nondet!(/** bench */),
    );
    let aggregate_results =
        aggregate_bench_results(bench_results, client_aggregator, interval_millis);
    print_parseable_bench_results(aggregate_results);
}
