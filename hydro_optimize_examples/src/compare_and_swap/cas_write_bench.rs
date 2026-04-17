use hydro_lang::{
    live_collections::{sliced::sliced, stream::NoOrder},
    location::{Location, cluster::CLUSTER_SELF_ID},
    nondet::nondet,
    prelude::{Cluster, KeyedStream, Process, Unbounded},
};
use hydro_std::{
    bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency},
    membership::track_membership,
};
use serde::{Deserialize, Serialize};
use stageleft::q;

use crate::{
    compare_and_swap::{
        cas_distributed::{DistributedCAS, Replica},
        cas_like::{CASLike, CASState, UniqueRequestId},
    },
    print_parseable_bench_results,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Client;
pub struct Aggregator;

pub fn cas_write_bench<'a>(
    replicas: &Cluster<'a, Replica>,
    f: usize,
    clients: &Cluster<'a, Client>,
    client_aggregator: &Process<'a, Aggregator>,
    retry_timeout: u64,
    interval_millis: u64,
) {
    let cas = DistributedCAS {
        cluster: replicas,
        f,
        benchmark_mode: true,
        retry_timeout,
    };

    let latencies = bench_client(
        clients,
        clients.singleton(q!(1usize)),
        write_workload_generator,
        |input| {
            let payloads = input.values().into_keyed();

            let cas_output = cas.build(
                payloads,
                clients.source_iter(q!([])),
                clients.source_iter(q!([])),
                clients,
            );

            cas_output
                .write_processed
                .entries()
                .map(q!(|(request_id, successful)| (
                    0,
                    (request_id.id, successful)
                )))
                .into_keyed()
        },
    )
    .values()
    // NOTE: This only counts goodput
    .filter_map(q!(
        |((_request_id, successful), latency)| successful.then_some(latency)
    ));

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

/// Creates writes with unique IDs across clients.
/// Version number is set to 0; since all clients contact the same leader replica,
/// and writes from the same replica are allowed to reuse versions, all writes will succeed.
///
/// RequestId = UniqueRequestId
/// The State in CASState<State> = u64
fn write_workload_generator<'a, Client: 'a>(
    ids_and_prev_payloads: KeyedStream<
        u32,
        Option<(u64, bool)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, (UniqueRequestId, CASState<u64>), Cluster<'a, Client>, Unbounded, NoOrder> {
    let clients = ids_and_prev_payloads.location();
    let members = track_membership(clients.source_cluster_members(clients));

    let nondet_members = nondet!(/** Client membership does not change */);
    sliced! {
        let members = use(members, nondet_members);
        let ids_and_prev_payloads = use(ids_and_prev_payloads, nondet_members);

        let num_members = members
            .entries()
            .filter_map(q!(|(member, is_active)| is_active.then_some(member)))
            .count();
        ids_and_prev_payloads
            .cross_singleton(num_members)
            .map(q!(move |(payload, num_members)| {
                let request_id = if let Some((count, _successful)) = payload {
                    count + num_members as u64
                }
                else {
                    CLUSTER_SELF_ID.clone().get_raw_id() as u64
                };
                (
                    UniqueRequestId {
                        id: request_id,
                        is_generated: false,
                    },
                    CASState {
                        version: 0,
                        writer: 0, // Spoof all writes to use the same writer so ordering doesn't reduce goodput
                        state: request_id,
                    },
                )
            }))
    }
}
