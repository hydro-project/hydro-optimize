use hydro_lang::{
    live_collections::stream::NoOrder,
    location::{Location, cluster::CLUSTER_SELF_ID},
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Process, Singleton, Unbounded},
};
use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use rand::random;
use serde::{Deserialize, Serialize};
use stageleft::q;

use crate::{
    compare_and_swap::{
        cas_distributed::{DistributedCAS, Replica, Uuid},
        cas_like::{CASLike, CASState},
    },
    print_parseable_bench_results,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Client;
pub struct Aggregator;

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct UniqueRequestId {
    id: u64,
    is_generated: bool,
}

impl Uuid for UniqueRequestId {
    fn generate() -> Self {
        Self {
            id: random(),
            is_generated: true,
        }
    }
}

pub fn cas_write_bench<'a>(
    replicas: &Cluster<'a, Replica>,
    f: usize,
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
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
        num_clients_per_node.clone(),
        write_workload_generator,
        |input| {
            let payloads = input.values().into_keyed();

            let cas_output = cas.build(
                payloads,
                clients.source_iter(q!([])),
                clients.source_iter(q!([])),
                clients,
            );

            // TODO: Only consider goodput? Mark writes rejected differently?
            cas_output
                .write_processed
                .cross_singleton(num_clients_per_node)
                .map(q!(|(request_id, num_virtual_clients)| (
                    (request_id.id % num_virtual_clients as u64) as u32,
                    request_id.id
                )))
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

/// Creates writes with unique IDs across clients.
/// Version number is set to 0; since all clients contact the same leader replica,
/// and writes from the same replica are allowed to reuse versions, all writes will succeed.
///
/// RequestId = UniqueRequestId
/// The State in CASState<State> = u64
/// TODO: Actually, CAS detects the sender's identity to decide how to handle versions, so multiple clients won't help. We need to spoof the writer's identity
fn write_workload_generator<'a, Client: 'a>(
    ids_and_prev_payloads: KeyedStream<u32, Option<u64>, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, (UniqueRequestId, CASState<u64>), Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev_payloads.map(q!(move |payload| {
        let id = CLUSTER_SELF_ID.get_raw_id() as u64;
        let request_id = if let Some(counter) = payload {
            counter + id
        } else {
            id
        };
        (
            UniqueRequestId {
                id: request_id,
                is_generated: false,
            },
            CASState {
                version: 0,
                state: request_id,
            },
        )
    }))
}
