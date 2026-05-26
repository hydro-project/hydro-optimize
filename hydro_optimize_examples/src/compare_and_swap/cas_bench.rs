use hydro_lang::{
    location::Location,
    nondet::nondet,
    prelude::{Cluster, Process},
};
use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use rand::RngExt;
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

pub fn cas_bench<'a>(
    replicas: &Cluster<'a, Replica>,
    f: usize,
    write_ratio: usize,
    clients: &Cluster<'a, Client>,
    client_aggregator: &Process<'a, Aggregator>,
    retry_timeout: u64,
    interval_millis: u64,
) {
    assert!(write_ratio <= 100, "Cannot exceed 100% writes");

    let cas = DistributedCAS {
        cluster: replicas,
        f,
        benchmark_mode: true,
        retry_timeout,
    };

    let latencies = bench_client(
        clients,
        clients.singleton(q!({
            std::env::var("NUM_VIRTUAL_CLIENTS")
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })),
        |payloads| payloads,
        |input| {
            let request_type = input.entries().map(q!(|(virtual_client_id, _)| {
                let rand_type = rand::rng().random_range(0..100);
                let mut request_id = UniqueRequestId::generate();
                request_id.virtual_client_id = virtual_client_id;
                request_id.is_generated = false;
                (request_id, rand_type)
            }));
            let (should_be_write, should_be_read) =
                request_type.partition(q!(move |(_request_id, r)| *r < write_ratio));

            let writes = should_be_write
                .map(q!(move |(request_id, _payload)| {
                    (
                        request_id,
                        CASState {
                            version: 0,
                            writer: 0, // Spoof all writes to use the same writer so ordering doesn't reduce goodput
                            state: request_id.id,
                        },
                    )
                }))
                .into_keyed();
            let reads = should_be_read.map(q!(move |(request_id, _)| request_id));

            let cas_output = cas.build(writes, reads, clients.source_iter(q!([])), clients);

            let write_results =
                cas_output
                    .write_processed
                    .entries()
                    .map(q!(|(request_id, successful)| (
                        request_id.virtual_client_id,
                        successful
                    )));
            let read_results = cas_output
                .read_result
                .entries()
                .map(q!(|(request_id, _result)| (
                    request_id.virtual_client_id,
                    true
                )));
            write_results.merge_unordered(read_results).into_keyed()
        },
    )
    .values()
    // NOTE: This only counts goodput
    .filter_map(q!(|(successful, latency)| successful.then_some(latency)));

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
