use ecies::{PublicKey, utils::generate_keypair};
use hydro_lang::{
    live_collections::stream::{NoOrder, TotalOrder},
    location::MemberId,
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Process, Singleton, TCP, Unbounded},
};
use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use stageleft::q;

use crate::print_parseable_bench_results;

pub struct Server;
pub struct Client;
pub struct Aggregator;

pub fn encrypt_bench<'a>(
    server: &Cluster<'a, Server>,
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
) {
    let (sk, pk) = generate_keypair();
    let sk_hex = sk
        .serialize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let sk_hex: &'static str = Box::leak(sk_hex.into_boxed_str());
    let pk_hex = pk
        .serialize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let pk_hex: &'static str = Box::leak(pk_hex.into_boxed_str());

    let latencies = bench_client(
        clients,
        num_clients_per_node,
        |payloads| encrypted_workload_generator(payloads, pk),
        |input| {
            let sent_inputs = input
                .entries()
                .map(q!(|payload| (MemberId::<Server>::from_raw_id(0), payload)))
                .demux(server, TCP.fail_stop().bincode());

            let outputs = sent_inputs
                .flat_map_unordered(q!(|(client_id, payload)| {
                    let sk_bytes = sk_hex
                        .as_bytes()
                        .chunks_exact(2)
                        .map(|digits| {
                            let high = (digits[0] as char).to_digit(16).unwrap();
                            let low = (digits[1] as char).to_digit(16).unwrap();
                            ((high << 4) | low) as u8
                        })
                        .collect::<Vec<_>>();

                    let payload = ecies::decrypt(&sk_bytes, &payload).unwrap();
                    vec![(client_id, 0u8, payload.clone()), (client_id, 1u8, payload)]
                }))
                .assume_ordering::<TotalOrder>(nondet!(
                    /** Processing order is intentionally nondeterministic for this benchmark. */
                ))
                .scan(
                    q!(|| 0usize),
                    q!(|count, (client_id, copy_id, payload)| {
                        let mut payload_bytes = [0u8; std::mem::size_of::<usize>()];
                        payload_bytes.copy_from_slice(&payload[..std::mem::size_of::<usize>()]);

                        let payload = usize::from_ne_bytes(payload_bytes) + *count;
                        *count += 1;

                        Some((client_id, copy_id, payload.to_ne_bytes().to_vec()))
                    }),
                )
                .filter_map(q!(|(client_id, copy_id, payload)| {
                    if copy_id != 0 {
                        None
                    } else {
                        let pk_bytes = pk_hex
                            .as_bytes()
                            .chunks_exact(2)
                            .map(|digits| {
                                let high = (digits[0] as char).to_digit(16).unwrap();
                                let low = (digits[1] as char).to_digit(16).unwrap();
                                ((high << 4) | low) as u8
                            })
                            .collect::<Vec<_>>();

                        Some((client_id, ecies::encrypt(&pk_bytes, &payload).unwrap()))
                    }
                }));

            // Send committed requests back to the original client
            outputs
                .demux(clients, TCP.fail_stop().bincode())
                .entries()
                .map(q!(|(_server_id, (client_id, payload))| {
                    (client_id, payload)
                }))
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

pub fn encrypted_workload_generator<'a, Client>(
    ids_and_prev_payloads: KeyedStream<
        u32,
        Option<Vec<u8>>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
    pk: PublicKey,
) -> KeyedStream<u32, Vec<u8>, Cluster<'a, Client>, Unbounded, NoOrder> {
    let pk_hex = pk
        .serialize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let pk_hex: &'static str = Box::leak(pk_hex.into_boxed_str());

    ids_and_prev_payloads.map(q!(move |payload| {
        if let Some(p) = payload {
            p
        } else {
            let zero: usize = 0;
            let pk_bytes = pk_hex
                .as_bytes()
                .chunks_exact(2)
                .map(|digits| {
                    let high = (digits[0] as char).to_digit(16).unwrap();
                    let low = (digits[1] as char).to_digit(16).unwrap();
                    ((high << 4) | low) as u8
                })
                .collect::<Vec<_>>();

            ecies::encrypt(&pk_bytes, &zero.to_ne_bytes()).unwrap()
        }
    }))
}
