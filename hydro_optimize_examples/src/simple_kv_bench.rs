use hydro_lang::{
    location::Location,
    nondet::nondet,
    prelude::{Cluster, Process, TCP},
};
use hydro_std::bench_client::{bench_client, compute_throughput_latency, print_bench_results};

use hydro_test::cluster::paxos_bench::inc_i32_workload_generator;
use stageleft::q;

pub struct Kv;
pub struct Client;
pub struct Aggregator;

pub fn simple_kv_bench<'a>(
    num_clients_per_node: usize,
    kv: &Process<'a, Kv>,
    clients: &Cluster<'a, Client>,
    client_aggregator: &Process<'a, Aggregator>,
) {
    let latencies = bench_client(clients, num_clients_per_node, inc_i32_workload_generator, |input| {
        let k_tick = kv.tick();
        // Use atomic to prevent outputting to the client before values are inserted to the KV store
        let k_payloads = input.send(kv, TCP.bincode()).atomic(&k_tick);

        let for_each_tick = kv.tick();
        // Insert each payload into the KV store
        k_payloads
            .clone()
            .assume_ordering(nondet!(/** Last writer wins per key. */))
            // Persist state across ticks
            .reduce(q!(|prev, new| {
                *prev = new;
            }))
            .end_atomic()
            .snapshot(
                &for_each_tick,
                nondet!(/** for_each does nothing, just need to end on a HydroRoot */),
            )
            .entries()
            .all_ticks()
            .assume_ordering(nondet!(/** for_each does nothing, just need to end on a HydroRoot */))
            .assume_retries(nondet!(/** for_each does nothing, just need to end on a HydroRoot */))
            .for_each(q!(|_| {})); // Do nothing, just need to end on a HydroRoot

        // Send committed requests back to the original client
        k_payloads
            .end_atomic()
            .demux(clients, TCP.bincode())
    }).values().map(q!(|(_value, latency)| latency));

    let bench_results = compute_throughput_latency(clients, latencies, nondet!(/** bench */));
    print_bench_results(bench_results, client_aggregator, clients);
}

#[cfg(test)]
mod tests {
    use dfir_lang::graph::WriteConfig;
    use hydro_build_utils::insta;
    use hydro_deploy::Deployment;
    use hydro_lang::{
        compile::ir::dbg_dedup_tee,
        deploy::{DeployCrateWrapper, HydroDeploy, TrybuildHost},
        prelude::FlowBuilder,
    };
    use std::str::FromStr;

    use regex::Regex;

    #[cfg(stageleft_runtime)]
    use crate::simple_kv_bench::simple_kv_bench;

    #[test]
    fn simple_kv_ir() {
        let mut builder = FlowBuilder::new();
        let kv = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();

        simple_kv_bench(1, &kv, &clients, &client_aggregator);
        let mut built = builder.with_default_optimize::<HydroDeploy>();

        dbg_dedup_tee(|| {
            insta::assert_debug_snapshot!(built.ir());
        });

        let preview = built.preview_compile();
        insta::with_settings!({snapshot_suffix => "kv_mermaid"}, {
            insta::assert_snapshot!(
                preview.dfir_for(&kv).to_mermaid(&WriteConfig {
                    no_subgraphs: true,
                    no_pull_push: true,
                    no_handoffs: true,
                    op_text_no_imports: true,
                    ..WriteConfig::default()
                })
            );
        });
    }

    #[tokio::test]
    async fn simple_kv_some_throughput() {
        let mut builder = FlowBuilder::new();
        let kv = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();

        simple_kv_bench(1, &kv, &clients, &client_aggregator);
        let mut deployment = Deployment::new();

        let nodes = builder
            .with_process(&kv, TrybuildHost::new(deployment.Localhost()))
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let client_node = &nodes.get_process(&client_aggregator);
        let client_out = client_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        let re = Regex::new(r"Throughput: ([^ ]+) - ([^ ]+) - ([^ ]+) requests/s").unwrap();
        let mut found = 0;
        let mut client_out = client_out;
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line) {
                if let Ok(lower) = f64::from_str(&caps[1]) {
                    if lower > 0.0 {
                        println!("Found throughput lower-bound: {}", lower);
                        found += 1;
                        if found == 2 {
                            break;
                        }
                    }
                }
            }
        }
    }
}
