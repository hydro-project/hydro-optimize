use hydro_lang::{
    location::Location,
    nondet::nondet,
    prelude::{Cluster, Process},
};
use hydro_std::bench_client::{bench_client, print_bench_results};

use hydro_test::cluster::paxos_bench::inc_u32_workload_generator;
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
    let bench_results = bench_client(
        clients,
        inc_u32_workload_generator,
        |payloads| {
            let k_tick = kv.tick();
            let k_payloads = payloads.send_bincode(kv).batch(&k_tick, nondet!(/** TODO: Actually can use atomic() here, but there's no way to exit atomic in KeyedStreams? */));

            // Insert each payload into the KV store
            k_payloads
                    .clone()
                    .values()
                    .assume_ordering(nondet!(/** Last writer wins. TODO: Technically, we only need to assume ordering over the keyed stream (ordering of values with different keys doesn't matter. But there's no .persist() for KeyedStreams) */))
                    .persist()
                    .into_keyed()
                    .reduce(q!(|prev, new| {
                        *prev = new;
                    }))
                    .entries()
                    .all_ticks()
                    .assume_ordering(nondet!(/** for_each does nothing, just need to end on a HydroLeaf */))
                    .assume_retries(nondet!(/** for_each does nothing, just need to end on a HydroLeaf */))
                    .for_each(q!(|_| {})); // Do nothing, just need to end on a HydroLeaf

            // Send committed requests back to the original client
            k_payloads.all_ticks().demux_bincode(clients).into()
        },
        num_clients_per_node,
        nondet!(/** bench */),
    );

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
        let builder = FlowBuilder::new();
        let kv = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();

        simple_kv_bench(1, &kv, &clients, &client_aggregator);
        let built = builder.with_default_optimize::<HydroDeploy>();

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
        let builder = FlowBuilder::new();
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
        let client_out = client_node.stdout_filter("Throughput:").await;

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
