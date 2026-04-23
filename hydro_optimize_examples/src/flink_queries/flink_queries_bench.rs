use crate::flink_queries::flink_queries::{Auction, Bid, Person, Queries};
use hydro_lang::{
    live_collections::stream::NoOrder,
    location::MemberId,
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Process, Singleton, Stream, TCP, Unbounded},
};

use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use rand::RngExt;
use serde::{Serialize, de::DeserializeOwned};
use stageleft::q;

use crate::print_parseable_bench_results;

pub struct Client;
pub struct Aggregator;

pub fn queries_bench<'a, Output>(
    query_sys: &Process<'a, Queries>,
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
    auctions_ratio: u64,
    bids_ratio: u64,
    persons_ratio: u64,
    query_fn: impl FnOnce(
        KeyedStream<MemberId<Client>, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
        KeyedStream<MemberId<Client>, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
        KeyedStream<MemberId<Client>, Person, Process<'a, Queries>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        MemberId<Client>,
        Output,
        Process<'a, Queries>,
        Unbounded,
        NoOrder,
    >,
    auction_workload_generator: impl FnOnce(
        Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
    )
        -> Stream<Auction, Cluster<'a, Client>, Unbounded, NoOrder>,
    bid_workload_generator: impl FnOnce(
        Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
    ) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder>,
    person_workload_generator: impl FnOnce(
        Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
    )
        -> Stream<Person, Cluster<'a, Client>, Unbounded, NoOrder>,
) where
    Output: Clone + Serialize + DeserializeOwned,
{
    let latencies = bench_client(
        clients,
        num_clients_per_node,
        |output| output,
        |input| {
            let payloads = input.entries().map(q!(move |_| {
                rand::rng().random_range(0..(auctions_ratio + bids_ratio + persons_ratio))
            }));

            // Split payload into 3 inputs
            let (auctions_payloads, not_auctions_payloads) =
                payloads.partition(q!(move |num| *num < auctions_ratio));
            let (bids_payloads, persons_payloads) =
                not_auctions_payloads.partition(q!(move |num| *num < auctions_ratio + bids_ratio));

            // Generate actual objects with a pattern relevant to specific query
            let auctions_input = auction_workload_generator(auctions_payloads)
                .send(query_sys, TCP.fail_stop().bincode());
            let bids_input =
                bid_workload_generator(bids_payloads).send(query_sys, TCP.fail_stop().bincode());
            let persons_input = person_workload_generator(persons_payloads)
                .send(query_sys, TCP.fail_stop().bincode());

            let result = query_fn(auctions_input, bids_input, persons_input);
            result
                .map(q!(|res| (0, res)))
                .demux(clients, TCP.fail_stop().bincode())
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

#[cfg(test)]
mod tests {
    use crate::flink_queries::{
        flink_queries::*, flink_queries_bench::*, flink_workload_generators::*,
    };
    use hydro_deploy::Deployment;
    use hydro_lang::{
        deploy::{DeployCrateWrapper, TrybuildHost},
        location::{Location, MemberId},
        prelude::FlowBuilder,
    };
    use std::str::FromStr;

    use regex::Regex;

    use crate::THROUGHPUT_PREFIX;
    #[cfg(stageleft_runtime)]
    use stageleft::q;

    #[tokio::test]
    async fn query_1_throughput() {
        test_template::<Bid>(
            0,
            100,
            0,
            q1,
            auction_workload_generator_empty,
            q1_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_2_throughput() {
        test_template::<Option<(i64, i64)>>(
            0,
            100,
            0,
            q2,
            auction_workload_generator_empty,
            q2_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_3_throughput() {
        test_template::<Option<(String, String, String, i64)>>(
            50,
            0,
            50,
            q3,
            q3_auction_workload_generator,
            bid_workload_generator_empty,
            q3_person_workload_generator,
        )
        .await;
    }

    #[tokio::test]
    async fn query_11_throughput() {
        test_template::<(i64, i32, i64, i64)>(
            50,
            0,
            50,
            q11,
            auction_workload_generator_empty,
            q11_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_14_throughput() {
        test_template::<Option<(i64, i64, f64, String, i64, String, i64)>>(
            0,
            100,
            0,
            q14,
            auction_workload_generator_empty,
            q14_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_17_throughput() {
        test_template::<(i64, i64, i64, i64, i64, i32, i64, i64, i64, i64)>(
            0,
            100,
            0,
            q17,
            auction_workload_generator_empty,
            q17_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_18_throughput() {
        test_template::<(i64, i64, i64, i64)>(
            0,
            100,
            0,
            q18,
            auction_workload_generator_empty,
            q18_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_19_throughput() {
        test_template::<(usize, i64, i64)>(
            0,
            100,
            0,
            q19,
            auction_workload_generator_empty,
            q19_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_20_throughput() {
        test_template::<Option<(i64, i64, String, i64, i64, i64)>>(
            30,
            70,
            0,
            q20,
            q20_auction_workload_generator,
            q20_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_22_throughput() {
        test_template::<(i64, i64, i64, String, String, String, String)>(
            0,
            100,
            0,
            q22,
            auction_workload_generator_empty,
            q22_bid_workload_generator,
            person_workload_generator_empty,
        )
        .await;
    }

    #[tokio::test]
    async fn query_23_throughput() {
        test_template::<(Auction, Bid, Person)>(
            20,
            60,
            20,
            q23,
            q23_auction_workload_generator,
            q23_bid_workload_generator,
            q23_person_workload_generator,
        )
        .await;
    }

    async fn test_template<'a, Output>(
        auctions_ratio: u64,
        bids_ratio: u64,
        persons_ratio: u64,
        query_fn: impl FnOnce(
            KeyedStream<MemberId<Client>, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
            KeyedStream<MemberId<Client>, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
            KeyedStream<MemberId<Client>, Person, Process<'a, Queries>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            MemberId<Client>,
            Output,
            Process<'a, Queries>,
            Unbounded,
            NoOrder,
        >,
        auction_workload_generator: impl FnOnce(
            Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> Stream<
            Auction,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
        bid_workload_generator: impl FnOnce(
            Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
        )
            -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder>,
        person_workload_generator: impl FnOnce(
            Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> Stream<
            Person,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
    ) where
        Output: Clone + Serialize + DeserializeOwned,
    {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<Output>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            auctions_ratio,
            bids_ratio,
            persons_ratio,
            query_fn,
            auction_workload_generator,
            bid_workload_generator,
            person_workload_generator,
        );
        let mut deployment = Deployment::new();

        let nodes = builder
            .with_process(&query_sys, TrybuildHost::new(deployment.Localhost()))
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let client_node = &nodes.get_process(&client_aggregator);
        let client_out = client_node.stdout_filter(THROUGHPUT_PREFIX);

        deployment.start().await.unwrap();

        let re = Regex::new(r"(\d+) requests/s").unwrap();
        let mut found = 0;
        let mut client_out = client_out;
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line)
                && let Ok(lower) = f64::from_str(&caps[1])
                && lower > 0.0
            {
                println!("Found throughput lower-bound: {}", lower);
                found += 1;
                if found == 2 {
                    break;
                }
            }
        }
    }
}
