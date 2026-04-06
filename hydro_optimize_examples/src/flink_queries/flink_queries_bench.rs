use crate::flink_queries::flink_queries::{Bid, Queries};
use hydro_lang::{
    live_collections::stream::NoOrder,
    location::MemberId,
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Process, Singleton, TCP, Unbounded},
};

use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use serde::{Serialize, de::DeserializeOwned};
use stageleft::q;

use crate::print_parseable_bench_results;

pub struct Client;
pub struct Aggregator;

/*
    For query functions whose input is only Bid streams.
    Ex: 1, 2, 11, 14, 17, 18, 19, 22
*/
pub fn queries_bench<'a, Output>(
    query_sys: &Process<'a, Queries>,
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
    query_fn: impl FnOnce(
        KeyedStream<(MemberId<Client>, u32), Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        (MemberId<Client>, u32),
        Output,
        Process<'a, Queries>,
        Unbounded,
        NoOrder,
    >,
    workload_generator: impl FnOnce(
        KeyedStream<u32, Option<Output>, Cluster<'a, Client>, Unbounded, NoOrder>,
    )
        -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder>,
) where
    Output: Clone + Serialize + DeserializeOwned,
{
    let latencies = bench_client(clients, num_clients_per_node, workload_generator, |input| {
        let bid_stream = input.send(query_sys, TCP.fail_stop().bincode());
        let result = query_fn(bid_stream);
        result.demux(clients, TCP.fail_stop().bincode())
    })
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

// Generates Bids
pub fn bid_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<u32, Option<Bid>, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: (b.auction + 1) % 50,
                bidder: b.bidder + 1,
                price: b.price + 10,
                date_time: b.date_time + 1,
                extra: b.extra,
                channel: b.channel,
                url: b.url,
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 100,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

/*
    Difficult to implement as output is filtered to those with auction % 123 == 0. Must prevent too much overfiltering
*/
pub fn query2_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<u32, Option<(i64, i64)>, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: 123,
                bidder: b.0 + 10,
                price: b.1 + 11,
                date_time: b.0 + 3,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 0,
                bidder: 100,
                price: 11,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query11_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i32, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Output corresponds to (bidder, aggregation count, datetim start, datetime end)
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: 1,
                price: b.0 + 15,
                date_time: b.2 + 7,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 1,
                price: 100,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query14_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, f64, String, i64, String, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: b.1 + 10,
                price: b.2 as i64 + 10,
                date_time: (b.4 + 3) % 24,
                extra: b.5,
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 40000000,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query17_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, i64, i64, i64, i32, i64, i64, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // (auction, datetime, ... [aggregation data])
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: (b.0 + 1) % 3,
                bidder: b.0 + 1,
                price: b.4 + 10000,
                date_time: (b.1 + 1) % 3,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 9000,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query18_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        // (auction, bidder, datetime, price)
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: b.1 + 10,
                price: b.3 as i64 + 15,
                date_time: (b.2 + 3) % 125,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 100,
                date_time: 0,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        }
    }))
}

pub fn query19_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(usize, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.1,
                bidder: b.0 as i64 + 11,
                price: b.2 + 10,
                date_time: (b.2 + 5) % 125,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 100,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query22_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, i64, String, String, String, String)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: b.1 + 10,
                price: b.2 + 15,
                date_time: b.0 + 123,
                extra: "".to_string(),
                channel: "".to_string(),
                url: format!("{}a/{}b/{}c/{}d/{}e/{}f/", b.4, b.5, b.6, b.4, b.5, b.6),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 150,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a/b/c/d/e/f".to_string(),
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use crate::flink_queries::flink_queries::*;
    use crate::flink_queries::flink_queries_bench::*;
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
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<Bid>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q1::<(MemberId<Client>, u32)>,
            bid_workload_generator,
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

    #[tokio::test]
    async fn query_2_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(i64, i64)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q2::<(MemberId<Client>, u32)>,
            query2_workload_generator,
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

    #[tokio::test]
    async fn query_11_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(i64, i32, i64, i64)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q11::<(MemberId<Client>, u32)>,
            query11_workload_generator,
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

    #[tokio::test]
    async fn query_14_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(i64, i64, f64, String, i64, String, i64)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q14::<(MemberId<Client>, u32)>,
            query14_workload_generator,
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

    #[tokio::test]
    async fn query_17_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(i64, i64, i64, i64, i64, i32, i64, i64, i64, i64)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q17::<(MemberId<Client>, u32)>,
            query17_workload_generator,
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

    #[tokio::test]
    async fn query_18_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(i64, i64, i64, i64)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q18::<(MemberId<Client>, u32)>,
            query18_workload_generator,
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

    #[tokio::test]
    async fn query_19_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(usize, i64, i64)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q19::<(MemberId<Client>, u32)>,
            query19_workload_generator,
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

    #[tokio::test]
    async fn query_22_throughput() {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench::<(i64, i64, i64, String, String, String, String)>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            q22::<(MemberId<Client>, u32)>,
            query22_workload_generator,
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
