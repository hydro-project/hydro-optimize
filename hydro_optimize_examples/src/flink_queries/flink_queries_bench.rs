use crate::flink_queries::flink_queries::Queries;
use hydro_lang::{
    live_collections::{Optional, sliced::sliced, stream::NoOrder},
    location::MemberId,
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Process, Singleton, TCP, Unbounded},
    properties::manual_proof,
};

use hydro_std::bench_client::{aggregate_bench_results, bench_client, compute_throughput_latency};
use serde::{Serialize, de::DeserializeOwned};
use stageleft::q;
use std::time::Instant;

use crate::print_parseable_bench_results;

pub struct Client;
pub struct Aggregator;

/*
    For query functions who have 1 input stream
    Ex: q1, q2, q11, q14, q17, q18, q19, q22
*/
pub fn queries_bench_single<'a, Input, Output>(
    query_sys: &Process<'a, Queries>,
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
    query_fn: impl FnOnce(
        KeyedStream<(MemberId<Client>, u32), Input, Process<'a, Queries>, Unbounded, NoOrder>,
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
        -> KeyedStream<u32, Input, Cluster<'a, Client>, Unbounded, NoOrder>,
) where
    Input: Clone + Serialize + DeserializeOwned,
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

/*
    For query functions who have 2 input streams
    Ex: q3, q20
*/
pub fn queries_bench_double<'a, Client, Input1, Input2, Output>(
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    input_1_workload_generator: impl FnOnce(
        KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        u32,
        Input1,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
    input_2_workload_generator: impl FnOnce(
        KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        u32,
        Input2,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
    query_sys: &Process<'a, Queries>,
    query_fn: impl FnOnce(
        KeyedStream<(MemberId<Client>, u32), Input1, Process<'a, Queries>, Unbounded, NoOrder>,
        KeyedStream<(MemberId<Client>, u32), Input2, Process<'a, Queries>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        (MemberId<Client>, u32),
        Output,
        Process<'a, Queries>,
        Unbounded,
        NoOrder,
    >,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
) where
    Client: 'a,
    Input1: Clone + Serialize + DeserializeOwned,
    Input2: Clone + Serialize + DeserializeOwned,
    Output: Clone + Serialize + DeserializeOwned,
{
    let new_payload_ids = sliced! {
        let num_clients_per_node = use(num_clients_per_node, nondet!(/** This is a constant */));
        let mut next_virtual_client = use::state(|l| Optional::from(l.singleton(q!((0u32, ())))));

        // Set up virtual clients - spawn new ones each tick until we reach the limit
        let new_virtual_client = next_virtual_client.clone();
        next_virtual_client = new_virtual_client
            .clone()
            .zip(num_clients_per_node)
            .filter_map(q!(move |((virtual_id, _), num_clients_per_node)| {
                if virtual_id < num_clients_per_node as u32 {
                    Some((virtual_id + 1, ()))
                } else {
                    None
                }
            }),
        );

        new_virtual_client.into_stream().into_keyed()
    }
    .weaken_ordering();

    let input_1s = input_1_workload_generator(new_payload_ids.clone());
    let input_2s = input_2_workload_generator(new_payload_ids.clone());

    // Send to query sys
    let input_1_stream = input_1s.clone().send(query_sys, TCP.fail_stop().bincode());
    let input_2_stream = input_2s.send(query_sys, TCP.fail_stop().bincode());

    // Get outputs
    let protocol_outputs = query_fn(input_1_stream, input_2_stream);
    let protocol_outputs = protocol_outputs.demux(clients, TCP.fail_stop().bincode());

    // Latency tracking
    let start_times = input_1s.fold(
        q!(|| Instant::now()),
        q!(
            |curr, _| {
                *curr = Instant::now();
            },
            commutative = manual_proof!(/** The value will be thrown away */)
        ),
    );

    let latencies = sliced! {
        let start_times = use(start_times, nondet!(/** Only one in-flight message per virtual client at any time, and outputs happen-after inputs, so if an output is received the start_times must contain its input time. */));
        let current_outputs = use(protocol_outputs, nondet!(/** Batching is required to compare output to input time, but does not actually affect the result. */));

        let end_times_and_output = current_outputs
            .reduce(q!(|curr, new| { *curr = new; },
            commutative = manual_proof!(/** Only one in-flight message per virtual client at any time, and they are causally dependent, so this just casts to KeyedSingleton */)),
        )
        .map(q!(|output| (Instant::now(), output)));

        start_times
            .defer_tick()
            .join_keyed_singleton(end_times_and_output)
            .map(q!(|(start_time, (end_time, output))| (output, end_time.duration_since(start_time))))
            .into_keyed_stream()
    };

    // Throughput + aggregation
    let latencies = latencies.values().map(q!(|(_, latency)| latency));

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

/*
    For query functions who have 3 input streams
    Ex: q23
*/
pub fn queries_bench_triple<'a, Client, Input1, Input2, Input3, Output>(
    clients: &Cluster<'a, Client>,
    num_clients_per_node: Singleton<usize, Cluster<'a, Client>, Bounded>,
    input_1_workload_generator: impl FnOnce(
        KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        u32,
        Input1,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
    input_2_workload_generator: impl FnOnce(
        KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        u32,
        Input2,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
    input_3_workload_generator: impl FnOnce(
        KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        u32,
        Input3,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
    query_sys: &Process<'a, Queries>,
    query_fn: impl FnOnce(
        KeyedStream<(MemberId<Client>, u32), Input1, Process<'a, Queries>, Unbounded, NoOrder>,
        KeyedStream<(MemberId<Client>, u32), Input2, Process<'a, Queries>, Unbounded, NoOrder>,
        KeyedStream<(MemberId<Client>, u32), Input3, Process<'a, Queries>, Unbounded, NoOrder>,
    ) -> KeyedStream<
        (MemberId<Client>, u32),
        Output,
        Process<'a, Queries>,
        Unbounded,
        NoOrder,
    >,
    client_aggregator: &Process<'a, Aggregator>,
    interval_millis: u64,
) where
    Client: 'a,
    Input1: Clone + Serialize + DeserializeOwned,
    Input2: Clone + Serialize + DeserializeOwned,
    Input3: Clone + Serialize + DeserializeOwned,
    Output: Clone + Serialize + DeserializeOwned,
{
    let new_payload_ids = sliced! {
        let num_clients_per_node = use(num_clients_per_node, nondet!(/** This is a constant */));
        let mut next_virtual_client = use::state(|l| Optional::from(l.singleton(q!((0u32, ())))));

        // Set up virtual clients - spawn new ones each tick until we reach the limit
        let new_virtual_client = next_virtual_client.clone();
        next_virtual_client = new_virtual_client
            .clone()
            .zip(num_clients_per_node)
            .filter_map(q!(move |((virtual_id, _), num_clients_per_node)| {
                if virtual_id < num_clients_per_node as u32 {
                    Some((virtual_id + 1, ()))
                } else {
                    None
                }
            }),
        );

        new_virtual_client.into_stream().into_keyed()
    }
    .weaken_ordering();

    let input_1s = input_1_workload_generator(new_payload_ids.clone());
    let input_2s = input_2_workload_generator(new_payload_ids.clone());
    let input_3s = input_3_workload_generator(new_payload_ids.clone());

    // Send to query sys
    let input_1_stream = input_1s.clone().send(query_sys, TCP.fail_stop().bincode());
    let input_2_stream = input_2s.send(query_sys, TCP.fail_stop().bincode());
    let input_3_stream = input_3s.send(query_sys, TCP.fail_stop().bincode());

    // Get outputs
    let protocol_outputs = query_fn(input_1_stream, input_2_stream, input_3_stream);
    let protocol_outputs = protocol_outputs.demux(clients, TCP.fail_stop().bincode());

    // Latency tracking
    let start_times = input_1s.fold(
        q!(|| Instant::now()),
        q!(
            |curr, _| {
                *curr = Instant::now();
            },
            commutative = manual_proof!(/** The value will be thrown away */)
        ),
    );

    let latencies = sliced! {
        let start_times = use(start_times, nondet!(/** Only one in-flight message per virtual client at any time, and outputs happen-after inputs, so if an output is received the start_times must contain its input time. */));
        let current_outputs = use(protocol_outputs, nondet!(/** Batching is required to compare output to input time, but does not actually affect the result. */));

        let end_times_and_output = current_outputs
            .reduce(q!(|curr, new| { *curr = new; },
            commutative = manual_proof!(/** Only one in-flight message per virtual client at any time, and they are causally dependent, so this just casts to KeyedSingleton */)),
        )
        .map(q!(|output| (Instant::now(), output)));

        start_times
            .defer_tick()
            .join_keyed_singleton(end_times_and_output)
            .map(q!(|(start_time, (end_time, output))| (output, end_time.duration_since(start_time))))
            .into_keyed_stream()
    };

    // Throughput + aggregation
    let latencies = latencies.values().map(q!(|(_, latency)| latency));

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
        test_single_input::<Bid, Bid>(q1, bid_workload_generator).await;
    }

    #[tokio::test]
    async fn query_2_throughput() {
        test_single_input::<Bid, (i64, i64)>(q2, query2_workload_generator).await;
    }

    #[tokio::test]
    async fn query_3_throughput() {
        test_double_input::<Client, Auction, Person, (String, String, String, i64)>(
            q3,
            auction_workload_generator,
            person_workload_generator,
        )
        .await;
    }

    #[tokio::test]
    async fn query_11_throughput() {
        test_single_input::<Bid, (i64, i32, i64, i64)>(q11, query11_workload_generator).await;
    }

    #[tokio::test]
    async fn query_14_throughput() {
        test_single_input::<Bid, (i64, i64, f64, String, i64, String, i64)>(
            q14,
            query14_workload_generator,
        )
        .await;
    }

    #[tokio::test]
    async fn query_17_throughput() {
        test_single_input::<Bid, (i64, i64, i64, i64, i64, i32, i64, i64, i64, i64)>(
            q17,
            query17_workload_generator,
        )
        .await;
    }

    #[tokio::test]
    async fn query_18_throughput() {
        test_single_input::<Bid, (i64, i64, i64, i64)>(q18, query18_workload_generator).await;
    }

    #[tokio::test]
    async fn query_19_throughput() {
        test_single_input::<Bid, (usize, i64, i64)>(q19, query19_workload_generator).await;
    }

    #[tokio::test]
    async fn query_20_throughput() {
        test_double_input::<Client, Auction, Bid, (i64, i64, String, i64, i64, i64)>(
            q20,
            auction_workload_generator,
            bid_workload_generator_no_prev,
        )
        .await;
    }

    #[tokio::test]
    async fn query_22_throughput() {
        test_single_input::<Bid, (i64, i64, i64, String, String, String, String)>(
            q22,
            query22_workload_generator,
        )
        .await;
    }

    #[tokio::test]
    async fn query_23_throughput() {
        test_triple_input::<Client, Auction, Bid, Person, (Auction, Bid, Person)>(
            q23,
            auction_workload_generator,
            bid_workload_generator_no_prev,
            person_workload_generator,
        )
        .await;
    }

    async fn test_single_input<'a, Input, Output>(
        query_fn: impl FnOnce(
            KeyedStream<(MemberId<Client>, u32), Input, Process<'a, Queries>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            (MemberId<Client>, u32),
            Output,
            Process<'a, Queries>,
            Unbounded,
            NoOrder,
        >,
        workload_generator: impl FnOnce(
            KeyedStream<u32, Option<Output>, Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            u32,
            Input,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
    ) where
        Input: Clone + Serialize + DeserializeOwned,
        Output: Clone + Serialize + DeserializeOwned,
    {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench_single::<Input, Output>(
            &query_sys,
            &clients,
            clients.singleton(q!(1usize)),
            &client_aggregator,
            interval_millis,
            query_fn,
            workload_generator,
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

    async fn test_double_input<'a, Client, Input1, Input2, Output>(
        query_fn: impl FnOnce(
            KeyedStream<(MemberId<Client>, u32), Input1, Process<'a, Queries>, Unbounded, NoOrder>,
            KeyedStream<(MemberId<Client>, u32), Input2, Process<'a, Queries>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            (MemberId<Client>, u32),
            Output,
            Process<'a, Queries>,
            Unbounded,
            NoOrder,
        >,
        input1_workload_generator: impl FnOnce(
            KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            u32,
            Input1,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
        input2_workload_generator: impl FnOnce(
            KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            u32,
            Input2,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
    ) where
        Client: 'a,
        Input1: Clone + Serialize + DeserializeOwned,
        Input2: Clone + Serialize + DeserializeOwned,
        Output: Clone + Serialize + DeserializeOwned,
    {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench_double::<Client, Input1, Input2, Output>(
            &clients,
            clients.singleton(q!(1usize)),
            input1_workload_generator,
            input2_workload_generator,
            &query_sys,
            query_fn,
            &client_aggregator,
            interval_millis,
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

    async fn test_triple_input<'a, Client, Input1, Input2, Input3, Output>(
        query_fn: impl FnOnce(
            KeyedStream<(MemberId<Client>, u32), Input1, Process<'a, Queries>, Unbounded, NoOrder>,
            KeyedStream<(MemberId<Client>, u32), Input2, Process<'a, Queries>, Unbounded, NoOrder>,
            KeyedStream<(MemberId<Client>, u32), Input3, Process<'a, Queries>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            (MemberId<Client>, u32),
            Output,
            Process<'a, Queries>,
            Unbounded,
            NoOrder,
        >,
        input1_workload_generator: impl FnOnce(
            KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            u32,
            Input1,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
        input2_workload_generator: impl FnOnce(
            KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            u32,
            Input2,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
        input3_workload_generator: impl FnOnce(
            KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
        ) -> KeyedStream<
            u32,
            Input3,
            Cluster<'a, Client>,
            Unbounded,
            NoOrder,
        >,
    ) where
        Client: 'a,
        Input1: Clone + Serialize + DeserializeOwned,
        Input2: Clone + Serialize + DeserializeOwned,
        Input3: Clone + Serialize + DeserializeOwned,
        Output: Clone + Serialize + DeserializeOwned,
    {
        let mut builder = FlowBuilder::new();
        let query_sys = builder.process();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let interval_millis = 1000;

        queries_bench_triple::<Client, Input1, Input2, Input3, Output>(
            &clients,
            clients.singleton(q!(1usize)),
            input1_workload_generator,
            input2_workload_generator,
            input3_workload_generator,
            &query_sys,
            query_fn,
            &client_aggregator,
            interval_millis,
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
