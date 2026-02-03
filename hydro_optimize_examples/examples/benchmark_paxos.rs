use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, deploy_and_optimize,
};
use hydro_optimize::parse_results::print_parseable_bench_results;
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct BenchmarkArgs {
    #[command(flatten)]
    graph: hydro_lang::viz::config::GraphConfig,

    /// Use Gcp for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use Aws, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

/// Result of a single benchmark run
struct BenchResult {
    virtual_clients: usize,
    throughput_mean: f64,
    p99_latency: f64,
}

/// Runs a single Paxos benchmark with the given parameters
async fn run_benchmark(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    num_clients: usize,
    num_clients_per_node: usize,
    run_seconds: usize,
) -> BenchResult {
    let f = 1;
    let checkpoint_frequency = 1000;
    let i_am_leader_send_timeout = 5;
    let i_am_leader_check_timeout = 10;
    let i_am_leader_check_timeout_delay_multiplier = 15;
    let print_result_frequency = 1000;

    println!(
        "Running Paxos with {} clients and {} virtual clients per node for {} seconds",
        num_clients, num_clients_per_node, run_seconds
    );

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let replicas = builder.cluster();

    hydro_test::cluster::paxos_bench::paxos_bench(
        num_clients_per_node,
        checkpoint_frequency,
        f,
        f + 1,
        CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config: PaxosConfig {
                f,
                i_am_leader_send_timeout,
                i_am_leader_check_timeout,
                i_am_leader_check_timeout_delay_multiplier,
            },
        },
        &clients,
        &client_aggregator,
        &replicas,
        print_result_frequency,
        print_parseable_bench_results,
    );

    let run_metadata = deploy_and_optimize(
        reusable_hosts,
        deployment,
        builder.finalize(),
        ReusableClusters::new()
            .with_cluster(proposers, f + 1)
            .with_cluster(acceptors, 2 * f + 1)
            .with_cluster(clients, num_clients)
            .with_cluster(replicas, f + 1),
        ReusableProcesses::new().with_process(client_aggregator),
        Optimizations::new(),
        Some(run_seconds),
    )
    .await;

    let (throughput_lower, throughput_mean, throughput_upper) = run_metadata.throughput;
    let (p50_latency, p99_latency, p999_latency, latency_samples) = run_metadata.latencies;

    println!(
        "Result: throughput={:.2} req/s (99% CI: {:.2} - {:.2}), latency p50={:.3} ms, p99={:.3} ms, p999={:.3} ms ({} samples)",
        throughput_mean,
        throughput_lower,
        throughput_upper,
        p50_latency,
        p99_latency,
        p999_latency,
        latency_samples
    );

    BenchResult {
        virtual_clients: num_clients_per_node,
        throughput_mean,
        p99_latency,
    }
}

#[tokio::main]
async fn main() {
    let args = BenchmarkArgs::parse();

    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };

    let mut reusable_hosts = ReusableHosts::new(host_type);

    // Fixed parameters
    const NUM_CLIENTS: usize = 3;
    const RUN_SECONDS: usize = 30;
    const LATENCY_SPIKE_THRESHOLD: f64 = 2.0; // p99 latency spike = 2x baseline

    // Binary search for optimal virtual clients
    let mut low = 1usize;
    let mut high = 200usize;
    let mut results: Vec<BenchResult> = Vec::new();

    // First run at low to establish baseline latency
    let baseline = run_benchmark(
        &mut reusable_hosts,
        &mut deployment,
        NUM_CLIENTS,
        low,
        RUN_SECONDS,
    )
    .await;
    let baseline_p99 = baseline.p99_latency.max(0.001); // Avoid division by zero
    results.push(baseline);

    // Binary search to find the point where p99 latency spikes
    while high - low > 10 {
        let mid = (low + high) / 2;
        let result = run_benchmark(
            &mut reusable_hosts,
            &mut deployment,
            NUM_CLIENTS,
            mid,
            RUN_SECONDS,
        )
        .await;

        let latency_ratio = result.p99_latency / baseline_p99;
        println!(
            "virtual_clients={}, latency_ratio={:.2}x baseline",
            mid, latency_ratio
        );

        if latency_ratio > LATENCY_SPIKE_THRESHOLD {
            // Latency spiked, search lower
            high = mid;
        } else {
            // Latency acceptable, search higher
            low = mid;
        }
        results.push(result);
    }

    // Final run at the converged value
    let optimal = run_benchmark(
        &mut reusable_hosts,
        &mut deployment,
        NUM_CLIENTS,
        low,
        RUN_SECONDS,
    )
    .await;
    results.push(optimal);

    // Print summary
    println!("\n=== Benchmark Summary ===");
    println!("Optimal virtual clients: {}", low);
    println!("\nAll results:");
    for r in &results {
        println!(
            "  virtual_clients={:3}, throughput={:8.2} req/s, p99={:6.3} ms",
            r.virtual_clients, r.throughput_mean, r.p99_latency
        );
    }
}
