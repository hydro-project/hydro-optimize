use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_lang::location::dynamic::LocationId;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    Optimizations, ReusableClusters, ReusableProcesses, deploy_and_optimize,
};
use hydro_optimize::parse_results::RunMetadata;
use hydro_optimize_examples::print_parseable_bench_results;
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

/// Writes per-second metrics CSV for each location, combining sar stats with
/// the shared throughput and latency time series.
fn write_metrics_csv(
    output_dir: &Path,
    run_metadata: &RunMetadata,
    location_id_to_cluster: &HashMap<LocationId, String>,
    num_clients: usize,
    num_clients_per_node: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;

    for (location, stats) in &run_metadata.sar_stats {
        if stats.is_empty() {
            continue;
        }

        let location_name = location_id_to_cluster.get(location).unwrap();
        let filename = output_dir.join(format!(
            "{}_{}c_{}vc.csv",
            location_name, num_clients, num_clients_per_node
        ));
        let num_rows = stats
            .len()
            .max(run_metadata.throughputs.len())
            .max(run_metadata.latencies.len());

        let mut file = File::create(&filename)?;
        writeln!(
            file,
            "time_s,cpu_user,cpu_system,cpu_idle,\
             network_tx_packets_per_sec,network_rx_packets_per_sec,\
             network_tx_bytes_per_sec,network_rx_bytes_per_sec,\
             throughput_rps,latency_p50_ms,latency_p99_ms,latency_p999_ms,latency_samples",
        )?;

        for i in 0..num_rows {
            let sar = stats.get(i).copied().unwrap_or_default();
            let thr = run_metadata.throughputs.get(i).copied().unwrap_or(0);
            let (p50, p99, p999, count) =
                run_metadata.latencies.get(i).copied().unwrap_or_default();
            writeln!(
                file,
                "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{:.3},{:.3},{:.3},{}",
                i,
                sar.cpu.user,
                sar.cpu.system,
                sar.cpu.idle,
                sar.network.tx_packets_per_sec,
                sar.network.rx_packets_per_sec,
                sar.network.tx_bytes_per_sec,
                sar.network.rx_bytes_per_sec,
                thr,
                p50,
                p99,
                p999,
                count,
            )?;
        }

        println!("Generated CSV: {}", filename.display());
    }

    Ok(())
}

/// Runs a single Paxos benchmark with the given parameters
async fn run_benchmark(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    num_clients: usize,
    num_clients_per_node: usize,
    run_seconds: usize,
) -> (RunMetadata, HashMap<LocationId, String>) {
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
    let location_id_to_cluster = HashMap::from([
        (proposers.id(), "proposer".to_string()),
        (acceptors.id(), "acceptor".to_string()),
        (clients.id(), "client".to_string()),
        (replicas.id(), "replica".to_string()),
        (client_aggregator.id(), "client_aggregator".to_string()),
    ]);

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
        print_result_frequency / 10,
        print_result_frequency,
        print_parseable_bench_results,
    );

    let run_metadata = deploy_and_optimize(
        reusable_hosts,
        deployment,
        builder.finalize(),
        ReusableClusters::default()
            .with_cluster(proposers, f + 1)
            .with_cluster(acceptors, 2 * f + 1)
            .with_cluster(clients, num_clients)
            .with_cluster(replicas, f + 1),
        ReusableProcesses::default().with_process(client_aggregator),
        Optimizations::default(),
        Some(run_seconds),
    )
    .await;

    (run_metadata, location_id_to_cluster)
}

async fn output_metrics(
    run_metadata: RunMetadata,
    location_id_to_cluster: &HashMap<LocationId, String>,
    output_dir: &Path,
    num_clients: usize,
    num_clients_per_node: usize,
) {
    if let Some(&throughput) = run_metadata.throughputs.last() {
        println!("Throughput: {} requests/s", throughput);
    }
    if let Some(&(p50, p99, p999, samples)) = run_metadata.latencies.last() {
        println!(
            "Latency: p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({} samples)",
            p50, p99, p999, samples
        );
    }
    run_metadata
        .sar_stats
        .iter()
        .for_each(|(location, sar_stats)| {
            println!(
                "{} CPU: {:.2}%",
                location_id_to_cluster.get(location).unwrap(),
                sar_stats
                    .last()
                    .map(|stats| stats.cpu.user + stats.cpu.system)
                    .unwrap_or_default()
            )
        });

    if let Err(e) = write_metrics_csv(
        output_dir,
        &run_metadata,
        &location_id_to_cluster,
        num_clients,
        num_clients_per_node,
    ) {
        eprintln!("Failed to write CSV: {}", e);
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

    let output_dir = Path::new("benchmark_results").join(format!(
        "paxos_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));

    // Fixed parameters
    const NUM_CLIENTS: usize = 3;
    const RUN_SECONDS: usize = 30;
    const LATENCY_SPIKE_THRESHOLD: f64 = 3.0; // p99 must be < 3x p50

    // Binary search for optimal virtual clients
    let mut low = 1usize;
    let mut high = 200usize;

    // Binary search to find the point where p99 latency spikes relative to p50
    while high - low > 10 {
        let mid = (low + high) / 2;
        let (run_metadata, location_id_to_cluster) = run_benchmark(
            &mut reusable_hosts,
            &mut deployment,
            NUM_CLIENTS,
            mid,
            RUN_SECONDS,
        )
        .await;
        let (p50, p99) = run_metadata
            .latencies
            .last()
            .map(|l| (l.0, l.1))
            .unwrap_or((0.001, 0.001));
        let p50 = p50.max(0.001); // Avoid division by zero
        output_metrics(
            run_metadata,
            &location_id_to_cluster,
            &output_dir,
            NUM_CLIENTS,
            mid,
        )
        .await;

        let latency_ratio = p99 / p50;
        println!(
            "virtual_clients={}, p99/p50={:.2}x (threshold={:.1}x)",
            mid, latency_ratio, LATENCY_SPIKE_THRESHOLD
        );

        if latency_ratio > LATENCY_SPIKE_THRESHOLD {
            // Latency spiked, search lower
            high = mid;
        } else {
            // Latency acceptable, search higher
            low = mid;
        }
    }

    // Print summary
    println!("\n=== Benchmark Summary ===");
    println!("Optimal virtual clients: low {} high {} ", low, high);
}
