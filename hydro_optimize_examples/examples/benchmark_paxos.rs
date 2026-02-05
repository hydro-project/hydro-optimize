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
use hydro_optimize::parse_results::{RunMetadata, SarStats};
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

/// Writes per-second CPU and network stats to CSV files for each location
fn write_sar_csv(
    output_dir: &Path,
    sar_stats: &HashMap<LocationId, Vec<SarStats>>,
    location_id_to_cluster: &HashMap<LocationId, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;

    for (location, stats) in sar_stats {
        if stats.is_empty() {
            continue;
        }

        let location_name = location_id_to_cluster.get(location).unwrap();
        let filename = output_dir.join(format!("{}.csv", location_name));

        let mut file = File::create(&filename)?;
        writeln!(
            file,
            "time_s,cpu_user,cpu_system,cpu_idle,network_tx_bytes_per_sec,network_rx_bytes_per_sec",
        )?;

        for (i, s) in stats.iter().enumerate() {
            writeln!(
                file,
                "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                i,
                s.cpu.user,
                s.cpu.system,
                s.cpu.idle,
                s.network.tx_packets_per_sec,
                s.network.rx_packets_per_sec,
                s.network.tx_bytes_per_sec,
                s.network.rx_bytes_per_sec,
            )?;
        }

        println!("Generated CSV: {}", filename.display());
    }

    Ok(())
}

/// Writes summary stats (throughput, latency) to a text file for each location
fn write_summary_txt(
    output_dir: &Path,
    throughput: (f64, f64, f64),
    latencies: (f64, f64, f64, u64),
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;

    let (throughput_lower, throughput_mean, throughput_upper) = throughput;
    let (p50_latency, p99_latency, p999_latency, latency_samples) = latencies;

    let filename = output_dir.join("summary.txt");

    let mut file = File::create(&filename)?;
    writeln!(file, "=== Benchmark Summary ===")?;
    writeln!(file, "Throughput (requests/s):")?;
    writeln!(file, "  Lower: {:.2}", throughput_lower)?;
    writeln!(file, "  Mean:  {:.2}", throughput_mean)?;
    writeln!(file, "  Upper: {:.2}", throughput_upper)?;
    writeln!(file)?;
    writeln!(file, "Latency (ms):")?;
    writeln!(file, "  p50:  {:.3}", p50_latency)?;
    writeln!(file, "  p99:  {:.3}", p99_latency)?;
    writeln!(file, "  p999: {:.3}", p999_latency)?;
    writeln!(file, "  Samples: {}", latency_samples)?;
    println!("Generated summary: {}", filename.display());

    Ok(())
}

/// Runs a single Paxos benchmark with the given parameters
async fn run_benchmark(
    reusable_hosts: &mut ReusableHosts,
    deployment: &mut Deployment,
    num_clients: usize,
    num_clients_per_node: usize,
    run_seconds: usize,
) -> (RunMetadata, HashMap<LocationId, String>, String) {
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
    let output_dir = format!(
        "paxos_{}clients_{}virt_{}s",
        num_clients, num_clients_per_node, run_seconds
    );

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

    (run_metadata, location_id_to_cluster, output_dir)
}

async fn output_metrics(
    run_metadata: RunMetadata,
    location_id_to_cluster: &HashMap<LocationId, String>,
    output_dir: String,
) {
    let (throughput_lower, throughput_mean, throughput_upper) = run_metadata.throughput;
    let (p50_latency, p99_latency, p999_latency, latency_samples) = run_metadata.latencies;

    println!(
        "Throughput: {:.2} - {:.2} - {:.2} requests/s",
        throughput_lower, throughput_mean, throughput_upper
    );
    println!(
        "Latency: p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({:} samples)",
        p50_latency, p99_latency, p999_latency, latency_samples
    );
    run_metadata
        .sar_stats
        .iter()
        .for_each(|(location, sar_stats)| {
            println!(
                "{} CPU: {:.2}%",
                location_id_to_cluster.get(location).unwrap(),
                sar_stats
                    .last()
                    .and_then(|stats| Some(stats.cpu.user + stats.cpu.system))
                    .unwrap_or_default()
            )
        });

    // Write CSV and summary files for sar stats
    if let Err(e) = write_sar_csv(
        Path::new(&output_dir),
        &run_metadata.sar_stats,
        &location_id_to_cluster,
    ) {
        eprintln!("Failed to write CSV: {}", e);
    }
    if let Err(e) = write_summary_txt(
        Path::new(&output_dir),
        run_metadata.throughput,
        run_metadata.latencies,
    ) {
        eprintln!("Failed to write summary: {}", e);
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

    // First run at low to establish baseline latency
    let (run_metadata, location_id_to_cluster, output_dir) = run_benchmark(
        &mut reusable_hosts,
        &mut deployment,
        NUM_CLIENTS,
        low,
        RUN_SECONDS,
    )
    .await;
    let baseline_p99 = run_metadata.latencies.1.max(0.001); // Avoid division by zero
    output_metrics(run_metadata, &location_id_to_cluster, output_dir).await;

    // Binary search to find the point where p99 latency spikes
    while high - low > 10 {
        let mid = (low + high) / 2;
        let (run_metadata, location_id_to_cluster, output_dir) = run_benchmark(
            &mut reusable_hosts,
            &mut deployment,
            NUM_CLIENTS,
            mid,
            RUN_SECONDS,
        )
        .await;
        let p99_latency = run_metadata.latencies.1;
        output_metrics(run_metadata, &location_id_to_cluster, output_dir).await;

        let latency_ratio = p99_latency / baseline_p99;
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
    }

    // Print summary
    println!("\n=== Benchmark Summary ===");
    println!("Optimal virtual clients: low {} high {} ", low, high);
}
