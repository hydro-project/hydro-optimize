use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::Local;
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
    measurement_second: usize,
) -> (RunMetadata, HashMap<LocationId, String>, PathBuf) {
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
        Some(measurement_second),
    )
    .await;

    let output_dir = Path::new("benchmark_results").join(format!(
        "paxos_{}",
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));

    (run_metadata, location_id_to_cluster, output_dir)
}

async fn output_metrics(
    run_metadata: RunMetadata,
    location_id_to_cluster: &HashMap<LocationId, String>,
    output_dir: &Path,
    num_clients: usize,
    num_clients_per_node: usize,
    measurement_second: usize,
) {
    if let Some(&throughput) = run_metadata.throughputs.get(measurement_second) {
        println!(
            "Throughput @{}s: {} requests/s",
            measurement_second + 1,
            throughput
        );
    }
    if let Some(&(p50, p99, p999, samples)) = run_metadata.latencies.get(measurement_second) {
        println!(
            "Latency @{}s: p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({} samples)",
            measurement_second + 1,
            p50,
            p99,
            p999,
            samples
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
        location_id_to_cluster,
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

    const MEASUREMENT_SECOND: usize = 59;
    const RUN_SECONDS: usize = 90;
    const PHYSICAL_CLIENTS_MIN: usize = 1;
    const PHYSICAL_CLIENTS_MAX: usize = 10;
    const VIRTUAL_CLIENTS_STEP: usize = 50;

    let mut best_throughput: usize = 0;
    let mut best_config: (usize, usize) = (0, 0);
    let mut output_dir = None;
    // Total virtual clients (physical × per-node) that saturated the previous
    // physical-client round. Used to pick the starting per-node count for the
    // next round so we don't re-explore the low end of the curve.
    let mut saturated_total_virtual: Option<usize> = None;

    'outer: for num_clients in (PHYSICAL_CLIENTS_MIN..=PHYSICAL_CLIENTS_MAX).step_by(1) {
        let mut prev_throughput: Option<usize> = None;
        let best_before_round = best_throughput;

        // Start at the per-node count that yields roughly the same total
        // virtual clients as the previous round's saturation point, rounded
        // down to the nearest step (minimum 1).
        let virtual_start = saturated_total_virtual
            .map(|total| (total / num_clients).max(1))
            .unwrap_or(1);

        let mut num_virtual = virtual_start;
        loop {
            let (run_metadata, location_id_to_cluster, new_output_dir) = run_benchmark(
                &mut reusable_hosts,
                &mut deployment,
                num_clients,
                num_virtual,
                RUN_SECONDS,
                MEASUREMENT_SECOND,
            )
            .await;

            let output_dir = output_dir.get_or_insert(new_output_dir);

            let current_throughput = run_metadata.throughputs[MEASUREMENT_SECOND];
            println!(
                "physical_clients={}, virtual_clients={}, throughput@{}s={}",
                num_clients,
                num_virtual,
                MEASUREMENT_SECOND + 1,
                current_throughput
            );

            output_metrics(
                run_metadata,
                &location_id_to_cluster,
                output_dir,
                num_clients,
                num_virtual,
                MEASUREMENT_SECOND,
            )
            .await;

            if current_throughput > best_throughput {
                best_throughput = current_throughput;
                best_config = (num_clients, num_virtual);
            }

            // Check saturation against the previous virtual-client run
            if let Some(prev) = prev_throughput
                && current_throughput <= prev
            {
                println!(
                    "Throughput saturated for {} physical clients \
                     ({}→{} rps). Moving to next physical client count.",
                    num_clients, prev, current_throughput
                );
                saturated_total_virtual = Some(num_clients * num_virtual);
                break; // break inner loop, increase physical clients
            }

            prev_throughput = Some(current_throughput);
            num_virtual += VIRTUAL_CLIENTS_STEP;
        }

        // After increasing physical clients, check if we're still saturated
        // compared to the best throughput *before* this round. If this round
        // didn't improve on the prior best, more physical clients won't help.
        if let Some(prev) = prev_throughput
            && prev <= best_before_round
            && num_clients > PHYSICAL_CLIENTS_MIN
        {
            println!(
                "Throughput still saturated after increasing physical clients \
                 (prior best={}, current_peak={}). Stopping search.",
                best_before_round, prev
            );
            break 'outer;
        }
    }

    println!("\n=== Benchmark Summary ===");
    println!(
        "Best throughput: {} rps with {} physical clients and {} virtual clients",
        best_throughput, best_config.0, best_config.1
    );
}
