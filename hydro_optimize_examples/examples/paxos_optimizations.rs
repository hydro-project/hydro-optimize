use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::compile::ir::deep_clone;
use hydro_lang::location::Location;
use hydro_lang::location::dynamic::LocationId;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::{
    ApplyResult, BottleneckState, MEASUREMENT_SECOND, Optimizations, PHYSICAL_CLIENTS, RUN_SECONDS,
    ReusableClusters, ReusableProcesses, SCENARIO_FINISHED_PREFIX, START_MEASUREMENT_SECOND,
    VIRTUAL_CLIENTS_MAX, apply_optimizations, deploy_and_optimize,
};
use hydro_optimize::parse_results::RunMetadata;
use hydro_optimize::repair::{inject_id, remove_counter};
use hydro_optimize::rewrites::{Rewrites, replay};
use hydro_optimize_examples::print_parseable_bench_results;
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
use stageleft::q;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct Args {
    /// Which bottleneck (cluster or process name from ReusableClusters/ReusableProcesses) to try.
    #[arg(long)]
    bottleneck: String,
    /// Path to profiling JSON from a previous benchmark run.
    #[arg(long)]
    profiling: String,
    /// Path to the existing Rewrites JSON to replay before applying new optimization.
    #[arg(long)]
    rewrites_in: Option<String>,
    /// Path to the existing BottleneckState JSON.
    #[arg(long)]
    state_in: Option<String>,
    /// Where to write the combined Rewrites JSON after this optimization.
    #[arg(long)]
    rewrites_out: String,
    /// Where to write the updated BottleneckState JSON.
    #[arg(long)]
    state_out: String,
    #[arg(long)]
    gcp: Option<String>,
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

fn load_rewrites(path: &Path) -> Rewrites {
    let s = std::fs::read_to_string(path).expect("failed to read rewrites file");
    serde_json::from_str(&s).expect("failed to deserialize rewrites")
}

fn save_rewrites(rewrites: &Rewrites, path: &Path) {
    let json = serde_json::to_string_pretty(rewrites).unwrap();
    std::fs::write(path, json).expect("failed to write rewrites");
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Build the original Paxos flow (same as benchmark_paxos::run_benchmark).
    let f = 1;
    let num_clients = PHYSICAL_CLIENTS;
    let checkpoint_frequency = 1000;
    let print_result_frequency = 1000;

    let mut builder = FlowBuilder::new();
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let replicas = builder.cluster();

    let proposers_id = proposers.id();
    let acceptors_id = acceptors.id();
    let clients_id = clients.id();
    let client_aggregator_id = client_aggregator.id();
    let replicas_id = replicas.id();

    hydro_test::cluster::paxos_bench::paxos_bench(
        checkpoint_frequency,
        f,
        f + 1,
        CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config: PaxosConfig {
                f,
                i_am_leader_send_timeout: 5,
                i_am_leader_check_timeout: 10,
                i_am_leader_check_timeout_delay_multiplier: 15,
            },
        },
        &clients,
        clients.singleton(q!(1usize)),
        &client_aggregator,
        &replicas,
        print_result_frequency / 10,
        print_result_frequency,
        print_parseable_bench_results,
    );

    let location_id_to_cluster = HashMap::from([
        (proposers_id.clone(), "proposer".to_string()),
        (acceptors_id.clone(), "acceptor".to_string()),
        (clients_id.clone(), "client".to_string()),
        (replicas_id.clone(), "replica".to_string()),
        (
            client_aggregator_id.clone(),
            "client_aggregator".to_string(),
        ),
    ]);

    let built = builder.optimize_with(|ir| {
        inject_id(ir);
    });

    // Replay any existing rewrites.
    let existing_rewrites: Rewrites = args
        .rewrites_in
        .as_deref()
        .map(|p| load_rewrites(Path::new(p)))
        .unwrap_or_default();
    let (new_clusters_from_replay, replay_builder) = replay(existing_rewrites.clone(), built);
    let replayed_built = replay_builder.finalize();

    // Rebuild ReusableClusters with all original + replayed clusters.
    let mut reusable_clusters = ReusableClusters::default()
        .with_cluster(proposers, f + 1)
        .with_cluster(acceptors, 2 * f + 1)
        .with_cluster(clients, num_clients)
        .with_cluster(replicas, f + 1);
    for (i, (cluster, num)) in new_clusters_from_replay.into_iter().enumerate() {
        reusable_clusters = reusable_clusters.with_named(
            cluster.id().clone(),
            format!("decouple-replay-{}", i),
            num,
        );
    }
    let processes = ReusableProcesses::default().with_process(client_aggregator);

    // Load profiling and inject into IR.
    let mut post_rewrite_builder = FlowBuilder::from_built(&replayed_built);
    let mut ir = deep_clone(replayed_built.ir());
    inject_id(&mut ir);
    remove_counter(&mut ir);
    let _profiling = RunMetadata::load_and_inject(Path::new(&args.profiling), &mut ir);

    // Find the bottleneck LocationId matching the name.
    let bottleneck = find_bottleneck(&args.bottleneck, &reusable_clusters, &processes)
        .expect("bottleneck name not found");

    // Load previous BottleneckState.
    let mut state = args
        .state_in
        .as_deref()
        .map(|p| BottleneckState::load(Path::new(p)))
        .unwrap_or_default();

    let result = apply_optimizations(
        &mut ir,
        &bottleneck,
        &mut reusable_clusters,
        &processes,
        &mut post_rewrite_builder,
        &mut state,
        0,
    );

    let new_rewrite = match result {
        ApplyResult::Decoupled(r) | ApplyResult::Partitioned(r) => r,
        ApplyResult::Skip => {
            println!("Scenario skipped (no decoupling and not partitionable).");
            println!("{} 0", SCENARIO_FINISHED_PREFIX);
            return;
        }
    };

    // Save new combined rewrites + state.
    let mut combined = existing_rewrites;
    combined.push(new_rewrite);
    save_rewrites(&combined, Path::new(&args.rewrites_out));
    state.save(Path::new(&args.state_out));

    // Deploy and measure throughput at VIRTUAL_CLIENTS_MAX clients only.
    post_rewrite_builder.replace_ir(ir);
    let finalized = post_rewrite_builder.finalize();

    let mut deployment = Deployment::new();
    let host_type: HostType = if let Some(project) = args.gcp.clone() {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };
    let mut reusable_hosts = ReusableHosts::new(&host_type);

    let optimizations = Optimizations::default()
        .excluding(clients_id.clone())
        .excluding(client_aggregator_id);

    let run_metadata = deploy_and_optimize(
        &mut reusable_hosts,
        &mut deployment,
        finalized,
        reusable_clusters,
        processes,
        optimizations,
        &clients_id,
        std::cmp::max(1, VIRTUAL_CLIENTS_MAX / PHYSICAL_CLIENTS),
        Some(RUN_SECONDS),
        Some(MEASUREMENT_SECOND),
        true,
    )
    .await;

    let throughput = run_metadata.avg_throughput(START_MEASUREMENT_SECOND, MEASUREMENT_SECOND);
    run_metadata.print_run_summary(&location_id_to_cluster, MEASUREMENT_SECOND);

    // Save run metadata (CSVs + profiling JSON) next to the rewrites.
    let profiling_out = derive_profiling_path(Path::new(&args.rewrites_out));
    let output_dir = profiling_out.parent().unwrap_or(Path::new("."));
    run_metadata.save_run_metadata(
        &location_id_to_cluster,
        output_dir,
        PHYSICAL_CLIENTS,
        std::cmp::max(1, VIRTUAL_CLIENTS_MAX / PHYSICAL_CLIENTS),
        0,
    );

    println!("{} {}", SCENARIO_FINISHED_PREFIX, throughput);
}

fn find_bottleneck(
    name: &str,
    clusters: &ReusableClusters,
    processes: &ReusableProcesses,
) -> LocationId {
    clusters.find_by_name(name).unwrap_or_else(|| {
        processes
            .find_by_name(name)
            .expect("bottleneck name not found in clusters or processes")
    })
}

fn derive_profiling_path(rewrites_path: &Path) -> PathBuf {
    // rewrites_iter{i}_{name}.json → profiling_iter{i}_{name}.json
    let dir = rewrites_path.parent().unwrap_or(Path::new("."));
    let file = rewrites_path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("rewrites.json")
        .replace("rewrites_", "profiling_");
    dir.join(file)
}
