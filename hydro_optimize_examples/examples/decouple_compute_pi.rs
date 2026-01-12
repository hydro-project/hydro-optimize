use std::cell::RefCell;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::debug;
use hydro_optimize::decoupler::{self, Decoupler};
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::deploy_and_analyze;
use hydro_test::cluster::compute_pi::{Leader, Worker};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, group(
    clap::ArgGroup::new("cloud")
        .args(&["gcp", "aws"])
        .multiple(false)
))]
struct Args {
    #[command(flatten)]
    graph: GraphConfig,

    /// Use Gcp for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    /// Use Aws, make sure credentials are set up
    #[arg(long, action = ArgAction::SetTrue)]
    aws: bool,
}

struct DecoupledCluster {}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut deployment = Deployment::new();

    let host_type: HostType = if let Some(project) = args.gcp {
        HostType::Gcp { project }
    } else if args.aws {
        HostType::Aws
    } else {
        HostType::Localhost
    };

    let mut reusable_hosts = ReusableHosts::new(host_type);

    let builder = hydro_lang::compile::builder::FlowBuilder::new();
    let num_cluster_nodes = 8;
    let (cluster, leader) = hydro_test::cluster::compute_pi::compute_pi(&builder, 8192);

    let clusters = vec![(
        cluster.id().raw_id(),
        std::any::type_name::<Worker>().to_string(),
        num_cluster_nodes,
    )];
    let decoupled_cluster = builder.cluster::<DecoupledCluster>();
    let processes = vec![(
        leader.id().raw_id(),
        std::any::type_name::<Leader>().to_string(),
    )];

    let decoupler = Decoupler {
        // Decouple between these operators:
        // .map(q!(|_| rand::random::<(f64, f64)>()))
        // .map(q!(|(x, y)| x * x + y * y < 1.0))
        output_to_decoupled_machine_after: vec![4],
        output_to_original_machine_after: vec![],
        place_on_decoupled_machine: vec![],
        orig_location: cluster.id().clone(),
        decoupled_location: decoupled_cluster.id().clone(),
    };

    let multi_run_metadata = RefCell::new(vec![]);
    let built = builder
        .optimize_with(|roots| decoupler::decouple(roots, &decoupler, &multi_run_metadata, 0))
        .optimize_with(debug::print_id);

    let (rewritten_ir_builder, ir, _, _, _) = deploy_and_analyze(
        &mut reusable_hosts,
        &mut deployment,
        built,
        &clusters,
        &processes,
        vec![],
        None,
        &multi_run_metadata,
        0,
    )
    .await;

    let built = rewritten_ir_builder.build_with(|_| ir).finalize();

    // Generate graphs if requested
    let _ = built.generate_graph_with_config(&args.graph, None);
}
