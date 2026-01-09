use std::cell::RefCell;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::deploy_and_analyze;

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

struct Process {}
struct Cluster {}

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
    let num_cluster_nodes = 2;
    let (process, cluster) = hydro_test::cluster::simple_cluster::simple_cluster(&builder);

    let clusters = vec![(
        cluster.id().raw_id(),
        std::any::type_name::<Cluster>().to_string(),
        num_cluster_nodes,
    )];
    let processes = vec![(
        process.id().raw_id(),
        std::any::type_name::<Process>().to_string(),
    )];

    let multi_run_metadata = RefCell::new(vec![]);

    let (rewritten_ir_builder, ir, _, _, _) = deploy_and_analyze(
        &mut reusable_hosts,
        &mut deployment,
        builder.finalize(),
        &clusters,
        &processes,
        vec![std::any::type_name::<Process>().to_string()],
        None,
        &multi_run_metadata,
        0,
    )
    .await;

    let built = rewritten_ir_builder.build_with(|_| ir).finalize();

    // Generate graphs if requested
    _ = built.generate_graph_with_config(&args.graph, None);
}
