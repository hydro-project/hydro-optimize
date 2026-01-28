use std::cell::RefCell;

use clap::{ArgAction, Parser};
use hydro_deploy::Deployment;
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::deploy::{HostType, ReusableHosts};
use hydro_optimize::deploy_and_analyze::deploy_and_optimize;
use hydro_test::cluster::two_pc::{Coordinator, Participant};
use hydro_test::cluster::two_pc_bench::{Aggregator, Client};

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
    let num_participants = 3;
    let num_clients = 3;
    let num_clients_per_node = 100;

    let coordinator = builder.process();
    let participants = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();

    hydro_test::cluster::two_pc_bench::two_pc_bench(
        num_clients_per_node,
        &coordinator,
        &participants,
        num_participants,
        &clients,
        &client_aggregator,
    );

    let clusters = vec![
        (
            participants.id().key(),
            std::any::type_name::<Participant>().to_string(),
            num_participants,
        ),
        (
            clients.id().key(),
            std::any::type_name::<Client>().to_string(),
            num_clients,
        ),
    ];
    let processes = vec![
        (
            coordinator.id().key(),
            std::any::type_name::<Coordinator>().to_string(),
        ),
        (
            client_aggregator.id().key(),
            std::any::type_name::<Aggregator>().to_string(),
        ),
    ];

    let multi_run_metadata = RefCell::new(vec![]);

    let (rewritten_ir_builder, ir, _, _, _) = deploy_and_optimize(
        &mut reusable_hosts,
        &mut deployment,
        builder.finalize(),
        &clusters,
        &processes,
        vec![
            std::any::type_name::<Client>().to_string(),
            std::any::type_name::<Aggregator>().to_string(),
        ],
        None,
        &multi_run_metadata,
        0,
    )
    .await;

    let built = rewritten_ir_builder.build_with(|_| ir).finalize();

    // Generate graphs if requested
    _ = built.generate_graph_with_config(&args.graph, None);
}
