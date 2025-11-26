use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use hydro_deploy::Deployment;
use hydro_deploy::gcp::GcpNetwork;
use hydro_lang::location::Location;
use hydro_lang::viz::config::GraphConfig;
use hydro_optimize::decoupler;
use hydro_optimize::deploy::ReusableHosts;
use hydro_optimize::deploy_and_analyze::deploy_and_analyze;
use hydro_test::cluster::paxos_bench::{Aggregator, Client};
use hydro_test::cluster::two_pc::{Coordinator, Participant};
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(flatten)]
    graph: GraphConfig,

    /// Use GCP for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut deployment = Deployment::new();
    let (host_arg, project) = if let Some(project) = args.gcp {
        ("gcp".to_string(), project)
    } else {
        ("localhost".to_string(), String::new())
    };
    let network = Arc::new(RwLock::new(GcpNetwork::new(&project, None)));

    let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
    let num_participants = 3;
    let num_clients = 3;
    let num_clients_per_node = 100; // Change based on experiment between 1, 50, 100.

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

    let mut clusters = vec![
        (
            participants.id().raw_id(),
            std::any::type_name::<Participant>().to_string(),
            num_participants,
        ),
        (
            clients.id().raw_id(),
            std::any::type_name::<Client>().to_string(),
            num_clients,
        ),
    ];
    let processes = vec![
        (
            coordinator.id().raw_id(),
            std::any::type_name::<Coordinator>().to_string(),
        ),
        (
            client_aggregator.id().raw_id(),
            std::any::type_name::<Aggregator>().to_string(),
        ),
    ];

    // Deploy
    let mut reusable_hosts = ReusableHosts {
        hosts: HashMap::new(),
        host_arg,
        project: project.clone(),
        network: network.clone(),
    };

    let multi_run_metadata = RefCell::new(vec![]);
    let num_times_to_optimize = 2;
    let num_seconds_to_run = 20;

    for i in 0..num_times_to_optimize {
        let (rewritten_ir_builder, mut ir, mut decoupler, bottleneck_name, bottleneck_num_nodes) =
            deploy_and_analyze(
                &mut reusable_hosts,
                &mut deployment,
                builder,
                &clusters,
                &processes,
                vec![
                    std::any::type_name::<Client>().to_string(),
                    std::any::type_name::<Aggregator>().to_string(),
                ],
                Some(num_seconds_to_run),
                &multi_run_metadata,
                i,
            )
            .await;

        // Apply decoupling
        let mut decoupled_cluster = None;
        builder = rewritten_ir_builder.build_with(|builder| {
            let new_cluster = builder.cluster::<()>();
            decoupler.decoupled_location = new_cluster.id().clone();
            decoupler::decouple(&mut ir, &decoupler, &multi_run_metadata, i);
            decoupled_cluster = Some(new_cluster);

            ir
        });
        if let Some(new_cluster) = decoupled_cluster {
            clusters.push((
                new_cluster.id().raw_id(),
                format!("{}-decouple-{}", bottleneck_name, i),
                bottleneck_num_nodes,
            ));
        }
    }

    let built = builder.finalize();

    // Generate graphs if requested
    _ = built.generate_graph_with_config(&args.graph, None);
}
