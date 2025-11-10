use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use hydro_deploy::Deployment;
use hydro_deploy::gcp::GcpNetwork;
use hydro_lang::viz::config::GraphConfig;
use hydro_lang::location::Location;
use hydro_lang::prelude::FlowBuilder;
use hydro_optimize::decoupler;
use hydro_optimize::deploy::ReusableHosts;
use hydro_optimize::deploy_and_analyze::deploy_and_analyze;
use hydro_optimize_examples::simple_graphs::{Client, Server, get_graph_function};
use hydro_optimize_examples::simple_graphs_bench::{Aggregator, simple_graphs_bench, simple_graphs_bench_no_union};
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(flatten)]
    graph: GraphConfig,

    /// Use GCP for deployment (provide project name)
    #[arg(long)]
    gcp: Option<String>,

    #[arg(long)]
    function: String,
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

    let mut builder = FlowBuilder::new();
    let num_clients = 10;
    let num_clients_per_node = 1000;
    let graph_function = get_graph_function(&args.function);
    let server = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();

    simple_graphs_bench(
        num_clients_per_node,
        &server,
        &clients,
        &client_aggregator,
        graph_function,
    );
    // simple_graphs_bench_no_union(
    //     num_clients_per_node,
    //     &server,
    //     &clients,
    //     &client_aggregator,
    // );

    let mut clusters = vec![
        (
            server.id().raw_id(),
            std::any::type_name::<Server>().to_string(),
            1,
        ),
        (
            clients.id().raw_id(),
            std::any::type_name::<Client>().to_string(),
            num_clients,
        ),
    ];
    let processes = vec![(
        client_aggregator.id().raw_id(),
        std::any::type_name::<Aggregator>().to_string(),
    )];

    // Deploy
    let mut reusable_hosts = ReusableHosts {
        hosts: HashMap::new(),
        host_arg,
        project: project.clone(),
        network: network.clone(),
    };

    let num_times_to_optimize = 2;
    let num_seconds_to_profile = Some(60);
    let multi_run_metadata = RefCell::new(vec![]);

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
                num_seconds_to_profile,
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
                format!("{}_decouple_{}", bottleneck_name, i),
                bottleneck_num_nodes,
            ));
        }
    }

    let built = builder.finalize();

    // Generate graphs if requested
    _ = built.generate_graph_with_config(&args.graph, None);
}