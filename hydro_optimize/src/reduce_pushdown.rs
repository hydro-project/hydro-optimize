use std::collections::{HashMap, HashSet};

use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};

use crate::repair::inject_id;

fn add_reduce(node: &mut HydroNode, mut reduce: HydroNode) {
    let HydroNode::Reduce { input, .. } = &mut reduce else {
        panic!("reduce_pushdown expected a Reduce node");
    };
    let node_content = std::mem::replace(node, HydroNode::Placeholder);
    **input = node_content;
    *node = reduce;
}

pub fn reduce_pushdown(ir: &mut [HydroRoot], decision: HashMap<usize, usize>) {
    let mut reduce_to_insert_locations = HashMap::new();
    for (op_id, reduce_id) in decision {
        reduce_to_insert_locations
            .entry(reduce_id)
            .or_insert_with(HashSet::new)
            .insert(op_id);
    }

    // Create new Reduce nodes
    let mut op_id_to_reduce = HashMap::new();
    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            if let Some(ops_to_insert) = reduce_to_insert_locations.get(op_id) {
                let HydroNode::Reduce { f, metadata, .. } = node else {
                    return;
                };
                for reduce_id in ops_to_insert {
                    let reduce_node = HydroNode::Reduce {
                        f: f.clone(),
                        input: Box::new(HydroNode::Placeholder), // Will be replaced
                        metadata: metadata.clone(),
                    };
                    op_id_to_reduce.insert(*reduce_id, reduce_node);
                }
            }
        },
    );

    // Insert the Reduce nodes
    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            if let Some(reduce_node) = op_id_to_reduce.remove(op_id) {
                add_reduce(node, reduce_node);
            }
        },
    );

    // Fix IDs
    inject_id(ir);
}

#[cfg(test)]
mod tests {
    use hydro_lang::{
        compile::ir::{HydroNode, traverse_dfir},
        live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder},
        location::Location,
        nondet::nondet,
        prelude::{FlowBuilder, TCP},
    };
    use stageleft::q;

    use crate::{
        reduce_pushdown::reduce_pushdown,
        reduce_pushdown_analysis::reduce_pushdown_decision,
        repair::{cycle_source_to_sink_input, inject_id, inject_location},
    };

    fn count_reduces(ir: &mut [hydro_lang::compile::ir::HydroRoot]) -> usize {
        let mut count = 0;
        traverse_dfir(
            ir,
            |_, _| {},
            |node, _| {
                if matches!(node, HydroNode::Reduce { .. }) {
                    count += 1;
                }
            },
        );
        count
    }

    #[tokio::test]
    async fn test_reduce_pushdown() {
        use futures::StreamExt;
        use hydro_deploy::Deployment;

        let mut builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let gateway = builder.process::<()>();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();

        let input = cluster1
            .source_iter(q!([1, 2, 3, 4, 5]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .weaken_ordering::<NoOrder>();
        let output = input
            .clone()
            .merge_unordered(input)
            .max()
            .sample_eager(nondet!(/** test */))
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .send(&gateway, TCP.fail_stop().bincode())
            .values()
            .send_bincode_external(&external);

        let built = builder.optimize_with(|ir| {
            inject_id(ir);
            let cycle_data = cycle_source_to_sink_input(ir);
            inject_location(ir, &cycle_data);

            let before = count_reduces(ir);

            let decision = reduce_pushdown_decision(ir, &cluster2.id());
            assert!(!decision.is_empty(), "Expected non-empty pushdown decision");
            reduce_pushdown(ir, decision);

            let after = count_reduces(ir);
            assert!(
                after > before,
                "Expected more Reduce nodes after pushdown ({before} -> {after})"
            );
        });

        let mut deployment = Deployment::new();
        let nodes = built
            .with_process(&gateway, deployment.Localhost())
            .with_cluster(&cluster1, vec![deployment.Localhost()])
            .with_cluster(&cluster2, vec![deployment.Localhost()])
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();
        let mut output = nodes.connect(output).await;
        deployment.start().await.unwrap();

        let val: i32 = tokio::time::timeout(std::time::Duration::from_secs(30), output.next())
            .await
            .expect("timeout waiting for output")
            .expect("output channel closed");
        assert_eq!(val, 5);
    }
}
