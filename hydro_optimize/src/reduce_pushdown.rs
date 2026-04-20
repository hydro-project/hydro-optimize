use std::collections::{HashMap, HashSet};

use hydro_lang::compile::ir::{HydroIrMetadata, HydroNode, HydroRoot, traverse_dfir};

use crate::repair::inject_id;
use crate::rewrites::collection_kind_to_debug_type;

/// Builds a Scan that performs the reduce inline, only emitting when it receives input.
/// Unlike a bare Reduce on an unbounded stream (which re-emits every tick),
/// Scan only fires when there's actual input that tick.
fn build_scan_reduce(
    input_node: HydroNode,
    reduce_f: syn::Expr,
    reduce_metadata: HydroIrMetadata,
) -> HydroNode {
    let element_type: syn::Type =
        (*collection_kind_to_debug_type(&reduce_metadata.collection_kind).0).clone();
    let loc = reduce_metadata.location_id.clone();
    let reduce_kind = reduce_metadata.collection_kind.clone();

    // Scan state: Option<T>
    // - None on first input: set to Some(input), emit Some(input)
    // - Some(acc): apply reduce_f(&mut acc, input), emit Some(acc)
    let scan_init: syn::Expr = syn::parse_quote!(|| None::<#element_type>);
    let scan_acc: syn::Expr = syn::parse_quote!({
        let reduce_fn = #reduce_f;
        move |state: &mut Option<#element_type>, new_val: #element_type| -> Option<#element_type> {
            match state {
                None => {
                    *state = Some(new_val.clone());
                    Some(new_val)
                }
                Some(acc) => {
                    reduce_fn(acc, new_val);
                    Some(acc.clone())
                }
            }
        }
    });

    HydroNode::Scan {
        init: scan_init.into(),
        acc: scan_acc.into(),
        input: Box::new(input_node),
        metadata: loc.clone().new_node_metadata(reduce_kind),
    }
}

pub fn reduce_pushdown(ir: &mut [HydroRoot], decision: HashMap<usize, usize>) {
    let mut reduce_to_insert_locations = HashMap::new();
    for (op_id, reduce_id) in decision {
        reduce_to_insert_locations
            .entry(reduce_id)
            .or_insert_with(HashSet::new)
            .insert(op_id);
    }

    // Collect reduce function and metadata for each reduce to push down
    let mut op_id_to_reduce_info: HashMap<usize, (syn::Expr, HydroIrMetadata)> = HashMap::new();
    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            if let Some(ops_to_insert) = reduce_to_insert_locations.get(op_id) {
                let HydroNode::Reduce { f, metadata, .. } = node else {
                    panic!(
                        "Expected Reduce node for op_id {op_id}, but found {:?}",
                        node
                    );
                };
                let f_expr: syn::Expr = (**f).clone();
                for reduce_id in ops_to_insert {
                    op_id_to_reduce_info.insert(*reduce_id, (f_expr.clone(), metadata.clone()));
                }
            }
        },
    );

    // Insert Scan at each target location
    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            if let Some((f, metadata)) = op_id_to_reduce_info.remove(op_id) {
                let input = std::mem::replace(node, HydroNode::Placeholder);
                *node = build_scan_reduce(input, f, metadata);
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
            |node, _| match node {
                HydroNode::Scan { .. } | HydroNode::Reduce { .. } => {
                    count += 1;
                }
                _ => {}
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

            let decision = reduce_pushdown_decision(ir);
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
