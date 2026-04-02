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
    use hydro_build_utils::assert_debug_snapshot;
    use hydro_lang::{
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

    #[test]
    fn test_reduce_pushdown() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();

        let input = cluster1
            .source_iter(q!([1, 2, 3, 4, 5]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .weaken_ordering::<NoOrder>();
        input
            .clone()
            .merge_unordered(input)
            .max()
            .sample_eager(nondet!(/** test */))
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .for_each(q!(|max_val| {
                println!("max value: {}", max_val);
            }));

        let built = builder.optimize_with(|ir| {
            inject_id(ir);
            let cycle_data = cycle_source_to_sink_input(ir);
            inject_location(ir, &cycle_data);
            let decision = reduce_pushdown_decision(ir, &cluster2.id());
            reduce_pushdown(ir, decision);
        });

        hydro_lang::compile::ir::dbg_dedup_tee(|| {
            assert_debug_snapshot!(built.ir());
        });
    }
}
