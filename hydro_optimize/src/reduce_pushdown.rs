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
