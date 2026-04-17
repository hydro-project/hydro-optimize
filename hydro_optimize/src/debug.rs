use std::collections::HashMap;

use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};

use crate::rewrites::tee_to_inner_id;

fn print_id_root(root: &mut HydroRoot, next_stmt_id: &mut usize) {
    let input = root.input_metadata().op.id;
    println!(
        "{} Root {}, Inputs: {:?}",
        next_stmt_id,
        root.print_root(),
        input,
    );
}

fn print_id_node(
    node: &mut HydroNode,
    next_stmt_id: &mut usize,
    tee_to_inner: &HashMap<usize, usize>,
) {
    let metadata = node.metadata();
    let inputs = match node {
        HydroNode::Tee { .. } => {
            vec![tee_to_inner.get(next_stmt_id).copied()]
        }
        _ => node
            .input_metadata()
            .iter()
            .map(|m| m.op.id)
            .collect::<Vec<Option<usize>>>(),
    };
    println!(
        "{} Node {}, {:?}, Cardinality: {:?}, CPU Usage: {:?}, Network Recv CPU Usage: {:?}, Inputs: {:?}",
        next_stmt_id,
        node.print_root(),
        metadata,
        metadata.cardinality,
        metadata.op.cpu_usage,
        metadata.op.network_recv_cpu_usage,
        inputs,
    );
}

pub fn print_id(ir: &mut [HydroRoot]) {
    let tee_to_inner = tee_to_inner_id(ir);
    traverse_dfir(ir, print_id_root, |node, op_id| {
        print_id_node(node, op_id, &tee_to_inner)
    });
}

fn name_to_id_node(
    node: &mut HydroNode,
    next_stmt_id: &mut usize,
    mapping: &mut HashMap<String, usize>,
) {
    if let Some(name) = &node.metadata().tag {
        mapping.insert(name.clone(), *next_stmt_id);
    }
}

// Create a mapping from named IR nodes (from `ir_node_named`) to their IDs
pub fn name_to_id_map(ir: &mut [HydroRoot]) -> HashMap<String, usize> {
    let mut mapping = HashMap::new();
    traverse_dfir(
        ir,
        |_, _| {},
        |node, next_stmt_id| name_to_id_node(node, next_stmt_id, &mut mapping),
    );
    mapping
}
