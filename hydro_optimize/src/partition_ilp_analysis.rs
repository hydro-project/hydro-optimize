use std::{cell::RefCell, collections::{BTreeMap, BTreeSet, HashMap, HashSet}};

use hydro_lang::{compile::ir::{HydroNode, HydroRoot, traverse_dfir}, location::dynamic::LocationId};
use syn::{token::Struct, visit::Visit};

use crate::partition_syn_analysis::{AnalyzeClosure, StructOrTuple};

/// Add id to dependent_nodes if its parent_id is already in dependent_nodes
fn insert_dependent_node(dependent_nodes: &RefCell<HashSet<usize>>, parent_id: Option<usize>, id: usize) {
    let mut dependent_nodes_borrow = dependent_nodes.borrow_mut();
    if let Some(parent_id) = parent_id {
        if dependent_nodes_borrow.contains(&parent_id) {
            dependent_nodes_borrow.insert(id);
        }
    }
}

/// Returns IDs of nodes that are dependent on at least 1 input
fn nodes_dependent_on_inputs(
    ir: &mut [HydroRoot],
    location: &LocationId,
    inputs: &Vec<usize>,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> HashSet<usize> {
    let dependent_nodes = RefCell::new(HashSet::from_iter(inputs.iter().cloned()));
    let mut num_dependent_nodes = inputs.len();

    loop {
        traverse_dfir(
            ir,
            |root, next_stmt_id| {
                // Don't check location, but should be fine because its parent HydroNode should check location
                insert_dependent_node(&dependent_nodes, root.input_metadata().op.id, *next_stmt_id);
            },
            |node, next_stmt_id| {
                if node.metadata().location_kind.root() == location.root() {
                    match node {
                        HydroNode::Tee { inner, ..} => {
                            insert_dependent_node(&dependent_nodes, inner.0.borrow().op_metadata().id, *next_stmt_id);
                        }
                        HydroNode::CycleSource { .. } => {
                            insert_dependent_node(&dependent_nodes, cycle_source_to_sink_input.get(next_stmt_id).cloned(), *next_stmt_id);
                        }
                        _ => {
                            node.input_metadata().iter().for_each(|metadata| {
                                insert_dependent_node(&dependent_nodes, metadata.op.id, *next_stmt_id);
                            });
                        }
                    }
                }
            },
        );

        if dependent_nodes.borrow().len() == num_dependent_nodes {
            // No new dependent nodes found, reached fixpoint
            break;
        }
        num_dependent_nodes = dependent_nodes.borrow().len();
        println!("Rerunning nodes_dependent_on_inputs loop until fixpoint.");
    }

    dependent_nodes.take()
}

/// Given a node type, return how its output fields is dependent on its left and right parents.
fn output_to_input_fields(node: &HydroNode) -> (StructOrTuple, StructOrTuple) {
    match &node {
        HydroNode::Placeholder => {
            panic!()
        }
        // Completely dependent on the left (or only) parent
        HydroNode::Cast { .. }
        | HydroNode::ObserveNonDet { .. }
        | HydroNode::Source { .. }
        | HydroNode::SingletonSource { .. }
        | HydroNode::CycleSource { .. }
        | HydroNode::Tee { .. }
        | HydroNode::Persist { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::Difference { .. } // Output contains only values from the left parent
        | HydroNode::AntiJoin { .. } // Output contains only values from the left parent
        | HydroNode::Filter { .. } // No changes to output
        | HydroNode::DeferTick { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::Unique { .. }
        | HydroNode::Sort { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Network { .. }
        | HydroNode::Counter { .. }
        => (StructOrTuple::new_completely_dependent(), StructOrTuple::default()),
        // Requires syn analysis
        HydroNode::Map { f, .. }
        | HydroNode::FilterMap { f, .. }
        => {
            let mut analyzer = AnalyzeClosure::default();
            analyzer.visit_expr(&f.0);

            // Keep topmost none field if this is FilterMap
            let keep_topmost_none_fields = matches!(node, HydroNode::FilterMap { .. });
            let parent_dependencies = analyzer.output_dependencies.remove_none_fields(keep_topmost_none_fields).unwrap_or_default();
            (parent_dependencies, StructOrTuple::default())
        }
        // Output contains the entirety of both parents
        HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. }
        => (StructOrTuple::new_completely_dependent(), StructOrTuple::new_completely_dependent()),
        // Result is (left parent, right parent)
        HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. }
        => {
            let mut left = StructOrTuple::default();
            left.add_dependency(&vec!["0".to_string()], vec![]);
            let mut right = StructOrTuple::default();
            right.add_dependency(&vec!["1".to_string()], vec![]);
            (left, right)
        },
        // (k, (v1, v2))
        HydroNode::Join { .. }
        => {
            let mut left = StructOrTuple::default();
            left.add_dependency(&vec!["0".to_string()], vec!["0".to_string()]);
            left.add_dependency(&vec!["1".to_string(), "0".to_string()], vec!["1".to_string()]);
            let mut right = StructOrTuple::default();
            right.add_dependency(&vec!["0".to_string()], vec!["0".to_string()]);
            right.add_dependency(&vec!["1".to_string(), "1".to_string()], vec!["1".to_string()]);
            (left, right)
        },
        // (index, input)
        HydroNode::Enumerate { .. }
        => {
            let mut left = StructOrTuple::default();
            left.add_dependency(&vec!["1".to_string()], vec![]);
            (left, StructOrTuple::default())
        }
        // Only the key is preserved
        HydroNode::FoldKeyed { .. }
        | HydroNode::ReduceKeyed { .. }
        | HydroNode::ReduceKeyedWatermark { .. }
        => {
            let mut left = StructOrTuple::default();
            left.add_dependency(&vec!["0".to_string()], vec!["0".to_string()]);
            (left, StructOrTuple::default())
        }
        // No mapping
        HydroNode::FlatMap { .. }
        | HydroNode::Scan { .. }
        | HydroNode::Fold { .. }
        | HydroNode::Reduce { .. }
        => (StructOrTuple::default(), StructOrTuple::default()),
    }
}