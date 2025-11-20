use std::{cell::RefCell, collections::{HashMap, HashSet}};

use hydro_lang::{compile::ir::{HydroNode, HydroRoot, traverse_dfir}, location::dynamic::LocationId};
use syn::visit::Visit;

use crate::{partition_syn_analysis::{AnalyzeClosure, StructOrTuple}, rewrites::input_ids};

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
                if root.input_metadata().location_kind.root() == location.root() {
                    insert_dependent_node(
                        &dependent_nodes,
                        root.input_metadata().op.id,
                        *next_stmt_id,
                    );
                }
            },
            |node, next_stmt_id| {
                if node.metadata().location_kind.root() == location.root() {
                    for parent_id in input_ids(node, Some(location), cycle_source_to_sink_input) {
                        insert_dependent_node(&dependent_nodes, Some(parent_id), *next_stmt_id);
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
/// A node that has 2 parents is only partitionable if it can be partitioned on a field with a dependency to a field in each parent, and both parents can also be partitioned on those fields.
/// TODO: If the node is a KeyedStream with TotalOrder, we need to restrict partitioning to only the key fields.
fn output_to_input_fields(node: &HydroNode) -> (StructOrTuple, StructOrTuple) {
    match node {
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
        | HydroNode::Difference { .. } // Although output doesn't contain right parent, the parents join on all fields
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
        // Output contains only values from left parent, but is joined with the right parent on the key
        HydroNode::AntiJoin { .. }
        => {
            let mut right = StructOrTuple::default();
            right.add_dependency(&vec!["0".to_string()], vec![]);
            (StructOrTuple::new_completely_dependent(), right)
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

/// Whether a node affects partitionability analysis
pub enum Partitionability {
    NoEffect,
    Conditional,
    Unpartitionable,
}

/// If this node is an IDB, and it is the only non-network node in its location, can we partition that location?
/// - `NoEffect`: Yes, and we can actually partition randomly for each element.
/// - `Conditional`: Yes, we can partition on some specific keys.
/// - `Unpartitionable`: No. 
fn node_partitionability(node: &HydroNode) -> Partitionability {
    match node {
        HydroNode::Placeholder => {
            panic!()
        }
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
        | HydroNode::Filter { .. }
        | HydroNode::DeferTick { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Network { .. }
        | HydroNode::Counter { .. }
        | HydroNode::Map { .. }
        | HydroNode::FilterMap { .. }
        | HydroNode::FlatMap { .. }
        | HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. } => Partitionability::NoEffect,
        HydroNode::Join { .. }
        | HydroNode::Difference { .. }
        | HydroNode::AntiJoin { .. }
        | HydroNode::Unique { .. }
        | HydroNode::FoldKeyed { .. }
        | HydroNode::ReduceKeyed { .. }
        | HydroNode::ReduceKeyedWatermark { .. } => Partitionability::Conditional,
        HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::Sort { .. }
        | HydroNode::Scan { .. }
        | HydroNode::Fold { .. }
        | HydroNode::Reduce { .. } => Partitionability::Unpartitionable,
    }
}