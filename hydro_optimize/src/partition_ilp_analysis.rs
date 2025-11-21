use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, hash_map::Entry},
};

use hydro_lang::{
    compile::ir::{HydroNode, HydroRoot, traverse_dfir},
    location::dynamic::LocationId,
};
use syn::visit::Visit;

use crate::{
    partition_syn_analysis::{AnalyzeClosure, StructOrTuple},
    rewrites::{input_ids, op_id_to_inputs},
};

/// Add id to dependent_nodes if its parent_id is already in dependent_nodes
fn insert_dependent_node(
    dependent_nodes: &RefCell<HashSet<usize>>,
    parent_id: Option<usize>,
    id: usize,
) {
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

/// Returns whether any additional dependencies were added
fn create_canonical_fields_node(
    node: Option<&HydroNode>,
    id: usize,
    op_to_parents: &HashMap<usize, Vec<usize>>,
    op_to_dependencies: &mut HashMap<usize, (StructOrTuple, StructOrTuple)>,
) -> bool {
    let mut mutated = false;

    // Compute per-operator dependencies
    let op_dependencies = op_to_dependencies.entry(id);
    let (left_dependencies, right_dependencies) = match op_dependencies {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            if let Some(node) = node {
                // Create the dependencies if necessary
                mutated = true;
                entry.insert(output_to_input_fields(&node))
            } else {
                // If we have found this node recursively, and there is no mapping yet, wait until we process this node
                return false;
            }
        }
    }
    .clone();

    let parents = op_to_parents.get(&id).unwrap();
    let left_parent = parents[0];
    // Propagate to left parent if its dependencies have been calculated
    if let Some((ll_grandparent, lr_grandparent)) = op_to_dependencies.get_mut(&left_parent) {
        let ll_grandparent_mutated = ll_grandparent.extend_parent_fields(&left_dependencies);
        let lr_grandparent_mutated = lr_grandparent.extend_parent_fields(&left_dependencies);
        if ll_grandparent_mutated || lr_grandparent_mutated {
            mutated = true;
            create_canonical_fields_node(None, left_parent, op_to_parents, op_to_dependencies);
        }
    }

    // Right parent may not exist
    if let Some(right_parent) = parents.get(1) {
        if let Some((rl_grandparent, rr_grandparent)) = op_to_dependencies.get_mut(right_parent) {
            let rl_grandparent_mutated = rl_grandparent.extend_parent_fields(&right_dependencies);
            let rr_grandparent_mutated = rr_grandparent.extend_parent_fields(&right_dependencies);
            if rl_grandparent_mutated || rr_grandparent_mutated {
                mutated = true;
                create_canonical_fields_node(
                    None,
                    *right_parent,
                    op_to_parents,
                    op_to_dependencies,
                );
            }
        }
    }

    mutated
}

/// Find all fields and subfields of each operator's outputs, based on how they are used by their children.
/// Returns a mapping from operator ID to (fields to left parent fields, fields to right parent fields)
fn create_canonical_fields(
    ir: &mut [HydroRoot],
    location: &LocationId,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> HashMap<usize, (StructOrTuple, StructOrTuple)> {
    let op_to_parents = op_id_to_inputs(ir, Some(location), cycle_source_to_sink_input);
    let mut op_to_dependencies = HashMap::new();

    loop {
        let mut fixpoint = true;
        traverse_dfir(
            ir,
            |_, _| {},
            |node, id| {
                if node.metadata().location_kind.root() == location.root() {
                    let mutated = create_canonical_fields_node(
                        Some(node),
                        *id,
                        &op_to_parents,
                        &mut op_to_dependencies,
                    );
                    fixpoint &= !mutated;
                }
            },
        );

        if fixpoint {
            break;
        }
        println!("Rerunning create_canonical_fields loop until fixpoint.");
    }

    op_to_dependencies
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

/// Whether this node introduces persistence.
/// Note: Must keep in sync with `emit_core` in hydro_lang.
fn node_persists(node: &HydroNode) -> bool {
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
        | HydroNode::YieldConcat { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::Filter { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Network { .. }
        | HydroNode::Counter { .. }
        | HydroNode::Map { .. }
        | HydroNode::FilterMap { .. }
        | HydroNode::FlatMap { .. }
        | HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::Sort { .. } => false,
        // Maybe, depending on if it's 'static (either hidden input parent is Persist)
        HydroNode::Join { left, right, .. } | HydroNode::CrossProduct { left, right, .. } => {
            matches!(left.as_ref(), HydroNode::Persist { .. })
                || matches!(right.as_ref(), HydroNode::Persist { .. })
                || left.metadata().location_kind.is_top_level()
                || right.metadata().location_kind.is_top_level()
        }
        // Maybe, depending on if it's 'static (neg parent is Persist)
        HydroNode::Difference { neg, .. } | HydroNode::AntiJoin { neg, .. } => {
            matches!(neg.as_ref(), HydroNode::Persist { .. })
                || neg.metadata().location_kind.is_top_level()
        }
        HydroNode::FoldKeyed { input, .. }
        | HydroNode::ReduceKeyed { input, .. }
        | HydroNode::ReduceKeyedWatermark { input, .. }
        | HydroNode::Scan { input, .. }
        | HydroNode::Fold { input, .. }
        | HydroNode::Reduce { input, .. } => matches!(input.as_ref(), HydroNode::Persist { .. }),
        // Maybe, depending on if it's 'static (input.metadata().location_kind.is_top_level())
        HydroNode::Enumerate { input, .. } | HydroNode::Unique { input, .. } => {
            input.metadata().location_kind.is_top_level()
        }
        HydroNode::Persist { .. } | HydroNode::DeferTick { .. } => true,
    }
}
