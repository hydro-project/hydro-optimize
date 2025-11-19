use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use hydro_lang::{
    compile::ir::{HydroNode, HydroRoot, traverse_dfir},
    location::dynamic::LocationId,
};
use syn::visit::Visit;

use crate::{
    partition_syn_analysis::{AnalyzeClosure, StructOrTuple},
    rewrites::{NetworkType, get_network_type, input_ids},
};

/// Add id to dependent_on_input if its parent_id is already in dependent_on_input
fn insert_dependent_node(
    dependent_on_input: &RefCell<HashSet<usize>>,
    op_to_sources: &RefCell<HashMap<usize, HashSet<usize>>>,
    source_to_dependents: &RefCell<HashMap<usize, HashSet<usize>>>,
    parent_id: Option<usize>,
    id: usize,
) {
    let mut dependent_on_input_borrow = dependent_on_input.borrow_mut();
    if let Some(parent_id) = parent_id {
        // If the parent is an IDB, so is this node
        if dependent_on_input_borrow.contains(&parent_id) {
            dependent_on_input_borrow.insert(id);
        }

        // This node depends on all sources its parent depends on
        let parent_sources = op_to_sources
            .borrow()
            .get(&parent_id)
            .cloned()
            .unwrap_or_default();
        // Add node as dependent to all the parent's sources
        for source in &parent_sources {
            source_to_dependents
                .borrow_mut()
                .entry(*source)
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        // Add to sources of this node
        op_to_sources
            .borrow_mut()
            .entry(id)
            .or_insert_with(HashSet::new)
            .extend(parent_sources);
    }
}

/// Checks whether the EDB source of a node should become relevant, then returns the ID of the node whose source should become relevant.
/// The source should become relevant if:
/// 1. The output of the component is an EDB (with no IDB dependencies), or
/// 2. There is a negation with an EDB on the positive edge.
///
/// Note: This function should only run after all inputs dependencies have been labeled in `dependent_on_input`, such that only more EDBs will become IDBs (but not vice versa).
fn should_node_source_become_relevant(
    node: &HydroNode,
    location: &LocationId,
    dependent_on_input: &RefCell<HashSet<usize>>,
) -> Option<usize> {
    match node {
        HydroNode::AntiJoin { pos, neg, .. } | HydroNode::Difference { pos, neg, .. } => {
            // If the positive edge is an EDB and the negative edge is an IDB
            let pos_id = pos.op_metadata().id.unwrap();
            let neg_id = neg.op_metadata().id.unwrap();
            if !dependent_on_input.borrow().contains(&pos_id)
                && dependent_on_input.borrow().contains(&neg_id)
            {
                Some(pos_id)
            } else {
                None
            }
        }
        HydroNode::Network { input, .. } => {
            // If this is an output
            match get_network_type(node, location.raw_id()) {
                Some(NetworkType::Send) | Some(NetworkType::SendRecv) => {
                    let parent_id = input.op_metadata().id.unwrap();
                    // If this output only depends on EDBs
                    if !dependent_on_input.borrow().contains(&parent_id) {
                        Some(parent_id)
                    } else {
                        None
                    }
                }
                Some(NetworkType::Recv) | None => None,
            }
        }
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
        | HydroNode::Counter { .. }
        | HydroNode::Map { .. }
        | HydroNode::FilterMap { .. }
        | HydroNode::FlatMap { .. }
        | HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. }
        | HydroNode::Join { .. }
        | HydroNode::Unique { .. }
        | HydroNode::FoldKeyed { .. }
        | HydroNode::ReduceKeyed { .. }
        | HydroNode::ReduceKeyedWatermark { .. }
        | HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::Sort { .. }
        | HydroNode::Scan { .. }
        | HydroNode::Fold { .. }
        | HydroNode::Reduce { .. } => None,
    }
}

/// See `should_node_source_become_relevant`.
fn should_root_source_become_relevant(
    root: &HydroRoot,
    dependent_on_input: &RefCell<HashSet<usize>>,
) -> Option<usize> {
    match root {
        HydroRoot::SendExternal { input, .. } | HydroRoot::DestSink { input, .. } => {
            let parent_id = input.op_metadata().id.unwrap();
            // If this output only depends on EDBs
            if !dependent_on_input.borrow().contains(&parent_id) {
                Some(parent_id)
            } else {
                None
            }
        }
        HydroRoot::ForEach { .. } | HydroRoot::CycleSink { .. } => None,
    }
}

/// Mark all EDB sources of `node_id`, and their dependents, as `dependent_on_input`.
fn make_source_relevant(
    node_id: usize,
    op_to_sources: &RefCell<HashMap<usize, HashSet<usize>>>,
    source_to_dependents: &RefCell<HashMap<usize, HashSet<usize>>>,
    dependent_on_input: &RefCell<HashSet<usize>>,
) {
    // Find all sources
    for source in op_to_sources.borrow().get(&node_id).unwrap() {
        // Mark all its dependents as dependent on input
        for dependent in source_to_dependents.borrow().get(&source).unwrap() {
            dependent_on_input.borrow_mut().insert(*dependent);
        }
    }
}

/// Returns IDs of nodes that are dependent on at least 1 input.
/// Labels EDBs as inputs if they cannot be replicated.
/// - `sources`: EDBs (Source) nodes. The source (and its dependents) may become relevant if either:
/// 1. The output is an EDB (with no IDB dependencies), or
/// 2. There is a negation with an EDB on the positive edge.
fn partition_relevant_nodes(
    ir: &mut [HydroRoot],
    location: &LocationId,
    inputs: &Vec<usize>,
    sources: &Vec<usize>,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> HashSet<usize> {
    let dependent_on_input = RefCell::new(HashSet::from_iter(inputs.iter().cloned()));
    let mut num_dependent_on_input = inputs.len();
    let op_to_sources = RefCell::new(HashMap::new());
    let source_to_dependents = RefCell::new(HashMap::new());

    // Each source depends on itself
    for source in sources {
        op_to_sources
            .borrow_mut()
            .insert(*source, HashSet::from([*source]));
        source_to_dependents
            .borrow_mut()
            .insert(*source, HashSet::from([*source]));
    }

    // 1. Find all nodes tainted by inputs without considering relevant EDBs. Run until fixpoint.
    loop {
        traverse_dfir(
            ir,
            |root, next_stmt_id| {
                if root.input_metadata().location_kind.root() == location.root() {
                    insert_dependent_node(
                        &dependent_on_input,
                        &op_to_sources,
                        &source_to_dependents,
                        root.input_metadata().op.id,
                        *next_stmt_id,
                    );
                }
            },
            |node, next_stmt_id| {
                if node.metadata().location_kind.root() == location.root() {
                    for parent_id in input_ids(node, Some(location), cycle_source_to_sink_input) {
                        insert_dependent_node(
                            &dependent_on_input,
                            &op_to_sources,
                            &source_to_dependents,
                            Some(parent_id),
                            *next_stmt_id,
                        );
                    }
                }
            },
        );

        if dependent_on_input.borrow().len() == num_dependent_on_input {
            // No new dependent nodes found, reached fixpoint
            break;
        }
        num_dependent_on_input = dependent_on_input.borrow().len();
        println!("Rerunning nodes_dependent_on_inputs input dependencies loop until fixpoint.");
    }

    // 2. Find relevant EDBs and propagate changes. Run again until fixpoint.
    loop {
        traverse_dfir(
            ir,
            |root, _| {
                if root.input_metadata().location_kind.root() == location.root() {
                    if let Some(parent) =
                        should_root_source_become_relevant(root, &dependent_on_input)
                    {
                        make_source_relevant(
                            parent,
                            &op_to_sources,
                            &source_to_dependents,
                            &dependent_on_input,
                        );
                    }
                }
            },
            |node, _| {
                if node.metadata().location_kind.root() == location.root() {
                    if let Some(parent) =
                        should_node_source_become_relevant(node, location, &dependent_on_input)
                    {
                        make_source_relevant(
                            parent,
                            &op_to_sources,
                            &source_to_dependents,
                            &dependent_on_input,
                        );
                    }
                }
            },
        );

        if dependent_on_input.borrow().len() == num_dependent_on_input {
            // No new dependent nodes found, reached fixpoint
            break;
        }
        num_dependent_on_input = dependent_on_input.borrow().len();
        println!("Rerunning nodes_dependent_on_inputs relevant EDBs loop until fixpoint.");
    }

    dependent_on_input.take()
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

/// If this node is relevant, and it is the only non-network node in its location, can we partition that location?
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
