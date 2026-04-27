use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, hash_map::Entry},
};

use good_lp::{Expression, Variable, constraint, variable};
use hydro_lang::{
    compile::ir::{CollectionKind, HydroNode, HydroRoot, StreamOrder, traverse_dfir},
    location::dynamic::LocationId,
};
use syn::visit::Visit;

use crate::{
    decouple_analysis::DecoupleILPMetadata,
    partition_node_analysis::all_inputs,
    partition_syn_analysis::{AnalyzeClosure, StructOrTuple, StructOrTupleIndex},
    rewrites::node_at_location,
};

/// Add id to dependent_nodes if its parent_id is already in dependent_nodes
fn insert_dependent_node(
    dependent_nodes: &RefCell<HashSet<usize>>,
    parent_id: Option<usize>,
    id: usize,
) {
    let mut dependent_nodes_borrow = dependent_nodes.borrow_mut();
    if let Some(parent_id) = parent_id
        && dependent_nodes_borrow.contains(&parent_id)
    {
        dependent_nodes_borrow.insert(id);
    }
}

/// Returns IDs of nodes that are dependent on at least 1 input
fn nodes_dependent_on_inputs(
    ir: &mut [HydroRoot],
    location: &LocationId,
    inputs: &[usize],
    op_to_parents: &HashMap<usize, Vec<usize>>,
) -> HashSet<usize> {
    let dependent_nodes = RefCell::new(HashSet::from_iter(inputs.iter().cloned()));
    let mut num_dependent_nodes = inputs.len();

    loop {
        traverse_dfir(
            ir,
            |root, next_stmt_id| {
                if root.input_metadata().location_id.root() == location {
                    insert_dependent_node(
                        &dependent_nodes,
                        root.input_metadata().op.id,
                        *next_stmt_id,
                    );
                }
            },
            |node, next_stmt_id| {
                if node_at_location(node, location)
                    && let Some(parents) = op_to_parents.get(next_stmt_id)
                {
                    for parent_id in parents {
                        insert_dependent_node(&dependent_nodes, Some(*parent_id), *next_stmt_id);
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

/// Given a node type, return how its output fields is dependent on its parents.
/// Must contain an entry for each parent, even if there is no dependency.
/// Contains 1 entry if there are no parents.
/// A node that has 2 parents is only partitionable if it can be partitioned on a field with a dependency to a field in each parent, and both parents can also be partitioned on those fields.
fn output_to_parent_fields(node: &HydroNode) -> Vec<StructOrTuple> {
    match node {
        HydroNode::Placeholder => {
            panic!()
        }
        // Completely dependent on the only parent, or no parent
        HydroNode::Cast { .. }
        | HydroNode::ObserveNonDet { .. }
        | HydroNode::Source { .. }
        | HydroNode::SingletonSource { .. }
        | HydroNode::CycleSource { .. }
        | HydroNode::Tee { .. }
        | HydroNode::Partition { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::Filter { .. } // No changes to output
        | HydroNode::DeferTick { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::Unique { .. }
        | HydroNode::Sort { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Network { .. }
        | HydroNode::Counter { .. }
        => vec![StructOrTuple::new_completely_dependent()],
        // Requires syn analysis
        HydroNode::Map { f, .. }
        | HydroNode::FilterMap { f, .. }
        => {
            let mut analyzer = AnalyzeClosure::default();
            analyzer.visit_expr(&f.0);

            // Keep topmost none field if this is FilterMap
            let keep_topmost_none_fields = matches!(node, HydroNode::FilterMap { .. });
            let parent_dependencies = analyzer.output_dependencies.remove_none_fields(keep_topmost_none_fields).unwrap_or_default();
            vec![parent_dependencies]
        }
        // Output contains the entirety of both parents
        HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. }
        | HydroNode::Difference { .. } // Although output doesn't contain right parent, the parents join on all fields
        => vec![StructOrTuple::new_completely_dependent(), StructOrTuple::new_completely_dependent()],
        // Result is (left parent, right parent)
        HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. }
        => {
            let mut left = StructOrTuple::default();
            left.add_dependency(&vec!["0".to_string()], vec![]);
            let mut right = StructOrTuple::default();
            right.add_dependency(&vec!["1".to_string()], vec![]);
            vec![left, right]
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
            vec![left, right]
        },
        // Output contains only values from left parent, but is joined with the right parent on the key
        HydroNode::AntiJoin { .. }
        => {
            let mut right = StructOrTuple::default();
            right.add_dependency(&vec!["0".to_string()], vec![]);
            vec![StructOrTuple::new_completely_dependent(), right]
        },
        // (index, input)
        HydroNode::Enumerate { .. }
        => {
            let mut parent = StructOrTuple::default();
            parent.add_dependency(&vec!["1".to_string()], vec![]);
            vec![parent]
        }
        // Only the key is preserved
        HydroNode::FoldKeyed { .. }
        | HydroNode::ReduceKeyed { .. }
        | HydroNode::ReduceKeyedWatermark { .. }
        => {
            let mut parent = StructOrTuple::default();
            parent.add_dependency(&vec!["0".to_string()], vec!["0".to_string()]);
            vec![parent]
        }
        // No mapping
        HydroNode::FlatMap { .. }
        | HydroNode::FlatMapStreamBlocking { .. }
        | HydroNode::Scan { .. }
        | HydroNode::ScanAsyncBlocking { .. }
        | HydroNode::Fold { .. }
        | HydroNode::Reduce { .. }
        => vec![StructOrTuple::default()],
    }
}

/// Returns whether any additional dependencies were added.
/// node = None during recursion
fn create_canonical_fields_node(
    node: Option<&HydroNode>,
    id: usize,
    op_to_parents: &HashMap<usize, Vec<usize>>,
    op_to_dependencies: &mut HashMap<usize, Vec<StructOrTuple>>,
) -> bool {
    let mut mutated = false;

    // Compute per-operator dependencies
    let op_dependencies = op_to_dependencies.entry(id);
    let dependencies = match op_dependencies {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            if let Some(node) = node {
                // Create the dependencies if necessary
                mutated = true;
                entry.insert(output_to_parent_fields(node))
            } else {
                // If we have found this node recursively, and there is no mapping yet, wait until we process this node
                return false;
            }
        }
    }
    .clone();
    // println!("Found dependencies for op {}: {:?}", id, dependencies);

    let parents = op_to_parents.get(&id).unwrap();
    // println!("Parents of op {}: {:?}", id, parents);

    // Propagate up each parent
    for (parent_index, parent) in parents.iter().enumerate() {
        let mut parent_mutated = false;
        // See if the parent's dependencies need to be updated
        if let Some(parent_dependencies) = op_to_dependencies.get_mut(parent) {
            for parent_dependency in parent_dependencies {
                // println!(
                //     "Found dependencies for parent {} of op {}: {:?}",
                //     parent, id, parent_dependency
                // );

                let mut dependency = dependencies[parent_index].clone();
                // println!(
                //     "Parent fields of op {} for parent {}: {:?}",
                //     id,
                //     parent,
                //     dependency.get_all_nested_dependencies()
                // );
                if parents.len() > 1 {
                    // Project dependencies to the other parent onto this dependency, see if we can learn about some more fields
                    // For example, if this is an AntiJoin, then we know that [] -> [] on the pos side and 0 -> [] on the neg side
                    // We should then learn that for the pos side, 0 exists as a field
                    let other_dependency_fields =
                        dependencies[1 - parent_index].get_all_nested_fields();
                    for field in other_dependency_fields {
                        dependency.create_child(field);
                    }
                    // println!(
                    //     "Parent fields of op {} for parent {} after projection: {:?}",
                    //     id,
                    //     parent,
                    //     dependency.get_all_nested_dependencies()
                    // );
                }

                for field in dependency.get_all_nested_dependencies() {
                    let (_, mutated) = parent_dependency.create_child(field.clone());
                    parent_mutated |= mutated;
                }
                // println!(
                //     "New dependencies for parent {} of op {}: {:?}",
                //     parent, id, parent_dependency
                // );
            }
        }

        if parent_mutated {
            mutated = true;
            // println!("Recursing from op {} to parent {}", recurse_id, parent);
            create_canonical_fields_node(None, *parent, op_to_parents, op_to_dependencies);
        } else {
            // println!("No mutation for parent {}, exiting {}", parent, id);
        }
    }

    mutated
}

/// Find all fields and subfields of each operator's outputs, based on how they are used by their children.
/// Returns a mapping from operator ID to (fields to left parent fields, fields to right parent fields)
fn create_canonical_fields(
    ir: &mut [HydroRoot],
    location: &LocationId,
    op_to_parents: &HashMap<usize, Vec<usize>>,
) -> HashMap<usize, Vec<StructOrTuple>> {
    let mut op_to_dependencies = HashMap::new();

    loop {
        let mut fixpoint = true;
        traverse_dfir(
            ir,
            |_, _| {},
            |node, id| {
                if node_at_location(node, location) {
                    let mutated = create_canonical_fields_node(
                        Some(node),
                        *id,
                        op_to_parents,
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

    println!("Canonical fields: {:?}", op_to_dependencies);

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
        | HydroNode::Partition { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
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
        | HydroNode::FlatMapStreamBlocking { .. }
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
        | HydroNode::ScanAsyncBlocking { .. }
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
        | HydroNode::Partition { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::Filter { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Network { .. }
        | HydroNode::Counter { .. }
        | HydroNode::Map { .. }
        | HydroNode::FilterMap { .. }
        | HydroNode::FlatMap { .. }
        | HydroNode::FlatMapStreamBlocking { .. }
        | HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::Sort { .. } => false,
        // Maybe, depending on if it's 'static (either hidden parent is top_level)
        HydroNode::Join { left, right, .. } | HydroNode::CrossProduct { left, right, .. } => {
            left.metadata().location_id.is_top_level()
                || right.metadata().location_id.is_top_level()
        }
        // Maybe, depending on if it's 'static (neg parent is top_level)
        HydroNode::Difference { neg, .. } | HydroNode::AntiJoin { neg, .. } => {
            neg.metadata().location_id.is_top_level()
        }
        // Maybe, depending on if it's 'static (input.metadata().location_id.is_top_level())
        HydroNode::FoldKeyed { input, .. }
        | HydroNode::ReduceKeyed { input, .. }
        | HydroNode::ReduceKeyedWatermark { input, .. }
        | HydroNode::Scan { input, .. }
        | HydroNode::ScanAsyncBlocking { input, .. }
        | HydroNode::Fold { input, .. }
        | HydroNode::Reduce { input, .. }
        | HydroNode::Enumerate { input, .. }
        | HydroNode::Unique { input, .. } => input.metadata().location_id.is_top_level(),
        HydroNode::DeferTick { .. } => true,
    }
}

/// Whether a node's collection_kind requires TotalOrder.
fn node_has_total_order(node: &HydroNode) -> bool {
    matches!(
        node.metadata().collection_kind,
        CollectionKind::Stream {
            order: StreamOrder::TotalOrder,
            ..
        } | CollectionKind::KeyedStream {
            value_order: StreamOrder::TotalOrder,
            // NOTE: This is unnecessarily conservative; we really just need to restrict partitioning to the key fields. Unfortunately that's somewhat hard to express, since this operator is not part of the set of operators considered during partitioning
            ..
        }
    )
}

/// For a TotalOrder child node, add ILP constraints that prevent partitioning
/// at any location where a parent sends to this child across a location boundary.
fn add_total_order_edge_constraints(
    child_id: usize,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    num_total_order_operators: &mut HashMap<usize, Expression>,
) {
    let parents = op_id_to_parents.get(&child_id).unwrap();
    if parents.is_empty() {
        return;
    }

    let DecoupleILPMetadata {
        op_id_to_var,
        variables,
        constraints,
        max_num_locations: num_locations,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    for parent_id in parents {
        let parent_vars = op_id_to_var.get(parent_id).unwrap();
        let child_vars = op_id_to_var.get(&child_id).unwrap();

        for loc in 0..*num_locations {
            let parent_at_loc = *parent_vars.get(&loc).unwrap();
            let child_at_loc = *child_vars.get(&loc).unwrap();

            // e == 1 iff parent is at loc AND child is NOT at loc
            let e = variables.add(variable().binary());
            constraints.push(constraint!(e <= parent_at_loc));
            constraints.push(constraint!(e <= 1 - child_at_loc));
            constraints.push(constraint!(e >= parent_at_loc - child_at_loc));

            num_total_order_operators.entry(loc).and_modify(|expr| {
                let temp = std::mem::take(expr);
                *expr = temp + e;
            });
        }
    }
}

pub(crate) struct PartitionILPMetadata {
    pub(crate) op_id_to_field_vars: HashMap<usize, HashMap<StructOrTupleIndex, Variable>>, // op_id: field_name: variable
    op_id_to_partition_expr: HashMap<usize, Expression>, // op_id: 1 if the op is partitioned on any of its fields, 0 otherwise
    pub(crate) num_relevant_operators: HashMap<usize, Expression>, // location: number of relevant operators
    pub(crate) partitionable_operators: HashMap<usize, Expression>, // location: number of partitionable operators. Partitioning is possible at the location if partitionable_operators == num_relevant_operators
    pub(crate) num_persist_operators: HashMap<usize, Expression>, // location: number of nodes where node_persists() == true. If 0, then partitioning is always possible
    pub(crate) num_total_order_operators: HashMap<usize, Expression>, // location: number of nodes where node_has_total_order() == true. If > 0, partitioning is impossible
    pub(crate) can_partition: HashMap<usize, Variable>, // location: 1 iff location is partitionable ((all relevant partitionable OR no persist) AND no total-order)
}

/// Add the operator with `id` to the location_sum for each location
fn add_op_to_location_sum(
    id: usize,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    location_sum: &mut HashMap<usize, Expression>,
) {
    for loc in 0..decoupling_metadata.borrow().max_num_locations {
        let op_var = *decoupling_metadata
            .borrow()
            .op_id_to_var
            .get(&id)
            .unwrap()
            .get(&loc)
            .unwrap();

        location_sum.entry(loc).and_modify(|expr| {
            let temp_expr = std::mem::take(expr);
            *expr = temp_expr + op_var;
        });
    }
}

fn add_is_input_expr(
    id: usize,
    parents: &[usize],
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) -> Expression {
    let DecoupleILPMetadata {
        op_id_to_var,
        variables,
        constraints,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let mut is_input_expr = Expression::default();

    // Only consider the 1st parent, since there's the invariant that both parents must have the same location
    let parent_vars = op_id_to_var.get(parents.first().unwrap()).unwrap();
    for (loc, var) in op_id_to_var.get(&id).unwrap() {
        let is_loc_input = variables.add(variable().binary());
        let parent_var = parent_vars.get(loc).unwrap();
        // Force is_input_var == |var - parent_var| for binary vars.
        constraints.push(constraint!(is_loc_input >= *var - *parent_var));
        constraints.push(constraint!(is_loc_input >= *parent_var - *var));
        // Force is_loc_input to be 0 if both vars are 0
        constraints.push(constraint!(is_loc_input <= *var + *parent_var));
        // Force is_loc_input to be 0 if both vars are 1
        constraints.push(constraint!(is_loc_input <= 2.0 - (*var + *parent_var)));

        is_input_expr += is_loc_input;
    }

    // Divide by 2 since if the parent's location differs from this nodes location, is_loc_input will be 1 for 2 different locations
    is_input_expr / 2
}

fn field_vars_from_op(
    op_id: usize,
    canonical_fields: &HashMap<usize, Vec<StructOrTuple>>,
    op_id_to_field_vars: &mut HashMap<usize, HashMap<StructOrTupleIndex, Variable>>,
    op_id_to_partition_expr: &mut HashMap<usize, Expression>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) -> HashMap<StructOrTupleIndex, Variable> {
    op_id_to_field_vars
        .entry(op_id)
        .or_insert_with(|| {
            // Find all fields that could possibly be referenced by this operator's children
            let dependencies = canonical_fields.get(&op_id).unwrap();
            let mut field_names = dependencies[0].get_all_nested_fields();
            for dep in &dependencies[1..] {
                field_names.extend(dep.get_all_nested_fields());
            }

            let mut field_to_var = HashMap::new();
            let mut sum_expr = Expression::default();
            for field_name in field_names {
                let var = decoupling_metadata
                    .borrow_mut()
                    .variables
                    .add(variable().binary());
                field_to_var.insert(field_name.clone(), var);
                sum_expr += var;
            }
            op_id_to_partition_expr.insert(op_id, sum_expr.clone());

            // Constrain sum to 1 (the op is partitioned on at most 1 field)
            decoupling_metadata
                .borrow_mut()
                .constraints
                .push(constraint!(sum_expr <= 1));

            field_to_var
        })
        .clone()
}

fn constrain_field_vars_to_parents(
    op_id: usize,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    canonical_fields: &HashMap<usize, Vec<StructOrTuple>>,
    metadata: &mut PartitionILPMetadata,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    is_relevant: bool,
) {
    let PartitionILPMetadata {
        op_id_to_field_vars,
        op_id_to_partition_expr,
        partitionable_operators,
        ..
    } = metadata;

    // Create field vars
    let field_vars = field_vars_from_op(
        op_id,
        canonical_fields,
        op_id_to_field_vars,
        op_id_to_partition_expr,
        decoupling_metadata,
    );

    // Create field vars for parents
    let parents = op_id_to_parents.get(&op_id).unwrap();
    if parents.is_empty() {
        // Must be a Source, no constraints
        return;
    }
    // println!("Node id {}, parents {:?}", op_id, parents);

    let dependencies_on_parents = canonical_fields.get(&op_id).unwrap();
    // println!("Canonical fields: {:?}", dependencies_on_parents);
    let parent_field_vars = parents
        .iter()
        .map(|parent_id| {
            field_vars_from_op(
                *parent_id,
                canonical_fields,
                op_id_to_field_vars,
                op_id_to_partition_expr,
                decoupling_metadata,
            )
        })
        .collect::<Vec<_>>();
    // println!("Parent fields: {:?}", parent_field_vars);

    // Get expr that is 1 if this node is an input, 0 otherwise
    let is_input_expr = add_is_input_expr(op_id, parents, decoupling_metadata);

    let DecoupleILPMetadata {
        variables,
        constraints,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let mut is_partitionable = Expression::default();
    for (field_name, field_var) in field_vars {
        // println!("Op field name: {:?}", field_name.join("."));
        // If there's a link from this field to all parents, constrain it to the corresponding field in all parents
        // Otherwise, only allow using this field for partitioning if this is an input node
        let mut corresponding_parent_field_vars = vec![vec![]; parents.len()];
        for (left_or_right, dependencies_on_parent) in dependencies_on_parents.iter().enumerate() {
            // For this field and its children, what fields in the parent does it depend on?
            // Note: We can't unwrap here, because
            // 1. A field_name and field_var exists for each field of the op that depends on EITHER parent
            // 2. dependencies_on_parent corresponds to only one of those parents, and may not contain field_name
            if let Some(nested_dependencies_in_field) =
                dependencies_on_parent.get_dependencies(&field_name)
            {
                // For this field and NOT its children, what fields in the parent does it depend on?
                let dependencies_in_field = nested_dependencies_in_field.get_dependency();
                // println!("Corresponding parent fields: {:?}", dependencies_in_field);
                for parent_field_name in &dependencies_in_field {
                    // Add the corresponding parent field var
                    corresponding_parent_field_vars[left_or_right].push(
                        parent_field_vars
                            .get(left_or_right)
                            .unwrap()
                            .get(parent_field_name)
                            .unwrap(),
                    );
                }
            }
        }

        let all_parents_have_corresponding_fields = corresponding_parent_field_vars
            .iter()
            .all(|vars| !vars.is_empty());
        if all_parents_have_corresponding_fields {
            // For each field:
            // 1. Both parents must partition on some corresponding field
            // 2. If a parent has multiple corresponding fields, it only needs to partition on one of them
            // 3. If this node is an input, no constraints
            for parent_vars in corresponding_parent_field_vars {
                let mut parent_var_sum = Expression::default();
                for parent_var in parent_vars {
                    parent_var_sum += *parent_var;
                }
                constraints.push(constraint!(
                    field_var <= parent_var_sum + is_input_expr.clone()
                ));
            }
        } else {
            // At least 1 parent doesn't have a corresponding field. Can only partition on this field if this is an input node
            constraints.push(constraint!(field_var <= is_input_expr.clone()));
        }

        // This node is partitionable if ANY field is partitionable
        is_partitionable += field_var;
    }

    // Add to partitionable operators only if this op is relevant (IDB that affects partitionability)
    if is_relevant {
        for (loc, partitionable_expr) in partitionable_operators {
            let is_at_loc_and_partitionable = variables.add(variable().binary());
            let is_at_loc = op_id_to_var.get(&op_id).unwrap().get(loc).unwrap();

            constraints.push(constraint!(
                is_at_loc_and_partitionable <= is_partitionable.clone()
            ));
            constraints.push(constraint!(is_at_loc_and_partitionable <= *is_at_loc));

            let temp_expr = std::mem::take(partitionable_expr);
            *partitionable_expr = temp_expr + is_at_loc_and_partitionable;
        }
    }
}

fn partition_ilp_node_analysis(
    node: &HydroNode,
    op_id: usize,
    idbs: &HashSet<usize>,
    canonical_fields: &HashMap<usize, Vec<StructOrTuple>>,
    metadata: &mut PartitionILPMetadata,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
) {
    if !node_at_location(node, &decoupling_metadata.borrow().bottleneck) {
        return;
    }

    let PartitionILPMetadata {
        num_relevant_operators,
        num_persist_operators,
        ..
    } = metadata;

    // A node is relevant if it is an IDB and affects partitionability
    let affect_partitionability = match node_partitionability(node) {
        Partitionability::NoEffect => false,
        Partitionability::Conditional | Partitionability::Unpartitionable => true,
    };
    // TODO: Known inaccuracy: if a node is not an IDB but flows into the positive edge of a negation (for example an anti-join), then it should also be relevant (and partitioned) to avoid producing too many outputs
    let is_relevant = idbs.contains(&op_id) && affect_partitionability;
    if is_relevant {
        add_op_to_location_sum(op_id, decoupling_metadata, num_relevant_operators);
    }

    // Partitioning is possible if no node in the location persists
    if node_persists(node) {
        add_op_to_location_sum(op_id, decoupling_metadata, num_persist_operators);
    }

    // Partitioning is impossible if a TotalOrder child is at a different location
    if !matches!(node, HydroNode::Network { .. }) && node_has_total_order(node) {
        add_total_order_edge_constraints(
            op_id,
            op_id_to_parents,
            decoupling_metadata,
            &mut metadata.num_total_order_operators,
        );
    }

    constrain_field_vars_to_parents(
        op_id,
        op_id_to_parents,
        canonical_fields,
        metadata,
        decoupling_metadata,
        is_relevant,
    );
}

/// Derive `can_partition[loc]` from the accumulated per-location expressions.
/// Partitionable iff (all relevant ops partitionable OR no persist) AND no total-order.
fn calculate_partitionable(
    metadata: &mut PartitionILPMetadata,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let max_num_ops = decoupling_metadata.borrow().op_id_to_var.len();
    let big_m = (max_num_ops + 1) as f64;
    let num_locations = decoupling_metadata.borrow().max_num_locations;
    let DecoupleILPMetadata {
        variables,
        constraints,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    for loc in 0..num_locations {
        // all_partitionable: 1 iff num_relevant == partitionable
        let diff = variables.add(variable().min(0));
        let num_relevant = metadata
            .num_relevant_operators
            .get(&loc)
            .cloned()
            .unwrap_or_default();
        let num_partitionable = metadata
            .partitionable_operators
            .get(&loc)
            .cloned()
            .unwrap_or_default();
        constraints.push(constraint!(
            diff >= num_relevant.clone() - num_partitionable.clone()
        ));
        constraints.push(constraint!(diff >= num_partitionable - num_relevant));

        let all_partitionable = variables.add(variable().binary());
        constraints.push(constraint!(diff <= big_m * (1 - all_partitionable)));
        constraints.push(constraint!(diff >= all_partitionable));

        // no_persist: 1 iff num_persist == 0
        let num_persist = metadata
            .num_persist_operators
            .get(&loc)
            .cloned()
            .unwrap_or_default();
        let has_persist = variables.add(variable().binary());
        constraints.push(constraint!(num_persist.clone() <= big_m * has_persist));
        constraints.push(constraint!(num_persist >= has_persist));
        let no_persist = variables.add(variable().binary());
        constraints.push(constraint!(no_persist == 1 - has_persist));

        // no_total_order: 1 iff num_total_order == 0
        let num_total_order = metadata
            .num_total_order_operators
            .get(&loc)
            .cloned()
            .unwrap_or_default();
        let has_total_order = variables.add(variable().binary());
        constraints.push(constraint!(
            num_total_order.clone() <= big_m * has_total_order
        ));
        constraints.push(constraint!(num_total_order >= has_total_order));

        // or_cond = all_partitionable OR no_persist
        let or_cond = variables.add(variable().binary());
        constraints.push(constraint!(or_cond >= all_partitionable));
        constraints.push(constraint!(or_cond >= no_persist));
        constraints.push(constraint!(or_cond <= all_partitionable + no_persist));

        // can_partition = or_cond AND no_total_order
        let can_partition = variables.add(variable().binary());
        constraints.push(constraint!(can_partition <= or_cond));
        constraints.push(constraint!(can_partition <= 1 - has_total_order));
        constraints.push(constraint!(
            can_partition >= or_cond + (1 - has_total_order) - 1
        ));
        metadata.can_partition.insert(loc, can_partition);
    }
}

pub(crate) fn partition_ilp_analysis(
    ir: &mut [HydroRoot],
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) -> PartitionILPMetadata {
    // Make all cost expressions at all locations default to 0
    let location_to_zero_expr: HashMap<usize, Expression> =
        (0..decoupling_metadata.borrow().max_num_locations)
            .map(|loc| (loc, Expression::from(0)))
            .collect();
    let mut metadata = PartitionILPMetadata {
        op_id_to_field_vars: HashMap::new(),
        op_id_to_partition_expr: HashMap::new(),
        num_relevant_operators: location_to_zero_expr.clone(),
        partitionable_operators: location_to_zero_expr.clone(),
        num_persist_operators: location_to_zero_expr.clone(),
        num_total_order_operators: location_to_zero_expr,
        can_partition: HashMap::new(),
    };

    let inputs = all_inputs(ir, &decoupling_metadata.borrow().bottleneck);
    let idbs = nodes_dependent_on_inputs(
        ir,
        &decoupling_metadata.borrow().bottleneck,
        &inputs,
        op_id_to_parents,
    );
    let canonical_fields = create_canonical_fields(
        ir,
        &decoupling_metadata.borrow().bottleneck,
        op_id_to_parents,
    );

    traverse_dfir(
        ir,
        |_, _| {},
        |node, id| {
            partition_ilp_node_analysis(
                node,
                *id,
                &idbs,
                &canonical_fields,
                &mut metadata,
                decoupling_metadata,
                op_id_to_parents,
            );
        },
    );

    calculate_partitionable(&mut metadata, decoupling_metadata);

    metadata
}

/// Apply resource-budget constraints: the total number of machines across all
/// locations must equal `budget`. Each used location consumes at least 1
/// machine; partitionable locations may consume more (each extra machine is an
/// additional partition that divides CPU).
///
/// Returns `is_n_partitions[loc][n]` – per-location binary variables where
/// `is_n_partitions[loc][n] = 1` iff location `loc` is assigned exactly `n`
/// machines (partitions). The caller reads these from the solved ILP to
/// populate `Rewrite::num_partitions`.
///
/// When `partition_metadata` is `None` (partitioning disabled), only the
/// location-budget constraint is added and every used location gets exactly 1
/// machine.
pub(crate) fn apply_budget_constraints(
    budget: usize,
    partition_metadata: Option<&PartitionILPMetadata>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) -> Vec<Vec<Variable>> {
    let max_num_locations = decoupling_metadata.borrow().max_num_locations;

    // --- loc_used[loc]: 1 iff any operator is assigned to location loc ---
    let mut loc_used: Vec<Variable> = Vec::with_capacity(max_num_locations);
    {
        let DecoupleILPMetadata {
            op_id_to_var,
            variables,
            constraints,
            ..
        } = &mut *decoupling_metadata.borrow_mut();

        for loc in 0..max_num_locations {
            let used = variables.add(variable().binary());
            let mut sum_at_loc = Expression::default();
            for op_vars in op_id_to_var.values() {
                if let Some(&v) = op_vars.get(&loc) {
                    constraints.push(constraint!(used >= v));
                    sum_at_loc += v;
                }
            }
            constraints.push(constraint!(used <= sum_at_loc));
            loc_used.push(used);
        }
    }

    // If partitioning is disabled, each used location gets exactly 1 machine.
    // Budget constraint: sum(loc_used) <= budget.
    let Some(pm) = partition_metadata else {
        let num_unique_locs: Expression = loc_used.iter().copied().map(Expression::from).sum();
        decoupling_metadata
            .borrow_mut()
            .constraints
            .push(constraint!(num_unique_locs <= budget as f64));
        // Return trivial is_n_partitions: each loc has [loc_used] (0 or 1 machine).
        let is_n_partitions: Vec<Vec<Variable>> = (0..max_num_locations)
            .map(|loc| vec![loc_used[loc]])
            .collect();
        return is_n_partitions;
    };

    // --- is_n_partitions[loc][n]: binary, 1 iff location loc uses exactly n machines ---
    // n ranges from 0 (unused) to budget (all machines at one location).
    let mut is_n_partitions: Vec<Vec<Variable>> = Vec::with_capacity(max_num_locations);

    {
        let DecoupleILPMetadata {
            variables,
            constraints,
            ..
        } = &mut *decoupling_metadata.borrow_mut();

        // Build is_n_partitions per location
        for loc in 0..max_num_locations {
            let mut vars_for_loc = Vec::with_capacity(budget + 1);
            let mut sum_vars = Expression::default();

            for num_partitions in 0..=budget {
                let has_n_partitions = variables.add(variable().binary());

                // For a location, num_partitions=0 means that this location has nothing on it
                // So it has 0 partitions if loc_used=0
                if num_partitions == 0 {
                    constraints.push(constraint!(has_n_partitions == 1 - loc_used[loc]));
                } else {
                    // If location is unused (0), then num_partitions = 0
                    constraints.push(constraint!(has_n_partitions <= loc_used[loc]));
                }
                // num_partitions >= 2 requires partitionable
                if num_partitions >= 2 {
                    constraints.push(constraint!(has_n_partitions <= *pm.can_partition.get(&loc).unwrap()));
                }

                sum_vars += has_n_partitions;
                vars_for_loc.push(has_n_partitions);
            }

            // Exactly one choice of num_partitions per location
            constraints.push(constraint!(sum_vars == 1));
            is_n_partitions.push(vars_for_loc);
        }

        // Budget: sum of machines across all locations = budget
        let mut total_machines = Expression::default();
        for loc in 0..max_num_locations {
            for num_partitions in 0..=budget {
                total_machines += Expression::from(is_n_partitions[loc][num_partitions]) * num_partitions as f64;
            }
        }
        constraints.push(constraint!(total_machines <= budget as f64));
    }

    // --- CPU division constraints ---
    // For each location, effective_cpu = cpu / num_partitions when
    // is_n_partitions[loc][num_partitions]=1 and num_partitions>=1.
    {
        let DecoupleILPMetadata {
            cpu_usages,
            variables,
            constraints,
            ..
        } = &mut *decoupling_metadata.borrow_mut();

        // Big-M for CPU division linearization. Must exceed the maximum possible
        // cpu_usage at any single location (op costs + decoupling overhead).
        // A generous constant suffices; an overly large M only affects solver
        // performance, not correctness.
        let cpu_big_m = 5.0_f64;

        for loc in 0..max_num_locations {
            let cpu = cpu_usages.get(&loc).cloned().unwrap_or_default();
            let effective_cpu = variables.add(variable().min(0));

            for num_partitions in 1..=budget {
                constraints.push(constraint!(
                    effective_cpu >= cpu.clone() * (1.0 / num_partitions as f64)
                        - cpu_big_m * (1 - is_n_partitions[loc][num_partitions])
                ));
            }

            cpu_usages.insert(loc, Expression::from(effective_cpu));
        }
    }

    is_n_partitions
}
