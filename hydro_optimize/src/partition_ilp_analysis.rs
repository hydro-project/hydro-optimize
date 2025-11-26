use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, hash_map::Entry},
};

use good_lp::{Expression, Variable, constraint, variable};
use hydro_lang::{
    compile::ir::{HydroNode, HydroRoot, traverse_dfir},
    location::dynamic::LocationId,
};
use syn::visit::Visit;

use crate::{
    decouple_analysis::{DecoupleILPMetadata, node_is_in_bottleneck},
    partition_node_analysis::all_inputs,
    partition_syn_analysis::{AnalyzeClosure, StructOrTuple, StructOrTupleIndex},
    rewrites::{op_id_to_parents, parent_ids},
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
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
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
                    for parent_id in parent_ids(node, Some(location), cycle_source_to_sink_parent) {
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
fn output_to_parent_fields(node: &HydroNode) -> (StructOrTuple, StructOrTuple) {
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
                entry.insert(output_to_parent_fields(&node))
            } else {
                // If we have found this node recursively, and there is no mapping yet, wait until we process this node
                return false;
            }
        }
    }
    .clone();
    let dependencies = vec![left_dependencies, right_dependencies];

    // Propagate up each parent
    for (i, parent) in op_to_parents.get(&id).unwrap().iter().enumerate() {
        if let Some((l_grandparent, r_grandparent)) = op_to_dependencies.get_mut(parent) {
            let l_grandparent_mutated = l_grandparent.extend_parent_fields(&dependencies[i]);
            let r_grandparent_mutated = r_grandparent.extend_parent_fields(&dependencies[i]);
            if l_grandparent_mutated || r_grandparent_mutated {
                mutated = true;
                create_canonical_fields_node(
                    None,
                    *parent,
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
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
) -> HashMap<usize, (StructOrTuple, StructOrTuple)> {
    let op_to_parents = op_id_to_parents(ir, Some(location), cycle_source_to_sink_parent);
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
        // Maybe, depending on if it's 'static (either hidden parent is Persist)
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

struct PartitionILPMetadata {
    op_id_to_field_vars: HashMap<usize, HashMap<StructOrTupleIndex, Variable>>, // op_id: field_name: variable
    op_id_to_partition_expr: HashMap<usize, Expression>, // op_id: 1 if the op is partitioned on any of its fields, 0 otherwise
    num_relevant_operators: HashMap<usize, Expression>, // location: number of relevant operators
    partitionable_operators: HashMap<usize, Expression>, // location: number of partitionable operators. Partitioning is possible at the location if partitionable_operators == num_relevant_operators
    num_persist_operators: HashMap<usize, Expression>, // location: number of nodes where node_persists() == true. If 0, then partitioning is always possible
}

/// Add the operator with `id` to the location_sum for each location
fn add_op_to_location_sum(
    id: usize,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    location_sum: &mut HashMap<usize, Expression>,
) {
    for loc in 0..decoupling_metadata.borrow().num_locations {
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
    parents: &Vec<usize>,
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
    let parent_vars = op_id_to_var.get(parents.get(0).unwrap()).unwrap();
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

        is_input_expr = is_input_expr + is_loc_input;
    }

    // Divide by 2 since if the parent's location differs from this nodes location, is_loc_input will be 1 for 2 different locations
    is_input_expr / 2
}

fn field_vars_from_op(
    op_id: usize,
    canonical_fields: &HashMap<usize, (StructOrTuple, StructOrTuple)>,
    op_id_to_field_vars: &mut HashMap<usize, HashMap<StructOrTupleIndex, Variable>>,
    op_id_to_partition_expr: &mut HashMap<usize, Expression>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) -> HashMap<StructOrTupleIndex, Variable> {
    op_id_to_field_vars
        .entry(op_id)
        .or_insert_with(|| {
            // Find all fields that could possibly be referenced by this operator's children
            let (l_dependencies, r_dependencies) = canonical_fields.get(&op_id).unwrap();
            let mut field_names = l_dependencies.get_all_nested_fields();
            field_names.extend(r_dependencies.get_all_nested_fields());

            let mut field_to_var = HashMap::new();
            let mut sum_expr = Expression::default();
            for field_name in field_names {
                let var = decoupling_metadata
                    .borrow_mut()
                    .variables
                    .add(variable().binary());
                field_to_var.insert(field_name.clone(), var);
                sum_expr = sum_expr + var;
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
    canonical_fields: &HashMap<usize, (StructOrTuple, StructOrTuple)>,
    metadata: &mut PartitionILPMetadata,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
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

    // Only track r_parent if this node has 2 parents
    let (l_parent_dependencies, r_parent_dependencies) = canonical_fields.get(&op_id).unwrap();
    let mut parent_dependencies = vec![l_parent_dependencies];
    if parents.len() == 2 {
        parent_dependencies.push(r_parent_dependencies);
    }

    // Create parent field vars
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

    // Get expr that is 1 if this node is an input, 0 otherwise
    let is_input_expr = add_is_input_expr(op_id, parents, decoupling_metadata);

    let DecoupleILPMetadata { variables, constraints, op_id_to_var, .. } = &mut *decoupling_metadata.borrow_mut();

    let mut is_partitionable = Expression::default();
    for (field_name, field_var) in field_vars {
        // If there's a link from this field to all parents, constrain it to the corresponding field in all parents
        // Otherwise, only allow using this field for partitioning if this is an input node
        let mut corresponding_parent_field_vars = vec![vec![]; parents.len()];
        for (left_or_right, parent_dependency) in parent_dependencies.iter().enumerate() {
            // Does this field point to something in this parent? If so, find the vars
            if let Some(corresponding_parent_nested_field) =
                parent_dependency.get_dependencies(&field_name)
            {
                for parent_field in &corresponding_parent_nested_field.get_dependency() {
                    corresponding_parent_field_vars[left_or_right].push(
                        parent_field_vars
                            .get(left_or_right)
                            .unwrap()
                            .get(parent_field)
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
                    parent_var_sum = parent_var_sum + *parent_var;
                }
                constraints.push(constraint!(field_var <= parent_var_sum + is_input_expr.clone()));
            }
        }
        else {
            // At least 1 parent doesn't have a corresponding field. Can only partition on this field if this is an input node
            constraints.push(constraint!(field_var <= is_input_expr.clone()));
        }

        // This node is partitionable if ANY field is partitionable
        is_partitionable = is_partitionable + field_var;
    }

    // Add to partitionable operators
    for (loc, partitionable_expr) in partitionable_operators {
        let is_at_loc_and_partitionable = variables.add(variable().binary());
        let is_at_loc = op_id_to_var.get(&op_id).unwrap().get(loc).unwrap();

        constraints.push(constraint!(is_at_loc_and_partitionable <= is_partitionable.clone()));
        constraints.push(constraint!(is_at_loc_and_partitionable <= *is_at_loc));

        let temp_expr = std::mem::take(partitionable_expr);
        *partitionable_expr = temp_expr + is_at_loc_and_partitionable;
    }
}

fn partition_ilp_node_analysis(
    node: &HydroNode,
    op_id: usize,
    idbs: &HashSet<usize>,
    canonical_fields: &HashMap<usize, (StructOrTuple, StructOrTuple)>,
    metadata: &mut PartitionILPMetadata,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
) {
    let network_type = decoupling_metadata
        .borrow()
        .network_ids
        .get(&op_id)
        .cloned();
    if !node_is_in_bottleneck(node, op_id, &network_type, decoupling_metadata) {
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
    if idbs.contains(&op_id) && affect_partitionability {
        add_op_to_location_sum(op_id, decoupling_metadata, num_relevant_operators);
    }

    // Partitioning is possible if no node in the location persists
    if node_persists(node) {
        add_op_to_location_sum(op_id, decoupling_metadata, num_persist_operators);
    }

    constrain_field_vars_to_parents(
        op_id,
        op_id_to_parents,
        canonical_fields,
        metadata,
        decoupling_metadata,
    );
}

fn zero_cost_if_expr_sums_to_zero(
    loc_to_expr: &HashMap<usize, Expression>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    max_num_ops: usize,
) {
    let DecoupleILPMetadata {
        cpu_usages,
        variables,
        constraints,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    for (loc, expr) in loc_to_expr {
        cpu_usages.entry(*loc).and_modify(|cpu_usage| {
            // If expr == 0, set cost to 0, otherwise, keep original cost
            // Enforce with big-M constraints, where M = max_num_ops + 1:
            //   expr <= M * zero_if_sums_to_zero
            //   expr >= zero_if_sums_to_zero
            let zero_if_sums_to_zero = variables.add(variable().binary());
            constraints.push(constraint!(
                expr.clone() <= (max_num_ops + 1) as f64 * zero_if_sums_to_zero
            ));
            constraints.push(constraint!(expr.clone() >= zero_if_sums_to_zero));

            // Similar constraints for CPU, where M = 1.0 (100% CPU usage)
            let prev_cpu_usage = std::mem::take(cpu_usage);
            let cpu_usage_var = variables.add(variable().min(0));
            constraints.push(constraint!(cpu_usage_var <= zero_if_sums_to_zero));
            constraints.push(constraint!(
                cpu_usage_var >= prev_cpu_usage - 1 + zero_if_sums_to_zero
            ));

            *cpu_usage = Expression::from(cpu_usage_var);
        });
    }
}

fn zero_cost_if_partitionable(
    num_relevant_operators: &HashMap<usize, Expression>,
    partitionable_operators: &HashMap<usize, Expression>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    max_num_ops: usize,
) {
    let DecoupleILPMetadata {
        variables,
        constraints,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let mut loc_to_difference = HashMap::new();
    for (loc, num_relevant) in num_relevant_operators {
        let num_partitionable = partitionable_operators.get(loc).unwrap();

        // Difference > 0 if num_relevant != num_partitionable
        let difference_var = variables.add(variable());
        constraints.push(constraint!(
            difference_var >= num_relevant.clone() - num_partitionable.clone()
        ));
        constraints.push(constraint!(
            difference_var >= num_partitionable.clone() - num_relevant.clone()
        ));
        loc_to_difference.insert(*loc, Expression::from(difference_var));
    }

    zero_cost_if_expr_sums_to_zero(&loc_to_difference, decoupling_metadata, max_num_ops);
}

pub(crate) fn partition_ilp_analysis(
    ir: &mut [HydroRoot],
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    // Make all cost expressions at all locations default to 0
    let location_to_zero_expr: HashMap<usize, Expression> =
        (0..decoupling_metadata.borrow().num_locations)
            .map(|loc| (loc, Expression::from(0)))
            .collect();
    let mut metadata = PartitionILPMetadata {
        op_id_to_field_vars: HashMap::new(),
        op_id_to_partition_expr: HashMap::new(),
        num_relevant_operators: location_to_zero_expr.clone(),
        partitionable_operators: location_to_zero_expr.clone(),
        num_persist_operators: location_to_zero_expr,
    };

    let inputs = all_inputs(ir, &decoupling_metadata.borrow().bottleneck);
    let idbs = nodes_dependent_on_inputs(
        ir,
        &decoupling_metadata.borrow().bottleneck,
        &inputs,
        cycle_source_to_sink_parent,
    );
    let canonical_fields = create_canonical_fields(
        ir,
        &decoupling_metadata.borrow().bottleneck,
        cycle_source_to_sink_parent,
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

    let num_ops = decoupling_metadata.borrow().op_id_to_var.len();

    // Set cost to 0 if all relevant operators are partitionable
    zero_cost_if_partitionable(
        &metadata.num_relevant_operators,
        &metadata.partitionable_operators,
        decoupling_metadata,
        num_ops,
    );
    // Set cost to 0 if no nodes persist
    zero_cost_if_expr_sums_to_zero(
        &metadata.num_persist_operators,
        decoupling_metadata,
        num_ops,
    );
}
