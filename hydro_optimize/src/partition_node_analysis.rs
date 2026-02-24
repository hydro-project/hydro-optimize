use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};

use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};
use hydro_lang::location::dynamic::LocationId;
use syn::visit::Visit;

use super::rewrites::{NetworkType, get_network_type};
use crate::partition_syn_analysis::{AnalyzeClosure, StructOrTuple, StructOrTupleIndex};
use crate::rewrites::op_id_to_inputs;

/// Create a mapping from all input nodes to their parents (across locations)
fn all_inputs_parents(
    ir: &mut [HydroRoot],
    inputs: &[usize],
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> BTreeMap<usize, usize> {
    op_id_to_inputs(ir, None, cycle_source_to_sink_input)
        .iter()
        .filter_map(|(op_id, parents)| {
            if inputs.contains(op_id) {
                Some((*op_id, parents[0])) // Each input only has one parent
            } else {
                None
            }
        })
        .collect()
}

/// Find all input nodes of a location
fn all_inputs(ir: &mut [HydroRoot], location: &LocationId) -> Vec<usize> {
    let mut inputs = vec![];

    traverse_dfir(ir,
    |_, _| {},
    |node, next_stmt_id| match get_network_type(node, &location.root().key()) {
        Some(NetworkType::Recv) | Some(NetworkType::SendRecv) => {
            inputs.push(*next_stmt_id);
        }
        _ => {}
    },);

    inputs
}

pub struct InputDependencyMetadata {
    // Const fields
    pub location: LocationId,
    pub inputs: Vec<usize>,
    pub op_id_to_parents: HashMap<usize, Vec<usize>>,
    // Variables
    pub optimistic_phase: bool, /* If true, tuple intersection continues even if one side does not exist */
    pub input_taint: BTreeMap<usize, BTreeSet<usize>>, /* op_id -> set of input op_ids that taint this node */
    pub input_dependencies: BTreeMap<usize, BTreeMap<usize, StructOrTuple>>, /* op_id -> (input op_id -> index of input in output) */
    pub syn_analysis: BTreeMap<usize, StructOrTuple>, /* Cached results for analyzing f for each operator */
}

impl Hash for InputDependencyMetadata {
    // Only consider input_taint and input_dependencies
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input_taint.hash(state);
        self.input_dependencies.hash(state);
    }
}

fn union(
    tuple1: Option<&StructOrTuple>,
    tuple1index: &StructOrTupleIndex,
    tuple2: Option<&StructOrTuple>,
    tuple2index: &StructOrTupleIndex,
) -> Option<StructOrTuple> {
    if let (Some(t1), Some(t2)) = (tuple1, tuple2)
        && let Some(t1_child) = t1.get_dependencies(tuple1index)
        && let Some(t2_child) = t2.get_dependencies(tuple2index)
    {
        return StructOrTuple::union(&t1_child, &t2_child);
    }
    None
}

fn input_dependency_analysis_node(
    node: &mut HydroNode,
    next_stmt_id: &mut usize,
    metadata: &mut InputDependencyMetadata,
) {
    // Filter unrelated nodes
    if metadata.location != *node.metadata().location_id.root() {
        return;
    }

    println!("Analyzing node {} {:?}", next_stmt_id, node.print_root());
    let parent_ids = metadata
        .op_id_to_parents
        .get(next_stmt_id)
        .cloned()
        .unwrap_or_default();

    let InputDependencyMetadata {
        inputs,
        optimistic_phase,
        input_taint,
        input_dependencies,
        syn_analysis,
        ..
    } = metadata;

    // Calculate input taints, find parent input dependencies
    let mut parent_input_dependencies: BTreeMap<usize, BTreeMap<usize, StructOrTuple>> =
        BTreeMap::new(); // input_id -> parent position (0,1,etc) -> dependencies
    let mut parent_taints: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new(); // input_id -> parents (positions) tainted by the input
    for (index, parent_id) in parent_ids.iter().enumerate() {
        if inputs.contains(parent_id) {
            // Parent is an input
            input_taint
                .entry(*next_stmt_id)
                .or_default()
                .insert(*parent_id);
            parent_taints.entry(*parent_id).or_default().insert(index);
            parent_input_dependencies
                .entry(*parent_id)
                .or_default()
                .insert(index, StructOrTuple::new_completely_dependent());
        } else if let Some(existing_parent_taints) = input_taint.get(parent_id).cloned() {
            // Otherwise, extend the parent's
            for input in &existing_parent_taints {
                parent_taints.entry(*input).or_default().insert(index);
            }
            input_taint
                .entry(*next_stmt_id)
                .or_default()
                .extend(existing_parent_taints);
            if let Some(parent_dependencies) = input_dependencies.get(parent_id) {
                // If the parent has dependencies for the input it's tainted by, add them
                for (input_id, parent_dependencies_on_input) in parent_dependencies {
                    parent_input_dependencies
                        .entry(*input_id)
                        .or_default()
                        .insert(index, parent_dependencies_on_input.clone());
                }
            }
        }

        // Difference & AntiJoin have 2 parents, but the 2nd parent influences neither its taint nor dependencies, so don't consider it
        match node {
            HydroNode::Difference { .. } | HydroNode::AntiJoin { .. } => {
                break;
            }
            _ => {}
        }
    }
    println!("Parents of node {}: {:?}", next_stmt_id, parent_ids);
    println!(
        "Parent input dependencies for node {}: {:?}",
        next_stmt_id, parent_input_dependencies
    );

    // Calculate input dependencies
    let input_taint_entry = input_taint.entry(*next_stmt_id).or_default();
    let input_dependencies_entry = input_dependencies.entry(*next_stmt_id).or_default();
    match &node {
        // 1:1 to parent
        HydroNode::CycleSource { .. }
        | HydroNode::Tee { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::DeferTick { .. }
        | HydroNode::Unique { .. }
        | HydroNode::Sort { .. }
        | HydroNode::Difference { .. } // [a,b,c] difference [c,d] = [a,b]. Since only a subset of the 1st input is taken, we only care about its dependencies
        | HydroNode::AntiJoin { .. } // [(a,1),(b,2)] anti-join [a] = [(b,2)]. Similar to Difference
        | HydroNode::Filter { .. } // Although it contains a function f, the output is just a subset of the input, so just inherit from the parent
        | HydroNode::Inspect { .. }
        | HydroNode::Network { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Cast { .. }
        | HydroNode::ObserveNonDet { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::YieldConcat { .. } => {
            // For each input the first (and potentially only) parent depends on, take its dependency
            for input_id in input_taint_entry.iter() {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(input_id) &&
                    let Some(parent_dependency) = parent_dependencies_on_input.get(&0)
                {
                    input_dependencies_entry.insert(*input_id, parent_dependency.clone());
                    continue;
                }
                // Parent is tainted by input but has no dependencies, delete
                input_dependencies_entry.remove(input_id);
            }
        }
        // Alters parent in a predicatable way
        HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. } => {
            assert_eq!(parent_ids.len(), 2, "Node {:?} has the wrong number of parents.", node);
            // [a,b] chain [c,d] = [a,b,c,d]. Take the intersection of dependencies of the two parents for each input. If only one parent is tainted, then just take that dependency
            for (input_id, parent_positions) in parent_taints {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(&input_id) {
                    let num_tainted_parents = parent_positions.len();
                    // For each tainted parent, see if we can find its dependency (filter_map to remove None)
                    let parent_dependency_tuples = parent_positions.into_iter().filter_map(|pos| parent_dependencies_on_input.get(&pos)).cloned().collect::<Vec<StructOrTuple>>();
                    if let Some(intersection) = StructOrTuple::intersect_tuples(&parent_dependency_tuples) {
                        // Only accept the dependency if each tainted parent contributed, or if we're in the optimistic phase
                        if parent_dependency_tuples.len() == num_tainted_parents || *optimistic_phase {
                            input_dependencies_entry.insert(input_id, intersection);
                            continue;
                        }
                    }
                }
                // At least one parent is tainted but has no dependencies, or there's no overlap, remove
                input_dependencies_entry.remove(&input_id);
            }
        }
        HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. } => {
            assert_eq!(parent_ids.len(), 2, "Node {:?} has the wrong number of parents.", node);
            // [a,b] cross product [c,d] = [(a,c), (a,d), (b,c), (b,d)]
            for input_id in input_taint_entry.iter() {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(input_id) {
                    let mut new_dependency = StructOrTuple::default();
                    if let Some(parent1_dependency) = parent_dependencies_on_input.get(&0) {
                        new_dependency.set_dependencies(&vec!["0".to_string()], parent1_dependency, &vec![]);
                    }
                    if let Some(parent2_dependency) = parent_dependencies_on_input.get(&1) {
                        new_dependency.set_dependencies(&vec!["1".to_string()], parent2_dependency, &vec![]);
                    }
                    if !new_dependency.is_empty() {
                        input_dependencies_entry.insert(*input_id, new_dependency);
                        continue;
                    }
                }
                // At least one parent has no dependencies or there's no overlap, delete
                input_dependencies_entry.remove(input_id);
            }
        }
        HydroNode::Join { .. } => {
            assert_eq!(parent_ids.len(), 2, "Node {:?} has the wrong number of parents.", node);
            // [(a,b)] join [(a,c)] = [(a,(b,c))]
            for input_id in input_taint_entry.iter() {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(input_id) {
                    let mut new_dependency = StructOrTuple::default();
                    // Set a to dependencies from either of the parents' 0th index
                    if let Some(union) = union(parent_dependencies_on_input.get(&0), &vec!["0".to_string()], parent_dependencies_on_input.get(&1), &vec!["0".to_string()]) {
                        new_dependency.set_dependencies(&vec!["0".to_string()], &union, &vec![]);
                    }
                    // Set b to 1st index of parent 0
                    if let Some(parent1_dependency) = parent_dependencies_on_input.get(&0) {
                        new_dependency.set_dependencies(&vec!["1".to_string(),"0".to_string()], parent1_dependency, &vec!["1".to_string()]);
                    }
                    // Set c to 1st index of parent 1
                    if let Some(parent2_dependency) = parent_dependencies_on_input.get(&1) {
                        new_dependency.set_dependencies(&vec!["1".to_string(),"1".to_string()], parent2_dependency, &vec!["1".to_string()]);
                    }
                    if !new_dependency.is_empty() {
                        input_dependencies_entry.insert(*input_id, new_dependency);
                        continue;
                    }
                }
                // At least one parent has no dependencies or there's no overlap, delete
                input_dependencies_entry.remove(input_id);
            }
        }
        HydroNode::Enumerate { .. } => {
            assert_eq!(parent_ids.len(), 1, "Node {:?} has the wrong number of parents.", node);
            // enumerate [(a,b)] = [(0,a),(1,b)]
            for input_id in input_taint_entry.iter() {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(input_id) &&
                    let Some(parent_dependency) = parent_dependencies_on_input.get(&0)
                {
                    // Set the 1st index to the parent's dependency
                    let mut new_dependency = StructOrTuple::default();
                    new_dependency.set_dependencies(&vec!["1".to_string()], parent_dependency, &vec![]);
                    input_dependencies_entry.insert(*input_id, new_dependency);
                    continue;
                }
                // Parent is tainted by input but has no dependencies, delete
                input_dependencies_entry.remove(input_id);
            }
        }
        // Based on f
        HydroNode::Map { f, .. }
        | HydroNode::FilterMap { f, .. } => {
            assert_eq!(parent_ids.len(), 1, "Node {:?} has the wrong number of parents.", node);
            // Analyze if we haven't yet
            let syn_analysis_results = syn_analysis.entry(*next_stmt_id).or_insert_with(|| {
                let mut analyzer = AnalyzeClosure::default();
                analyzer.visit_expr(&f.0);

                // Keep topmost none field if this is filter_map
                let keep_topmost_none_fields = matches!(node, HydroNode::FilterMap { .. });
                analyzer.output_dependencies.remove_none_fields(keep_topmost_none_fields).unwrap_or_default()
            });
            for input_id in input_taint_entry.iter() {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(input_id) &&
                    let Some(parent_dependency) = parent_dependencies_on_input.get(&0) &&
                        // Project the parent's dependencies based on how f transforms the output
                    let Some(projected_dependencies) = StructOrTuple::project_parent(parent_dependency, syn_analysis_results)
                {
                    println!("Node {:?} input {:?} has projected dependencies: {:?}", next_stmt_id, input_id, projected_dependencies);
                    input_dependencies_entry.insert(*input_id, projected_dependencies);
                    continue;
                }
                // Parent is tainted by input but has no dependencies, delete
                input_dependencies_entry.remove(input_id);
            }
        }
        // Only the key is preserved
        HydroNode::ReduceKeyed { .. }
        | HydroNode::FoldKeyed { .. }
        | HydroNode::ReduceKeyedWatermark { .. } => {
            for input_id in input_taint_entry.iter() {
                if let Some(parent_dependencies_on_input) = parent_input_dependencies.get(input_id)
                    && let Some(parent_dependency) = parent_dependencies_on_input.get(&0)
                {
                    // Inherit only the 0th index of the parent (the key)
                    let mut new_dependency = StructOrTuple::default();
                    new_dependency.set_dependencies(&vec!["0".to_string()], parent_dependency, &vec!["0".to_string()]);
                    input_dependencies_entry.insert(*input_id, new_dependency);
                    continue;
                }
                // Parent is tainted by input but has no dependencies, delete
                input_dependencies_entry.remove(input_id);
            }
        }
        // No dependencies on the parent (or no parent)
        HydroNode::Reduce { .. }
        | HydroNode::Fold { .. }
        | HydroNode::Scan { .. }
        | HydroNode::FlatMap { .. }
        | HydroNode::Source { .. }
        | HydroNode::SingletonSource { .. } => {
            input_dependencies_entry.clear();
        }
        HydroNode::Placeholder
        | HydroNode::Counter { .. } => {
            panic!("Unexpected node type {:?} in input dependency analysis.", node);
        }
    }

    // If there is no input taint, remove it
    if input_taint_entry.is_empty() {
        input_taint.remove(next_stmt_id);
    }

    println!(
        "Input dependencies for node {}: {:?}",
        next_stmt_id, input_dependencies_entry
    );
}

fn input_dependency_analysis(
    ir: &mut [HydroRoot],
    location: &LocationId,
    op_id_to_parents: HashMap<usize, Vec<usize>>,
) -> InputDependencyMetadata {
    let mut metadata = InputDependencyMetadata {
        location: location.clone(),
        inputs: all_inputs(ir, location),
        op_id_to_parents,
        optimistic_phase: true,
        input_taint: BTreeMap::new(),
        input_dependencies: BTreeMap::new(),
        syn_analysis: BTreeMap::new(),
    };

    println!("\nBegin input dependency analysis");

    let mut num_iters = 0;
    let mut prev_hash = None;
    loop {
        println!("Input dependency analysis iteration {}", num_iters);

        traverse_dfir(ir,
        |_, _| {}, // Don't need to analyze leaves since they don't output anyway
        |node, next_stmt_id| {
            input_dependency_analysis_node(node, next_stmt_id, &mut metadata);
        },);

        // Check if we've hit fixpoint
        let mut hasher = DefaultHasher::new();
        metadata.hash(&mut hasher);
        let hash = hasher.finish();
        if prev_hash.is_some_and(|prev| prev == hash) {
            if metadata.optimistic_phase {
                println!("Optimistic phase reached fixpoint, starting pessimistic phase");
                metadata.optimistic_phase = false;
            } else {
                break;
            }
        }
        prev_hash = Some(hash);
        num_iters += 1;
    }

    metadata
}

/// If the tuple with the given id is tainted but has no dependencies from some input, return None
fn get_inputs_and_dependencies(
    input_taint: &BTreeMap<usize, BTreeSet<usize>>,
    input_dependencies: &BTreeMap<usize, BTreeMap<usize, StructOrTuple>>,
    id: usize,
    index: &StructOrTupleIndex,
) -> Option<(Vec<usize>, Vec<StructOrTuple>)> {
    let mut ordered_input = vec![];
    let mut ordered_dependencies = vec![];

    if let Some(taints) = input_taint.get(&id) {
        for input in taints {
            if let Some(dependency) = input_dependencies
                .get(&id)
                .and_then(|map| map.get(input))
                .and_then(|tuple| tuple.get_dependencies(index))
            {
                ordered_input.push(*input);
                ordered_dependencies.push(dependency);
            } else {
                // Parent is tainted but has no dependencies, cannot partition
                return None;
            }
        }
    }

    Some((ordered_input, ordered_dependencies))
}

fn partitioning_constraint_analysis_node(
    node: &mut HydroNode,
    next_stmt_id: &mut usize,
    dependency_metadata: &InputDependencyMetadata,
    possible_partitionings: &mut BTreeMap<usize, BTreeSet<BTreeMap<usize, StructOrTupleIndex>>>, /* op -> set of partitioning requirements (input -> indices), one for each input taint. Empty set = cannot partition */
) {
    let InputDependencyMetadata {
        op_id_to_parents,
        input_taint,
        input_dependencies,
        ..
    } = dependency_metadata;

    // If this node is tainted by an input (otherwise we don't care)
    if input_taint.contains_key(next_stmt_id) {
        let parent_ids = op_id_to_parents
            .get(next_stmt_id)
            .cloned()
            .unwrap_or_default();

        // If there's at least 1 parent that is not tainted by any input, then we can always partition
        if parent_ids
            .iter()
            .map(|id| input_taint.get(id))
            .any(|taint| taint.is_none())
        {
            return;
        }

        let mut ordered_inputs = vec![];
        let mut ordered_dependencies = vec![];
        match node {
            HydroNode::Difference { .. } => {
                // Get the dependencies from the entirety of parent 0 and parent 1
                // Only allow partitioning if both have dependencies (the earlier if statement already checks that that both are tainted)
                if let Some((parent0_inputs, parent0_dependencies)) = get_inputs_and_dependencies(
                    input_taint,
                    input_dependencies,
                    parent_ids[0],
                    &vec![],
                ) && let Some((parent1_inputs, parent1_dependencies)) =
                    get_inputs_and_dependencies(
                        input_taint,
                        input_dependencies,
                        parent_ids[1],
                        &vec![],
                    )
                {
                    ordered_inputs.extend(parent0_inputs);
                    ordered_dependencies.extend(parent0_dependencies);
                    ordered_inputs.extend(parent1_inputs);
                    ordered_dependencies.extend(parent1_dependencies);
                }
            }
            HydroNode::AntiJoin { .. } => {
                // Get the dependencies from field 0 of parent 0 and the entirety of parent 1
                // Only allow partitioning if both have dependencies (the earlier if statement already checks that that both are tainted)
                if let Some((parent0_inputs, parent0_dependencies)) = get_inputs_and_dependencies(
                    input_taint,
                    input_dependencies,
                    parent_ids[0],
                    &vec!["0".to_string()],
                ) && let Some((parent1_inputs, parent1_dependencies)) =
                    get_inputs_and_dependencies(
                        input_taint,
                        input_dependencies,
                        parent_ids[1],
                        &vec![],
                    )
                {
                    ordered_inputs.extend(parent0_inputs);
                    ordered_dependencies.extend(parent0_dependencies);
                    ordered_inputs.extend(parent1_inputs);
                    ordered_dependencies.extend(parent1_dependencies);
                }
            }
            HydroNode::Join { .. } => {
                // Get the dependencies from field 0 of parent 0 and parent 1
                // Only allow partitioning if both have dependencies (the earlier if statement already checks that that both are tainted)
                if let Some((parent0_inputs, parent0_dependencies)) = get_inputs_and_dependencies(
                    input_taint,
                    input_dependencies,
                    parent_ids[0],
                    &vec!["0".to_string()],
                ) && let Some((parent1_inputs, parent1_dependencies)) =
                    get_inputs_and_dependencies(
                        input_taint,
                        input_dependencies,
                        parent_ids[1],
                        &vec!["0".to_string()],
                    )
                {
                    ordered_inputs.extend(parent0_inputs);
                    ordered_dependencies.extend(parent0_dependencies);
                    ordered_inputs.extend(parent1_inputs);
                    ordered_dependencies.extend(parent1_dependencies);
                }
            }
            HydroNode::ReduceKeyed { .. }
            | HydroNode::FoldKeyed { .. }
            | HydroNode::ReduceKeyedWatermark { .. } => {
                // Can only partition on the key. The key's inherited dependencies are already in input_dependencies for this node
                if let Some((inputs, dependencies)) = get_inputs_and_dependencies(
                    input_taint,
                    input_dependencies,
                    *next_stmt_id,
                    &vec![],
                ) {
                    ordered_inputs.extend(inputs);
                    ordered_dependencies.extend(dependencies);
                }
            }
            HydroNode::Reduce { .. }
            | HydroNode::Fold { .. }
            | HydroNode::Scan { .. }
            | HydroNode::Enumerate { .. }
            | HydroNode::CrossProduct { .. }
            | HydroNode::CrossSingleton { .. } => {} // Partitioning is impossible
            HydroNode::Placeholder
            | HydroNode::Source { .. }
            | HydroNode::CycleSource { .. }
            | HydroNode::Tee { .. }
            | HydroNode::Chain { .. }
            | HydroNode::ChainFirst { .. }
            | HydroNode::ResolveFutures { .. }
            | HydroNode::ResolveFuturesOrdered { .. }
            | HydroNode::Map { .. }
            | HydroNode::FlatMap { .. }
            | HydroNode::Filter { .. }
            | HydroNode::FilterMap { .. }
            | HydroNode::DeferTick { .. }
            | HydroNode::Inspect { .. }
            | HydroNode::Unique { .. }
            | HydroNode::Sort { .. }
            | HydroNode::Network { .. }
            | HydroNode::ExternalInput { .. }
            | HydroNode::Counter { .. }
            | HydroNode::Cast { .. }
            | HydroNode::ObserveNonDet { .. }
            | HydroNode::SingletonSource { .. }
            | HydroNode::BeginAtomic { .. }
            | HydroNode::EndAtomic { .. }
            | HydroNode::Batch { .. }
            | HydroNode::YieldConcat { .. } => {
                // Doesn't impede partitioning, return
                return;
            }
        }

        // If there are no ordered_dependencies, we will insert an empty set which means that partitioning is impossible
        let intersection =
            StructOrTuple::intersect_dependencies_with_matching_fields(&ordered_dependencies);
        // Convert to set of maps
        let mut possible_partitionings_for_node = BTreeSet::new();
        for possible_set in intersection {
            let mut possible_set_mapped_to_input = BTreeMap::new();
            for (pos, dependency) in possible_set.iter().enumerate() {
                let input = ordered_inputs.get(pos).unwrap();
                possible_set_mapped_to_input.insert(*input, dependency.clone());
            }
            possible_partitionings_for_node.insert(possible_set_mapped_to_input);
        }
        possible_partitionings.insert(*next_stmt_id, possible_partitionings_for_node);
    }
}

/// Returns (all possible partitionings, inputs -> inputs' parents)
#[expect(clippy::type_complexity, reason = "internal optimization code")]
pub fn partitioning_analysis(
    ir: &mut [HydroRoot],
    location: &LocationId,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> Option<(
    Vec<BTreeMap<usize, StructOrTupleIndex>>,
    BTreeMap<usize, usize>,
)> {
    let op_id_to_parents = op_id_to_inputs(ir, Some(&location.key()), cycle_source_to_sink_input);
    let dependency_metadata = input_dependency_analysis(ir, location, op_id_to_parents);
    let mut possible_partitionings = BTreeMap::new();

    println!("\nBegin partitioning constraint analysis");

    traverse_dfir(ir,
    |_, _| {},
    |node, next_op_id| {
        partitioning_constraint_analysis_node(
            node,
            next_op_id,
            &dependency_metadata,
            &mut possible_partitionings,
        );
    },);

    for (op_id, partitioning_options) in &possible_partitionings {
        println!(
            "Partitioning options for op {}: {:?}",
            op_id, partitioning_options
        );
    }

    println!("\nBegin partitioning analysis");

    // See if there are any ops preventing us from partitioning first
    let mut partitioning_impossible = false;
    for (op_id, partitioning_options) in &possible_partitionings {
        if partitioning_options.is_empty() {
            println!("No partitioning possible due to op_id {}", op_id);
            partitioning_impossible = true;
        }
    }
    if partitioning_impossible {
        return None;
    }

    // Find all possible global partitionings
    let mut prev_global_partitionings = vec![]; // vec of map from input_id to StructOrTupleIndex
    let mut next_global_partitionings = vec![];

    // Set the 1st global partitioning
    if let Some((op_id, partitioning_options)) = possible_partitionings.pop_first() {
        for partitioning in partitioning_options {
            assert!(
                !partitioning.is_empty(),
                "Op {} has partitioning constraints yet no specific input fields for those constraints",
                op_id
            );
            prev_global_partitionings.push(partitioning.clone());
        }
    } else {
        // possible_partitionings is empty
        println!("No restrictions on partitioning");
        return Some((
            Vec::new(),
            all_inputs_parents(ir, &dependency_metadata.inputs, cycle_source_to_sink_input),
        ));
    }

    // Iterate through remaining constraints
    // Compare each prev_global_partitionings against each constraint, add to next_global_partitionings if the constraint is satisfied
    // Continue iterating after setting prev_global_partitionings to next_global_partitionings
    for (_op_id, partitioning_options) in possible_partitionings {
        for constraint in partitioning_options {
            for prev_partitioning in &prev_global_partitionings {
                // For each constraint, check if the prev_partitioning is still possible
                let mut next_partitioning = BTreeMap::new();
                let mut constraint_satisfiable = true;
                for (input, tuple_index) in prev_partitioning {
                    if let Some(constraint_index) = constraint.get(input) {
                        if let Some(index_intersection) =
                            StructOrTuple::index_intersection(tuple_index, constraint_index)
                        {
                            // Constraint satisfied
                            next_partitioning.insert(*input, index_intersection);
                        } else {
                            // No intersection, cannot partition
                            constraint_satisfiable = false;
                            break;
                        }
                    } else {
                        // No constraint on this input, keep old constraints
                        next_partitioning.insert(*input, tuple_index.clone());
                    }
                }
                // Add any new constraints not found in prev_partitioning
                for (input, constraint_index) in &constraint {
                    if !next_partitioning.contains_key(input) {
                        next_partitioning.insert(*input, constraint_index.clone());
                    }
                }

                if constraint_satisfiable {
                    next_global_partitionings.push(next_partitioning);
                }
            }
        }

        // Update prev partitionings before next loop
        prev_global_partitionings = std::mem::take(&mut next_global_partitionings);
    }

    // We don't know which partitioning is "better", so just return one
    println!(
        "Found {} possible global partitionings",
        prev_global_partitionings.len()
    );
    for (partitioning_index, partitioning) in prev_global_partitionings.iter().enumerate() {
        println!("Partitioning {}: {:?}", partitioning_index, partitioning);
    }
    if prev_global_partitionings.is_empty() {
        return None; // No possible partitioning
    }
    Some((
        prev_global_partitionings,
        all_inputs_parents(ir, &dependency_metadata.inputs, cycle_source_to_sink_input),
    ))
}

/// Given the set of possible partitionings, select an arbitrary one to use, and find all the parents of Network nodes that need to be modified
#[expect(clippy::type_complexity, reason = "internal optimization code")]
pub fn nodes_to_partition(
    analysis_results: Option<(
        Vec<BTreeMap<usize, StructOrTupleIndex>>,
        BTreeMap<usize, usize>,
    )>,
) -> Option<HashMap<usize, StructOrTupleIndex>> {
    if let Some((possible_partitionings, input_parents)) = analysis_results {
        let default_partitioning = BTreeMap::new();
        let partitioning = possible_partitionings
            .first()
            .unwrap_or(&default_partitioning);
        let mut nodes_to_partition = HashMap::new();

        for (input, parent) in input_parents {
            if let Some(partition_index) = partitioning.get(&input) {
                nodes_to_partition.insert(parent, partition_index.clone());
            } else {
                // No restriction, partition on the entire thing
                nodes_to_partition.insert(parent, vec![]);
            }
        }

        return Some(nodes_to_partition);
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use hydro_lang::compile::ir::deep_clone;
    use hydro_lang::deploy::HydroDeploy;
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::location::dynamic::LocationId;
    use hydro_lang::prelude::*;
    use stageleft::q;

    use crate::partition_node_analysis::partitioning_analysis;
    use crate::repair::{cycle_source_to_sink_input, inject_id, inject_location};

    fn test_input_partitionable(
        builder: FlowBuilder<'_>,
        location: LocationId,
        partitionable: bool,
    ) {
        let mut cycle_data = HashMap::new();
        let built = builder
            .optimize_with(|ir| {
                inject_id(ir);
                cycle_data = cycle_source_to_sink_input(ir);
                inject_location(ir, &cycle_data);
            })
            .into_deploy::<HydroDeploy>();
        let mut ir = deep_clone(built.ir());
        let partitioning = partitioning_analysis(&mut ir, &location, &cycle_data);

        assert_eq!(partitionable, partitioning.is_some());
    }

    #[test]
    fn test_map_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .map(q!(|(a, b)| (b, a + 2)))
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(b, a2)| {
                println!("b: {}, a+2: {}", b, a2);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_map_complex_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, (2, (3, 4)))]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .map(q!(|(a, b)| (b.1, a, b.0 - a)))
            .map(q!(|(b1, _a, b0a)| (b0a, b1.0)))
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(b0a, b10)| {
                println!("b.0 - a: {}, b.1.0: {}", b0a, b10);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_filter_map_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .filter_map(q!(|(a, b)| { if a > 1 { Some((b, a + 2)) } else { None } }))
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(b, a2)| {
                println!("b: {}, a+2: {}", b, a2);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_filter_map_remove_none_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .filter_map(q!(|(a, b)| {
                if a > 1 {
                    Some((None, a + 2))
                } else if a < 1 {
                    Some((Some(b), a + 2))
                } else {
                    None
                }
            }))
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(none, a2)| {
                println!("None: {:?}, a+2: {}", none, a2);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_chain_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input = cluster1
            .source_iter(q!([(1, (2, 3))]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values();
        let stream1 = input.clone().map(q!(|(a, b)| (b, a + 2)));
        let stream2 = input.map(q!(|(a, b)| ((b.1, b.1), a + 3)));
        let tick = cluster2.tick();
        stream2
            .batch(&tick, nondet!(/** test */))
            .chain(stream1.batch(&tick, nondet!(/** test */)))
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|((x, b1), y)| {
                println!("x: {}, b.1: {}, y: {}", x, b1, y);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_cross_product_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input = cluster1
            .source_iter(q!([(1, (2, 3))]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values();
        let stream1 = input.clone().map(q!(|(a, b)| (b, a + 2)));
        let stream2 = input.map(q!(|(a, b)| ((b.1, b.1), a + 3)));
        let tick = cluster2.tick();
        stream2
            .batch(&tick, nondet!(/** test */))
            .cross_product(stream1.batch(&tick, nondet!(/** test */)))
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(((b1, b1_again), a3), (b, a2))| {
                println!("((({}, {}), {}), ({:?}, {}))", b1, b1_again, a3, b, a2);
            }));

        test_input_partitionable(builder, cluster2.id(), false);
    }

    #[test]
    fn test_join_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input = cluster1
            .source_iter(q!([(1, (2, 3))]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("network")
            .values();
        let stream1 = input.clone().map(q!(|(a, b)| (b, a)));
        let stream2 = input.map(q!(|(a, b)| ((b.1, b.1), a + 3)));
        let tick = cluster2.tick();
        stream2
            .batch(&tick, nondet!(/** test */))
            .join(stream1.batch(&tick, nondet!(/** test */)))
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|((b1, b1_again), (a3, a))| {
                println!("(({}, {}), {}, {})", b1, b1_again, a3, a);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_enumerate_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .assume_ordering(nondet!(/** test */))
            .enumerate()
            .for_each(q!(|(i, (a, b))| {
                println!("i: {}, a: {}, b: {}", i, a, b);
            }));

        test_input_partitionable(builder, cluster2.id(), false);
    }

    #[test]
    fn test_reduce_keyed_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("network")
            .values()
            .batch(&cluster2.tick(), nondet!(/** test */))
            .into_keyed()
            .reduce(q!(
                |acc, b| *acc += b,
                commutative = ManualProof(/* Addition is commutative */)
            ))
            .entries()
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(a, b_sum)| {
                println!("a: {}, b_sum: {}", a, b_sum);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_reduce_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values()
            .batch(&cluster2.tick(), nondet!(/** test */))
            .reduce(q!(
                |(acc_a, acc_b), (a, b)| {
                    *acc_a += a;
                    *acc_b += b;
                },
                commutative = ManualProof(/* Addition is commutative */)
            ))
            .all_ticks()
            .for_each(q!(|(a_sum, b_sum)| {
                println!("a_sum: {}, b_sum: {}", a_sum, b_sum);
            }));

        test_input_partitionable(builder, cluster2.id(), false);
    }

    #[test]
    fn test_cycle_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input = cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values();
        let cluster2_tick = cluster2.tick();
        let (complete_cycle, cycle) =
            cluster2_tick.cycle::<Stream<(usize, usize), _, Bounded, NoOrder>>();
        let prev_tick_input = cycle
            .clone()
            .filter(q!(|(a, _b)| *a > 2))
            .map(q!(|(a, b)| (a, b + 2)));
        complete_cycle.complete_next_tick(
            prev_tick_input.chain(input.batch(&cluster2_tick, nondet!(/** test */))),
        );

        cycle
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(a, b)| {
                println!("a: {}, b: {}", a, b);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_nested_cycle_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input = cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("network")
            .values();
        let cluster2_tick = cluster2.tick();
        let (complete_cycle1, cycle1) =
            cluster2_tick.cycle::<Stream<(usize, usize), _, Bounded, NoOrder>>();
        let (complete_cycle2, cycle2) =
            cluster2_tick.cycle::<Stream<(usize, usize), _, Bounded, NoOrder>>();
        let chained = {
            cycle1
                .join(input.batch(&cluster2_tick, nondet!(/** test */)))
                .map(q!(|(_, (b1, b2))| (b1, b2))) // Both values are influenced by the join with cycle2_out
                .chain(cycle2)
        };
        complete_cycle1.complete_next_tick(chained.clone());
        let cycle2_out = chained.map(q!(|(_a, b)| (b, b)));
        complete_cycle2.complete_next_tick(cycle2_out.clone());
        cycle2_out
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(b, _)| {
                println!("b: {}", b);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_source_iter_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input = cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values();
        let tick = cluster2.tick();
        let stream1 = input.map(q!(|(a, b)| (b, a + 2)));
        let stream2 = cluster2.source_iter(q!([(3, 4)]));
        stream2
            .batch(&tick, nondet!(/** test */))
            .chain(stream1.batch(&tick, nondet!(/** test */)))
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|_| {
                println!("No dependencies");
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_multiple_inputs_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input1 = cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("input1")
            .values();
        let input2 = cluster1
            .source_iter(q!([(3, 4)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("input2")
            .values();
        let tick = cluster2.tick();
        let stream1 = input1
            .map(q!(|(a, b)| (b, a * 2)))
            .batch(&tick, nondet!(/** test */));
        let stream2 = input2
            .map(q!(|(a, b)| (b, -a)))
            .batch(&tick, nondet!(/** test */));
        stream2
            .clone()
            .chain(stream1.clone())
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|_| {
                println!("Dependent on both input1.b and input2.b");
            }));
        stream2
            .join(stream1)
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(_, (a1, a2))| {
                println!("a*2 from input 1: {}, -a from input 2: {}", a1, a2);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }

    #[test]
    fn test_difference_partitionable() {
        let mut builder = FlowBuilder::new();
        let cluster1 = builder.cluster::<()>();
        let cluster2 = builder.cluster::<()>();
        let input1 = cluster1
            .source_iter(q!([(1, 2)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("input1")
            .values();
        let input2 = cluster1
            .source_iter(q!([(3, 4)]))
            .broadcast(&cluster2, TCP.fail_stop().bincode(), nondet!(/** test */))
            .ir_node_named("input2")
            .values();
        let tick = cluster2.tick();
        input1
            .batch(&tick, nondet!(/** test */))
            .filter_not_in(input2.batch(&tick, nondet!(/** test */)))
            .all_ticks()
            .assume_ordering(nondet!(/** test */))
            .assume_retries(nondet!(/** test */))
            .for_each(q!(|(a, b)| {
                println!("a: {}, b: {}", a, b);
            }));

        test_input_partitionable(builder, cluster2.id(), true);
    }
}
