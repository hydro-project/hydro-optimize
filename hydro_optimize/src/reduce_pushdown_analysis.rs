use std::collections::HashMap;

use hydro_lang::{
    compile::{
        builder::CycleId,
        ir::{BoundKind, CollectionKind, HydroIrMetadata, HydroNode, HydroRoot, StreamOrder},
    },
    location::dynamic::LocationId,
};

use crate::{repair::cycle_source_to_sink_input, rewrites::op_id_to_inputs};

/// - `reduce_op_id`: The op_id of the `Reduce` that can pushed after this node
/// - `distance_from_reduce`: Number of nodes between this node and the `Reduce`. If this node's child is Reduce, the distance is 0.
#[derive(Clone, Copy, Debug)]
pub struct ReduceRef {
    pub reduce_op_id: usize,
    pub distance_from_reduce: usize,
}

impl ReduceRef {
    pub fn increment_distance(&self) -> Self {
        Self {
            reduce_op_id: self.reduce_op_id,
            distance_from_reduce: self.distance_from_reduce + 1,
        }
    }
}

/// - `possible_locations`: Map from op_id to `Reduce` that can be inserted as the child of that op.
/// - `observe_nondet_possibilities`: Map from `ObserveNonDet` (that may not be `Unbounded`/`NoOrder`) to the `Reduce`. Used to see if the parent of the `ObservableNondet` is actually `Unbounded`/`NoOrder`.
/// - `cycle_possibilities`: Map from `CycleId` to `Reduce` that can be pushed through the cycle. Used so the Sink can push the reduce further in the next iteration.
/// - `op_id_to_reduce`: Map from op_id to the `Reduce` at that position, with Placeholder as input.
#[derive(Default, Debug)]
struct ReducePushdownMetadata {
    possible_locations: HashMap<usize, ReduceRef>,
    observe_nondet_possibilities: HashMap<usize, ReduceRef>,
    cycle_possibilities: HashMap<CycleId, ReduceRef>,
    op_id_to_reduce: HashMap<usize, HydroNode>,
}

/// `Reduce` can be pushed through `Unbounded` and `NoOrder` streams, because the ordering and batching don't affect the result.
fn can_pushdown(metadata: &HydroIrMetadata) -> bool {
    match &metadata.collection_kind {
        CollectionKind::Stream { bound, order, .. }
        | CollectionKind::KeyedStream {
            bound,
            value_order: order,
            ..
        } => *bound == BoundKind::Unbounded && *order == StreamOrder::NoOrder,
        CollectionKind::Singleton { .. }
        | CollectionKind::Optional { .. }
        | CollectionKind::KeyedSingleton { .. } => false,
    }
}

fn recurse_reduce_pushdown_analysis(
    node: &HydroNode,
    node_to_analyze: &LocationId,
    reduce_metadata: &mut ReducePushdownMetadata,
) {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::Partition { inner, .. } => {
            let inner_node = inner.0.borrow();
            reduce_pushdown_analysis_node(&inner_node, node_to_analyze, reduce_metadata);
        }
        _ => {
            for input in node.input() {
                reduce_pushdown_analysis_node(input, node_to_analyze, reduce_metadata);
            }
        }
    }
}

/// Analyze if a `Reduce` can be pushed "down" through the node's input
/// - `reduces_pending_cycle_resolution`: Map from cycle_id to HydroNode::Reduce that should be pushed through the CycleSink, but we don't have a reference to the sink without another iteration.
fn reduce_pushdown_analysis_node(
    node: &HydroNode,
    node_to_analyze: &LocationId,
    reduce_metadata: &mut ReducePushdownMetadata,
) {
    if node.metadata().location_id != *node_to_analyze {
        recurse_reduce_pushdown_analysis(node, node_to_analyze, reduce_metadata);
        return;
    }

    let op_id = node.op_metadata().id.unwrap();
    let my_pushed_reduce = match node {
        HydroNode::Reduce { f, input, metadata } => {
            reduce_metadata.op_id_to_reduce.insert(
                op_id,
                HydroNode::Reduce {
                    f: f.clone(),
                    input: Box::new(HydroNode::Placeholder),
                    metadata: metadata.clone(),
                },
            );
            // If the input is `ObserveNonDet`, mark it for potentially pushing down.
            // It may be used to cast an Unbounded, NoOrder stream into the reduce
            if let HydroNode::ObserveNonDet {
                metadata: input_metadata,
                ..
            } = &**input
            {
                reduce_metadata.observe_nondet_possibilities.insert(
                    input_metadata.op.id.unwrap(),
                    ReduceRef {
                        reduce_op_id: op_id,
                        distance_from_reduce: 0,
                    },
                );
            }
            recurse_reduce_pushdown_analysis(node, node_to_analyze, reduce_metadata);
            return;
        }
        HydroNode::ObserveNonDet { inner, .. } => {
            // Bubble up through ObserveNonDet
            if let Some(possible_reduce) = reduce_metadata
                .observe_nondet_possibilities
                .get(&op_id)
                .cloned()
            {
                match &**inner {
                    HydroNode::ObserveNonDet { metadata, .. } => {
                        reduce_metadata.observe_nondet_possibilities.insert(
                            metadata.op.id.unwrap(),
                            possible_reduce.increment_distance(),
                        );
                    }
                    _ => {
                        reduce_metadata.possible_locations.insert(
                            inner.op_metadata().id.unwrap(),
                            possible_reduce.increment_distance(),
                        );
                    }
                }
            }

            recurse_reduce_pushdown_analysis(node, node_to_analyze, reduce_metadata);
            return;
        }
        _ => reduce_metadata.possible_locations.get(&op_id).cloned(),
    };

    // Can't push through the input if we don't have a reduce
    let Some(my_bubbled_reduce) = my_pushed_reduce else {
        recurse_reduce_pushdown_analysis(node, node_to_analyze, reduce_metadata);
        return;
    };

    let mut check_inputs = vec![];
    match node {
        // Shouldn't see BeginAtomic or Batch, because we shouldn't be in an atomic region in the first place
        HydroNode::Placeholder | HydroNode::Counter { .. } => {
            panic!("Reduce pushdown encountered unexpected Placeholder or Counter node")
        }
        HydroNode::BeginAtomic { .. } | HydroNode::Batch { .. } => {
            panic!("Reduce pushdown entered ticked region")
        }
        HydroNode::Sort { .. } => panic!("Reduce pushdown was marked possible over a sorted stream?"),
        HydroNode::Reduce { .. } => panic!("Reduce pushdown reprocessing a reduce?"),
        HydroNode::CycleSource { cycle_id, .. } => {
            reduce_metadata
                .cycle_possibilities
                .insert(*cycle_id, my_bubbled_reduce.increment_distance());
        }
        HydroNode::Cast { inner: input, .. }
        | HydroNode::ObserveNonDet { inner: input, .. }
        | HydroNode::Inspect { input, .. } => {
            check_inputs.push(input.metadata());
        }
        HydroNode::Chain { first, second, .. }
        | HydroNode::ChainFirst { first, second, .. } => {
            check_inputs.push(first.metadata());
            check_inputs.push(second.metadata());
        }
        // Can't push through
        // Comments indicate why `Reduce` can't be pushed through the HydroNode (and the cases before the next comment)
        HydroNode::Source { .. } // No inputs
        | HydroNode::SingletonSource { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Tee { .. } // Would need to check other branch of input to push up (too hard for now)
        | HydroNode::Partition { .. }
        | HydroNode::EndAtomic { .. } // Don't enter atomic region
        | HydroNode::YieldConcat { .. }
        | HydroNode::Map { .. } // Type changes, reduce not defined over the parent type
        | HydroNode::FlatMap { .. }
        | HydroNode::FlatMapStreamBlocking { .. }
        | HydroNode::FilterMap { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::CrossProduct { .. }
        | HydroNode::Join { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
        | HydroNode::Fold { .. }
        | HydroNode::FoldKeyed { .. }
        | HydroNode::Scan { .. }
        | HydroNode::ReduceKeyed { .. } // NOTE: Type changes. On the other hand, ReduceKeyed should be push-downable? Unsupported for now.
        | HydroNode::ReduceKeyedWatermark { .. }
        | HydroNode::Filter { .. } // Removes elements, reduce will execute over too many elements
        | HydroNode::Difference { .. }
        | HydroNode::AntiJoin { .. }
        | HydroNode::Unique { .. }
        | HydroNode::Network { .. } // Network expects a specific input type
        | HydroNode::DeferTick { .. } // Not sure what the semantics would be
        => {}
    }

    // Insert the reduce into the parent if possible
    for input in check_inputs {
        can_pushdown(input).then(|| {
            reduce_metadata
                .possible_locations
                .insert(input.op.id.unwrap(), my_bubbled_reduce.increment_distance());
        });
    }

    recurse_reduce_pushdown_analysis(node, node_to_analyze, reduce_metadata);
}

fn reduce_pushdown_analysis_root(
    root: &HydroRoot,
    node_to_analyze: &LocationId,
    reduce_metadata: &mut ReducePushdownMetadata,
) {
    if let HydroRoot::CycleSink {
        cycle_id, input, ..
    } = root
        && let Some(possible_reduce) = reduce_metadata.cycle_possibilities.get(cycle_id).cloned()
    {
        reduce_metadata
            .possible_locations
            .insert(input.op_metadata().id.unwrap(), possible_reduce);
    }

    // Tail recursion up
    reduce_pushdown_analysis_node(root.input(), node_to_analyze, reduce_metadata);
}

/// Finds all possible locations where we can push any commutative `Reduce` on `node_to_analyze` down through `Unbounded`, `NoOrder` streams
fn reduce_pushdown_analysis(
    ir: &mut [HydroRoot],
    node_to_analyze: &LocationId,
) -> ReducePushdownMetadata {
    let mut metadata = ReducePushdownMetadata::default();
    loop {
        let num_cycle_sink_possibilites = metadata.cycle_possibilities.len();
        // Manual tail recursion so we process the roots first, walking to the source
        for root in ir.iter() {
            reduce_pushdown_analysis_root(root, node_to_analyze, &mut metadata);
        }

        if metadata.cycle_possibilities.len() == num_cycle_sink_possibilites {
            // No new possibilities from pushing through cycle sinks, we can stop
            return metadata;
        }
    }
}

/// Push commutative `Reduce` down as far as possible.
/// # Returns
/// Map from op_id to the `Reduce` that should be added as the child of that op
pub fn reduce_pushdown_decision(
    ir: &mut [HydroRoot],
    node_to_analyze: &LocationId,
) -> HashMap<usize, usize> {
    let metadata = reduce_pushdown_analysis(ir, node_to_analyze);
    let cycle_map = cycle_source_to_sink_input(ir);
    // Note: This accounts for cycles (parent of a CycleSource is its CycleSink's input)
    let op_id_to_input = op_id_to_inputs(ir, Some(&node_to_analyze.key()), &cycle_map);

    let mut decisions = HashMap::new();
    // Pushing down furthest = pushing to nodes where we can't push to all of their parents.
    // If they have no parents, push down to here too.
    for (op_id, reduce_ref) in &metadata.possible_locations {
        let parents = op_id_to_input.get(op_id).cloned().unwrap_or_default();
        let no_parents = parents.is_empty();
        let mut cant_push_to_some_parent = false;

        for parent in parents {
            if !metadata.possible_locations.contains_key(&parent)
                && !metadata.observe_nondet_possibilities.contains_key(&parent)
            {
                cant_push_to_some_parent = true;
            }
        }

        if no_parents || cant_push_to_some_parent {
            decisions.insert(*op_id, reduce_ref.reduce_op_id);
            println!("Pushing reduce {} to op {}", reduce_ref.reduce_op_id, op_id);
        }
    }

    decisions
}

#[cfg(test)]
mod tests {
    use hydro_lang::{
        compile::ir::deep_clone,
        live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder},
        location::Location,
        nondet::nondet,
        prelude::{FlowBuilder, TCP},
    };
    use stageleft::q;

    use crate::{
        reduce_pushdown_analysis::reduce_pushdown_analysis,
        repair::{cycle_source_to_sink_input, inject_id, inject_location},
    };

    #[test]
    fn test_can_reduce_pushdown() {
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

        let mut ir = deep_clone(
            builder
                .optimize_with(|ir| {
                    inject_id(ir);
                    let cycle_data = cycle_source_to_sink_input(ir);
                    inject_location(ir, &cycle_data);
                })
                .ir(),
        );
        let analysis = reduce_pushdown_analysis(&mut ir, &cluster2.id());
        assert!(
            !analysis.possible_locations.is_empty(),
            "Expected to be able to push down reduce"
        );
    }
}
