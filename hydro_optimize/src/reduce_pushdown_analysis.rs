use std::{cell::RefCell, collections::HashMap};

use hydro_lang::{
    compile::{
        builder::CycleId,
        ir::{
            BoundKind, CollectionKind, HydroIrMetadata, HydroNode, HydroRoot, StreamOrder,
            traverse_dfir,
        },
    },
    location::dynamic::LocationId,
};

/// - `reduce_op_id`: The op_id of the `Reduce` that can pushed after this node
/// - `distance_from_reduce`: Number of nodes between this node and the `Reduce`. If this node's child is Reduce, the distance is 0.
#[derive(Clone, Copy)]
struct ReduceRef {
    reduce_op_id: usize,
    distance_from_reduce: usize,
}

impl ReduceRef {
    fn increment_distance(&self) -> Self {
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
#[derive(Default)]
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

/// Analyze if a `Reduce` can be pushed "down" through the node's input
/// - `reduces_pending_cycle_resolution`: Map from cycle_id to HydroNode::Reduce that should be pushed through the CycleSink, but we don't have a reference to the sink without another iteration.
fn reduce_pushdown_analysis_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    node_to_analyze: &LocationId,
    reduce_metadata: &RefCell<ReducePushdownMetadata>,
) {
    if node.metadata().location_id != *node_to_analyze {
        return;
    }

    let mut reduce_metadata = reduce_metadata.borrow_mut();
    let my_pushed_reduce = match node {
        HydroNode::Reduce { f, input, metadata } => {
            reduce_metadata.op_id_to_reduce.insert(
                *op_id,
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
                        reduce_op_id: *op_id,
                        distance_from_reduce: 0,
                    },
                );
            }
            return;
        }
        HydroNode::ObserveNonDet { inner, .. } => {
            // Bubble up through ObserveNonDet
            if let Some(possible_reduce) = reduce_metadata
                .observe_nondet_possibilities
                .get(op_id)
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

            return;
        }
        _ => reduce_metadata.possible_locations.get(op_id).cloned(),
    };

    // Can't push through the input if we don't have a reduce
    let Some(my_bubbled_reduce) = my_pushed_reduce else {
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
                .insert(*cycle_id, my_bubbled_reduce);
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
        | HydroNode::FilterMap { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::CrossProduct { .. }
        | HydroNode::Join { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
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
}

fn reduce_pushdown_analysis_root(
    root: &mut HydroRoot,
    reduce_metadata: &RefCell<ReducePushdownMetadata>,
) {
    let mut reduce_metadata = reduce_metadata.borrow_mut();
    if let HydroRoot::CycleSink { cycle_id, input, .. } = root
        && let Some(possible_reduce) = reduce_metadata.cycle_possibilities.get(cycle_id).cloned() {
            reduce_metadata
                .possible_locations
                .insert(input.op_metadata().id.unwrap(), possible_reduce);
        }
}

/// Pushes any commutative `Reduce` on `node_to_analyze` down through as many `Unbounded`, `NoOrder` streams as possible
pub fn reduce_pushdown_analysis(ir: &mut [HydroRoot], node_to_analyze: &LocationId) {
    let metadata = RefCell::new(ReducePushdownMetadata::default());
    loop {
        let num_cycle_sink_possibilites = metadata.borrow().cycle_possibilities.len();
        traverse_dfir(
            ir,
            |root, _op_id| { 
                reduce_pushdown_analysis_root(root, &metadata);
            },
            |node, op_id| {
                reduce_pushdown_analysis_node(node, op_id, node_to_analyze, &metadata);
            },
        );

        if metadata.borrow().cycle_possibilities.len() == num_cycle_sink_possibilites {
            // No new possibilities from pushing through cycle sinks, we can stop
            break;
        }
    }
}
