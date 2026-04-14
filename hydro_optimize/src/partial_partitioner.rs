use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use hydro_lang::compile::builder::{CycleId, FlowBuilder};
use hydro_lang::compile::ir::{
    BoundKind, CollectionKind, HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot,
    SharedNode, SingletonBoundKind, StreamOrder, StreamRetry, traverse_dfir,
};
use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use serde::{Deserialize, Serialize};

use crate::decoupler::add_network_raw;
use crate::partition_syn_analysis::StructOrTupleIndex;
use crate::partitioner::{Partitioner, partition};
use crate::rewrites::{collection_kind_to_debug_type, prepend_member_id_to_collection_kind};

#[derive(Clone, Serialize, Deserialize)]
pub struct PartialPartitioner {
    pub nodes_before_partitioned_input: HashMap<usize, StructOrTupleIndex>, /* ID of node right before a Network -> what to partition on */
    pub nodes_after_partitioned_input: HashSet<usize>, /* ID of Network recv node at the partitioned cluster */
    pub nodes_to_replicate: HashSet<usize>,            /* ID of node right before a Network */
    pub nodes_for_garbage_collection: HashSet<usize>,  /* ID of Network */
    pub num_partitions: usize,
    pub location_id: LocationKey,
    pub new_cluster_id: Option<LocationKey>,
}

/// Shared state
pub struct PartialPartitionState {
    pub new_cycle_sinks: Vec<HydroRoot>,
    pub coordinator_location: LocationId, // single-member coordinator cluster location
    pub coordinator_tick: LocationId,     // tick within the coordinator cluster
    pub partition_tick: LocationId,       // tick within the partition cluster

    // ── Coordinator state tees (within coordinator tick) ──
    pub coordinator_state_tee: Option<Rc<RefCell<HydroNode>>>, // Singleton<(is_idle, logical_clock)>
    pub max_assigned_clock_tee: Option<Rc<RefCell<HydroNode>>>, // Singleton<usize>, max clock assigned to any replicated input
    pub c_prepare_clock_tee: Option<Rc<RefCell<HydroNode>>>, // Optional<usize>, clock of the value just sent as a prepare
    pub c_prepare_keyed_tee: Option<Rc<RefCell<HydroNode>>>, // Optional<(usize, ())>, keyed version for joining with buffered values

    // ── Partition state tees (within partition tick) ──
    pub p_state_tee: Option<Rc<RefCell<HydroNode>>>, // Singleton<(is_idle, logical_clock)>
    pub p_prepare_signal_cycle: Option<CycleId>,     // cycle fed by replicated input arrivals
    pub p_prepare_clock_streams: Vec<HydroNode>, // collected from each replicated input, chained into one CycleSink
}

fn metadata_at(
    location: &LocationId,
    collection_kind: CollectionKind,
    backtrace_source: &HydroIrMetadata,
) -> HydroIrMetadata {
    HydroIrMetadata {
        location_id: location.clone(),
        collection_kind,
        cardinality: None,
        tag: None,
        op: HydroIrOpMetadata {
            backtrace: backtrace_source.op.backtrace.clone(),
            cpu_usage: None,
            network_recv_cpu_usage: None,
            id: None,
        },
    }
}

fn unbounded_stream(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Stream {
        bound: BoundKind::Unbounded,
        order: StreamOrder::NoOrder,
        retry: StreamRetry::ExactlyOnce,
        element_type: element_type.into(),
    }
}

fn bounded_stream(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Stream {
        bound: BoundKind::Bounded,
        order: StreamOrder::NoOrder,
        retry: StreamRetry::ExactlyOnce,
        element_type: element_type.into(),
    }
}

fn bounded_optional(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Optional {
        bound: BoundKind::Bounded,
        element_type: element_type.into(),
    }
}

fn bounded_singleton(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Singleton {
        bound: SingletonBoundKind::Bounded,
        element_type: element_type.into(),
    }
}

fn op_meta(backtrace_source: &HydroIrMetadata) -> HydroIrOpMetadata {
    HydroIrOpMetadata {
        backtrace: backtrace_source.op.backtrace.clone(),
        cpu_usage: None,
        network_recv_cpu_usage: None,
        id: None,
    }
}

/// Broadcast from coordinator to all partitions of the corresponding original node.
/// Uses the coordinator's self_id to compute the base partition index: `self_id * num_partitions`.
fn broadcast_to_partitions(
    node: &mut HydroNode,
    coord_loc: &LocationId,
    cluster_loc: &LocationId,
    num_partitions: usize,
) {
    let metadata = node.metadata().clone();
    let original_kind = metadata.collection_kind.clone();
    let element_type: syn::Type = (*collection_kind_to_debug_type(&original_kind).0).clone();
    let new_kind = prepend_member_id_to_collection_kind(&original_kind);
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    let self_id_ident = syn::Ident::new(
        &format!("__hydro_lang_cluster_self_id_{}", coord_loc.key()),
        proc_macro2::Span::call_site(),
    );

    let mut mapped = HydroNode::FlatMap {
        f: {
            let e: syn::Expr = syn::parse_quote!(|b: #element_type| {
                let base = #self_id_ident.get_raw_id() as usize * #num_partitions;
                (base..base + #num_partitions).map(move |i|
                    (hydro_lang::location::MemberId::<()>::from_raw_id(i as u32), b.clone())
                )
            });
            e.into()
        },
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            location_id: coord_loc.clone(),
            collection_kind: new_kind,
            cardinality: None,
            tag: None,
            op: op_meta(&metadata),
        },
    };

    add_network_raw(&mut mapped, cluster_loc, &original_kind);
    *node = mapped;
}

/// Send from a partition to its corresponding coordinator (self_id / num_partitions).
fn send_to_coordinator(
    node: &mut HydroNode,
    cluster_loc: &LocationId,
    coord_loc: &LocationId,
    num_partitions: usize,
) {
    let metadata = node.metadata().clone();
    let original_kind = metadata.collection_kind.clone();
    let new_kind = prepend_member_id_to_collection_kind(&original_kind);
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    let self_id_ident = syn::Ident::new(
        &format!("__hydro_lang_cluster_self_id_{}", cluster_loc.key()),
        proc_macro2::Span::call_site(),
    );

    let mut mapped = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|b| (
                hydro_lang::location::MemberId::<()>::from_raw_id(#self_id_ident.get_raw_id() / #num_partitions as u32),
                b
            ));
            e.into()
        },
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            location_id: cluster_loc.clone(),
            collection_kind: new_kind,
            cardinality: None,
            tag: None,
            op: op_meta(&metadata),
        },
    };

    add_network_raw(&mut mapped, coord_loc, &original_kind);
    *node = mapped;
}

/// Creates a persisted stream.
/// - `cycle_id`: Cycle containing values persisted from the last tick.
/// - `new_values`: Stream of new values from the current tick to chain after the persisted value.
fn persisted_stream_with_initial(
    cycle_id: CycleId,
    new_values: HydroNode,
    element_type: syn::Type,
    tick_loc: &LocationId,
    bt: &HydroIrMetadata,
) -> HydroNode {
    let kind = bounded_stream(element_type);
    let source = HydroNode::CycleSource {
        cycle_id,
        metadata: metadata_at(tick_loc, kind.clone(), bt),
    };
    let deferred = HydroNode::DeferTick {
        input: Box::new(source),
        metadata: metadata_at(tick_loc, kind.clone(), bt),
    };
    HydroNode::Chain {
        first: Box::new(deferred),
        second: Box::new(new_values),
        metadata: metadata_at(tick_loc, kind, bt),
    }
}

/// Creates a persisted singleton with an initial value for the first tick.
/// - `cycle_id`: Cycle containing the new value from the last tick.
/// - `initial_value`: Value to use on the first tick before the cycle produces a value.
/// # Returns
/// `Rc<RefCell<HydroNode>>` suitable for Tee'ing.
fn persisted_singleton_with_initial(
    cycle_id: CycleId,
    initial_value: syn::Expr,
    element_type: syn::Type,
    tick_loc: &LocationId,
    bt: &HydroIrMetadata,
) -> Rc<RefCell<HydroNode>> {
    let kind = bounded_singleton(element_type);
    let source = HydroNode::CycleSource {
        cycle_id,
        metadata: metadata_at(tick_loc, kind.clone(), bt),
    };
    persisted_singleton_from_source(source, initial_value, kind, tick_loc, bt)
}

/// Like [`persisted_singleton_with_initial`] but takes an already-constructed source node
/// (e.g. a Tee of a CycleSource) instead of a CycleId.
fn persisted_singleton_from_source(
    source: HydroNode,
    initial_value: syn::Expr,
    kind: CollectionKind,
    tick_loc: &LocationId,
    bt: &HydroIrMetadata,
) -> Rc<RefCell<HydroNode>> {
    let deferred = HydroNode::DeferTick {
        input: Box::new(source),
        metadata: metadata_at(tick_loc, kind.clone(), bt),
    };
    let default = HydroNode::SingletonSource {
        value: initial_value.into(),
        first_tick_only: true,
        metadata: metadata_at(tick_loc, kind.clone(), bt),
    };
    let this_tick = HydroNode::ChainFirst {
        first: Box::new(deferred),
        second: Box::new(default),
        metadata: metadata_at(tick_loc, kind, bt),
    };
    Rc::new(RefCell::new(this_tick))
}

/// Creates a (is_idle, logical_clock) state, initialized to (true, 0).
/// Returns (state_tee, cycle_id), where the cycle_id can be used to update the state.
fn build_state(
    builder: &mut FlowBuilder,
    tick_loc: &LocationId,
    bt: &HydroIrMetadata,
) -> (Rc<RefCell<HydroNode>>, CycleId) {
    let state_type: syn::Type = syn::parse_quote! { (bool, usize) };
    let cycle_id = builder.next_cycle_id();
    let state_tee = persisted_singleton_with_initial(
        cycle_id,
        syn::parse_quote!((true, 0usize)),
        state_type,
        tick_loc,
        bt,
    );
    (state_tee, cycle_id)
}

/// Updates state in the following priority: prepare update → commit update → previous state.
/// - `pending_roots`: CycleSinks to be added to the IR. This function will add a CycleSink.
/// - `state_tee`: The current state.
/// - `cycle_id`: The cycle ID to hook the CycleSink to.
/// - `commit_update`: Update to the state if a commit was received.
/// - `prepare_update`: Update to the state if a prepare was received. Takes priority over commit_update if both are present in the same tick, since the commit must then be stale.
fn update_state(
    pending_roots: &mut Vec<HydroRoot>,
    state_tee: &Rc<RefCell<HydroNode>>,
    cycle_id: CycleId,
    commit_update: HydroNode,
    prepare_update: HydroNode,
    tick_loc: &LocationId,
    bt: &HydroIrMetadata,
) {
    let state_type: syn::Type = syn::parse_quote! { (bool, usize) };
    let prev = HydroNode::Tee {
        inner: SharedNode(state_tee.clone()),
        metadata: metadata_at(tick_loc, bounded_singleton(state_type.clone()), bt),
    };
    let prepare_or_commit = HydroNode::ChainFirst {
        first: Box::new(prepare_update),
        second: Box::new(commit_update),
        metadata: metadata_at(tick_loc, bounded_optional(state_type.clone()), bt),
    };
    let updated = HydroNode::ChainFirst {
        first: Box::new(prepare_or_commit),
        second: Box::new(prev),
        metadata: metadata_at(tick_loc, bounded_singleton(state_type), bt),
    };
    pending_roots.push(HydroRoot::CycleSink {
        cycle_id,
        input: Box::new(updated),
        op_metadata: op_meta(bt),
    });
}

/// Build the coordinator on a single-member Cluster.
///
/// State (DeferTick cycles within coordinator tick):
/// - `coordinator_state`: Singleton<(bool, usize)> = (is_idle, logical_clock), default (true, 0)
/// - `max_assigned_clock`: Singleton<usize>, default 0. Monotonically increasing, uniquely assigned to replicated inputs.
///
/// 1. Send Prepare if Idle and there are buffered messages (logical_clock < max_assigned_clock).
/// 2. Send Commit once ACKs have been received from all partitions.
fn build_coordinator(
    state: &mut PartialPartitionState,
    partitioner: &PartialPartitioner,
    builder: &mut FlowBuilder,
    backtrace_source: &HydroIrMetadata,
    c_prepare_ack_cycle: CycleId,
) -> (HydroNode, CycleId) {
    let coord_tick = &state.coordinator_tick.clone();
    let coord_loc = &state.coordinator_location.clone();
    let cluster_loc = &LocationId::Cluster(partitioner.location_id);
    let num_partitions = partitioner.num_partitions;
    let bt = backtrace_source;

    let usize_type: syn::Type = syn::parse_quote! { usize };
    let coord_state_type: syn::Type = syn::parse_quote! { (bool, usize) };

    // Coordinator state: (is_idle, logical_clock)
    let (c_state_tee, coordinator_state_cycle) = build_state(builder, coord_tick, bt);
    state.coordinator_state_tee = Some(c_state_tee.clone());

    // State: Logical clock to assign to next message
    // Create one CycleSource and tee it: one branch through DeferTick (persisted state
    // for replicated inputs), the other used directly (to read the latest value for this tick).
    let max_assigned_clock_cycle = builder.next_cycle_id();
    let usize_kind = bounded_singleton(usize_type.clone());
    let c_max_cycle_source = Rc::new(RefCell::new(HydroNode::CycleSource {
        cycle_id: max_assigned_clock_cycle,
        metadata: metadata_at(coord_tick, usize_kind.clone(), bt),
    }));
    let c_max_deferred_branch = HydroNode::Tee {
        inner: SharedNode(c_max_cycle_source.clone()),
        metadata: metadata_at(coord_tick, usize_kind.clone(), bt),
    };
    let c_max_clock_tee = persisted_singleton_from_source(
        c_max_deferred_branch,
        syn::parse_quote!(0usize),
        usize_kind.clone(),
        coord_tick,
        bt,
    );
    state.max_assigned_clock_tee = Some(c_max_clock_tee.clone());
    let c_max_same_tick = HydroNode::Tee {
        inner: SharedNode(c_max_cycle_source),
        metadata: metadata_at(coord_tick, usize_kind.clone(), bt),
    };
    let c_max_for_prepare = HydroNode::ChainFirst {
        first: Box::new(c_max_same_tick),
        second: Box::new(HydroNode::SingletonSource {
            value: {
                let e: syn::Expr = syn::parse_quote!(0usize);
                e.into()
            },
            first_tick_only: true,
            metadata: metadata_at(coord_tick, usize_kind, bt),
        }),
        metadata: metadata_at(coord_tick, bounded_singleton(usize_type.clone()), bt),
    };

    // 1. Send prepares when is_idle and there are pending messages (logical_clock < max_assigned_clock)
    let c_state_for_prepare = HydroNode::Tee {
        inner: SharedNode(c_state_tee.clone()),
        metadata: metadata_at(coord_tick, bounded_singleton(coord_state_type.clone()), bt),
    };
    let state_with_max_type: syn::Type = syn::parse_quote! { ((bool, usize), usize) };
    let c_state_with_max = HydroNode::CrossSingleton {
        left: Box::new(c_state_for_prepare),
        right: Box::new(c_max_for_prepare),
        metadata: metadata_at(
            coord_tick,
            bounded_singleton(state_with_max_type.clone()),
            bt,
        ),
    };
    let c_prepare_clock_optional = HydroNode::FilterMap {
        f: {
            let e: syn::Expr = syn::parse_quote!(|(
                (is_idle, logical_clock),
                max_assigned_clock,
            ): ((bool, usize), usize)| if is_idle
                && logical_clock < max_assigned_clock
            {
                Some(logical_clock)
            } else {
                None
            });
            e.into()
        },
        input: Box::new(c_state_with_max),
        metadata: metadata_at(coord_tick, bounded_optional(usize_type.clone()), bt),
    };
    let c_prepare_clock_shared = Rc::new(RefCell::new(c_prepare_clock_optional));
    state.c_prepare_clock_tee = Some(c_prepare_clock_shared.clone());
    // Prepare clock keyed for Join: (clock, ()) — shared across all replicated inputs
    let c_prepare_keyed = HydroNode::Tee {
        inner: SharedNode(c_prepare_clock_shared.clone()),
        metadata: metadata_at(coord_tick, bounded_optional(usize_type.clone()), bt),
    };
    let c_prepare_keyed_type: syn::Type = syn::parse_quote! { (usize, ()) };
    let c_prepare_keyed_mapped = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|clock: usize| (clock, ()));
            e.into()
        },
        input: Box::new(c_prepare_keyed),
        metadata: metadata_at(coord_tick, bounded_optional(c_prepare_keyed_type), bt),
    };
    state.c_prepare_keyed_tee = Some(Rc::new(RefCell::new(c_prepare_keyed_mapped)));
    // Change state from Idle to Prepared if a prepare is ready
    let c_prepare_for_state = HydroNode::Tee {
        inner: SharedNode(c_prepare_clock_shared.clone()),
        metadata: metadata_at(coord_tick, bounded_optional(usize_type.clone()), bt),
    };
    let c_state_after_prepare = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|prepare_clock: usize| (false, prepare_clock));
            e.into()
        },
        input: Box::new(c_prepare_for_state),
        metadata: metadata_at(coord_tick, bounded_optional(coord_state_type.clone()), bt),
    };

    // 2. Send commit once all prepare ACKs have been received
    let c_ack_source = HydroNode::CycleSource {
        cycle_id: c_prepare_ack_cycle,
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    let ack_quorum_output_type: syn::Type = syn::parse_quote! { Option<usize> };
    let c_ack_quorum_scan = HydroNode::Scan {
        init: {
            let e: syn::Expr = syn::parse_quote!(|| (0usize, 0usize));
            e.into()
        },
        acc: {
            let e: syn::Expr = syn::parse_quote!(
                |(expected_clock, ack_count): &mut (usize, usize), ack_clock: usize| -> Option<Option<usize>> {
                    assert_eq!(ack_clock, *expected_clock, "Received future prepare ACK before current prepare completed");
                    *ack_count += 1;
                    if *ack_count == #num_partitions {
                        *ack_count = 0;
                        let committed_clock = *expected_clock;
                        *expected_clock += 1;
                        Some(Some(committed_clock))
                    } else { Some(None) }
                }
            );
            e.into()
        },
        input: Box::new(c_ack_source),
        metadata: metadata_at(coord_loc, unbounded_stream(ack_quorum_output_type), bt),
    };
    let c_ack_quorum_filtered = HydroNode::FilterMap {
        f: {
            let e: syn::Expr = syn::parse_quote!(|v: Option<usize>| v);
            e.into()
        },
        input: Box::new(c_ack_quorum_scan),
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    let c_prepare_quorum_shared = Rc::new(RefCell::new(c_ack_quorum_filtered));
    let mut c_commit_broadcast = HydroNode::Tee {
        inner: SharedNode(c_prepare_quorum_shared.clone()),
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    broadcast_to_partitions(
        &mut c_commit_broadcast,
        coord_loc,
        cluster_loc,
        num_partitions,
    );
    let p_commit = c_commit_broadcast;
    // Change state from Prepared to Idle on commit
    let c_commit_for_feedback = HydroNode::Tee {
        inner: SharedNode(c_prepare_quorum_shared),
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    let c_commit_feedback_batched = HydroNode::Batch {
        inner: Box::new(c_commit_for_feedback),
        metadata: metadata_at(coord_tick, bounded_stream(usize_type.clone()), bt),
    };
    // Commit path: CrossSingleton(commit_stream, state) → (true, committed_clock + 1)
    // Prepare path: CrossSingleton(state, prepare_batched) → (false, next_clock)
    // Default: previous state
    let c_state_after_commit = HydroNode::Map {
        f: {
            let e: syn::Expr =
                syn::parse_quote!(|committed_clock: usize| (true, committed_clock + 1));
            e.into()
        },
        input: Box::new(c_commit_feedback_batched),
        metadata: metadata_at(coord_tick, bounded_optional(coord_state_type.clone()), bt),
    };
    update_state(
        &mut state.new_cycle_sinks,
        &c_state_tee,
        coordinator_state_cycle,
        c_state_after_commit,
        c_state_after_prepare,
        coord_tick,
        bt,
    );

    (p_commit, max_assigned_clock_cycle)
}

/// Add the following state to each partition:
/// - `p_state_tee`: Singleton<(bool, usize)> = (is_idle, logical_clock), default (true, 0)
///
/// 1. Change state to Idle after Commit
/// 2. Update state & ack after receiving Prepare
fn build_partition_state(
    state: &mut PartialPartitionState,
    commit_at_partitions: HydroNode,
    partitioner: &PartialPartitioner,
    builder: &mut FlowBuilder,
    backtrace_source: &HydroIrMetadata,
    c_prepare_ack_cycle: CycleId,
) {
    let p_tick = &state.partition_tick.clone();
    let coord_loc = &state.coordinator_location.clone();
    let cluster_loc = &LocationId::Cluster(partitioner.location_id);
    let bt = backtrace_source;
    let usize_type: syn::Type = syn::parse_quote! { usize };
    let state_type: syn::Type = syn::parse_quote! { (bool, usize) };

    let (p_state_tee, p_cycle) = build_state(builder, p_tick, bt);
    state.p_state_tee = Some(p_state_tee.clone());

    // 1. Upon receiving a commit, change state if it is not stale
    let p_commit_batched = HydroNode::Batch {
        inner: Box::new(commit_at_partitions),
        metadata: metadata_at(p_tick, bounded_stream(usize_type.clone()), bt),
    };
    let p_state_for_commit = HydroNode::Tee {
        inner: SharedNode(p_state_tee.clone()),
        metadata: metadata_at(p_tick, bounded_singleton(state_type.clone()), bt),
    };
    let commit_with_state_type: syn::Type = syn::parse_quote! { (usize, (bool, usize)) };
    let p_commit_with_state = HydroNode::CrossSingleton {
        left: Box::new(p_commit_batched),
        right: Box::new(p_state_for_commit),
        metadata: metadata_at(p_tick, bounded_stream(commit_with_state_type), bt),
    };
    let p_state_after_commit = HydroNode::FilterMap {
        f: {
            let e: syn::Expr = syn::parse_quote!(
                |(committed_clock, (_is_idle, logical_clock)): (usize, (bool, usize))| {
                    if committed_clock == logical_clock {
                        Some((true, logical_clock + 1))
                    } else {
                        None
                    }
                }
            );
            e.into()
        },
        input: Box::new(p_commit_with_state),
        metadata: metadata_at(p_tick, bounded_optional(state_type.clone()), bt),
    };

    // 2. Update state upon receiving a prepare
    let p_prepare_cycle = builder.next_cycle_id();
    let p_prepare_source = HydroNode::CycleSource {
        cycle_id: p_prepare_cycle,
        metadata: metadata_at(p_tick, bounded_stream(usize_type.clone()), bt),
    };
    let p_prepare_shared = Rc::new(RefCell::new(p_prepare_source));
    let p_prepare_for_state = HydroNode::Tee {
        inner: SharedNode(p_prepare_shared.clone()),
        metadata: metadata_at(p_tick, bounded_stream(usize_type.clone()), bt),
    };
    let p_state_after_prepare = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|clock: usize| (false, clock));
            e.into()
        },
        input: Box::new(p_prepare_for_state),
        metadata: metadata_at(p_tick, bounded_optional(state_type), bt),
    };
    update_state(
        &mut state.new_cycle_sinks,
        &p_state_tee,
        p_cycle,
        p_state_after_commit,
        p_state_after_prepare,
        p_tick,
        bt,
    );

    // 3. ACK the prepare
    let p_ack_tee = HydroNode::Tee {
        inner: SharedNode(p_prepare_shared),
        metadata: metadata_at(p_tick, bounded_stream(usize_type.clone()), bt),
    };
    let p_ack_unbounded = HydroNode::YieldConcat {
        inner: Box::new(p_ack_tee),
        metadata: metadata_at(cluster_loc, unbounded_stream(syn::parse_quote!(usize)), bt),
    };
    let mut p_ack_to_coordinator = p_ack_unbounded;
    send_to_coordinator(
        &mut p_ack_to_coordinator,
        cluster_loc,
        coord_loc,
        partitioner.num_partitions,
    );
    state.new_cycle_sinks.push(HydroRoot::CycleSink {
        cycle_id: c_prepare_ack_cycle,
        input: Box::new(p_ack_to_coordinator),
        op_metadata: op_meta(bt),
    });

    state.p_prepare_signal_cycle = Some(p_prepare_cycle);
}

/// Per replicated input:
/// 1. Reroute to coordinator
/// 2. Assign a sequence number, buffer in DeferTick cycle
/// 3. If the coordinator is ready for prepare, broadcast the value
/// 4. Partitions extract the value, buffer it, and prepare to ACK
fn handle_replicated_input(
    node: &mut HydroNode,
    state: &mut PartialPartitionState,
    partitioner: &PartialPartitioner,
    builder: &mut FlowBuilder,
) {
    let original_node = std::mem::replace(node, HydroNode::Placeholder);
    let node_metadata = original_node.metadata().clone();
    let element_type: syn::Type =
        (*collection_kind_to_debug_type(&node_metadata.collection_kind).0).clone();
    let cluster_loc = &LocationId::Cluster(partitioner.location_id);
    let coord_loc = &state.coordinator_location.clone();
    let coord_tick = &state.coordinator_tick.clone();
    let bt = &node_metadata;

    let value_buffer_cycle = builder.next_cycle_id();

    // 1. Reroute from sender to coordinator
    let mut c_input = original_node;
    send_to_coordinator(
        &mut c_input,
        &node_metadata.location_id,
        coord_loc,
        partitioner.num_partitions,
    );

    // 2. Assign logical clock
    let c_batched_input = HydroNode::Batch {
        inner: Box::new(c_input),
        metadata: metadata_at(coord_tick, bounded_stream(element_type.clone()), bt),
    };
    let enumerated_type: syn::Type = syn::parse_quote! { (usize, #element_type) };
    let c_enumerated = HydroNode::Enumerate {
        input: Box::new(c_batched_input),
        metadata: metadata_at(coord_tick, bounded_stream(enumerated_type.clone()), bt),
    };
    let c_max_clock_for_assign = HydroNode::Tee {
        inner: SharedNode(state.max_assigned_clock_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(coord_tick, bounded_singleton(syn::parse_quote!(usize)), bt),
    };
    let with_base_type: syn::Type = syn::parse_quote! { ((usize, #element_type), usize) };
    let c_enumerated_with_base = HydroNode::CrossSingleton {
        left: Box::new(c_enumerated),
        right: Box::new(c_max_clock_for_assign),
        metadata: metadata_at(coord_tick, bounded_stream(with_base_type), bt),
    };
    let clocked_type: syn::Type = syn::parse_quote! { (usize, #element_type) };
    let c_clocked_values = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|((index, value), base_clock): (
                (usize, _),
                usize
            )| (base_clock + index, value));
            e.into()
        },
        input: Box::new(c_enumerated_with_base),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    // ── Update max_assigned_clock: max(clock) + 1 from clocked values ──
    let c_clocked_shared = Rc::new(RefCell::new(c_clocked_values));
    let c_clocked_for_max = HydroNode::Tee {
        inner: SharedNode(c_clocked_shared.clone()),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_new_max = HydroNode::Reduce {
        f: {
            let e: syn::Expr = syn::parse_quote!(|max: &mut usize, next: usize| {
                if next > *max {
                    *max = next;
                }
            });
            e.into()
        },
        input: Box::new(HydroNode::Map {
            f: {
                let e: syn::Expr = syn::parse_quote!(|(clock, _val): (usize, _)| clock + 1);
                e.into()
            },
            input: Box::new(c_clocked_for_max),
            metadata: metadata_at(coord_tick, bounded_stream(syn::parse_quote!(usize)), bt),
        }),
        metadata: metadata_at(coord_tick, bounded_optional(syn::parse_quote!(usize)), bt),
    };
    // Update the tee so the next replicated input starts from this new max.
    // ChainFirst: use new max if this input had values, else keep previous max
    let c_prev_max_fallback = HydroNode::Tee {
        inner: SharedNode(state.max_assigned_clock_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(coord_tick, bounded_singleton(syn::parse_quote!(usize)), bt),
    };
    let c_updated_max = HydroNode::ChainFirst {
        first: Box::new(c_new_max),
        second: Box::new(c_prev_max_fallback),
        metadata: metadata_at(coord_tick, bounded_singleton(syn::parse_quote!(usize)), bt),
    };
    let c_new_max_tee = Rc::new(RefCell::new(c_updated_max));
    state.max_assigned_clock_tee = Some(c_new_max_tee);

    // 3. Buffer replicated values
    let c_clocked_for_buffer = HydroNode::Tee {
        inner: SharedNode(c_clocked_shared),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_all_buffered = persisted_stream_with_initial(
        value_buffer_cycle,
        c_clocked_for_buffer,
        clocked_type.clone(),
        coord_tick,
        bt,
    );
    let all_buffered_tee = Rc::new(RefCell::new(c_all_buffered));

    // 4. Release value when clock matches prepare_clock (Join on clock)
    let c_prepare_keyed = HydroNode::Tee {
        inner: SharedNode(state.c_prepare_keyed_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(
            coord_tick,
            bounded_optional(syn::parse_quote!((usize, ()))),
            bt,
        ),
    };
    let c_buffered_for_release = HydroNode::Tee {
        inner: SharedNode(all_buffered_tee.clone()),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let joined_type: syn::Type = syn::parse_quote! { (usize, (#element_type, ())) };
    let c_joined = HydroNode::Join {
        left: Box::new(c_buffered_for_release),
        right: Box::new(c_prepare_keyed),
        metadata: metadata_at(coord_tick, bounded_stream(joined_type), bt),
    };
    let c_released = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|(clock, (value, ()))| (clock, value));
            e.into()
        },
        input: Box::new(c_joined),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_released_unbounded = HydroNode::YieldConcat {
        inner: Box::new(c_released),
        metadata: metadata_at(coord_loc, unbounded_stream(clocked_type.clone()), bt),
    };
    let mut c_prepare_broadcast = c_released_unbounded;
    broadcast_to_partitions(
        &mut c_prepare_broadcast,
        coord_loc,
        cluster_loc,
        partitioner.num_partitions,
    );

    // 5. Partitions receive the prepare and ACK
    let p_prepare_shared = Rc::new(RefCell::new(c_prepare_broadcast));
    let p_tick = &state.partition_tick.clone();
    // Output: extract T from (clock, T) — batched into partition tick
    let p_value_tee = HydroNode::Tee {
        inner: SharedNode(p_prepare_shared.clone()),
        metadata: metadata_at(cluster_loc, unbounded_stream(clocked_type.clone()), bt),
    };
    let p_value_batched = HydroNode::Batch {
        inner: Box::new(p_value_tee),
        metadata: metadata_at(p_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let p_output_value = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|(_clock, value)| value);
            e.into()
        },
        input: Box::new(p_value_batched),
        metadata: metadata_at(p_tick, node_metadata.collection_kind.clone(), bt),
    };
    *node = p_output_value;
    // Feed prepare signal: extract clock, Reduce to single Optional<usize>
    let p_prepare_signal_tee = HydroNode::Tee {
        inner: SharedNode(p_prepare_shared.clone()),
        metadata: metadata_at(cluster_loc, unbounded_stream(clocked_type.clone()), bt),
    };
    let p_prepare_clock = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|(clock, _value): (usize, _)| clock);
            e.into()
        },
        input: Box::new(p_prepare_signal_tee),
        metadata: metadata_at(cluster_loc, unbounded_stream(syn::parse_quote!(usize)), bt),
    };
    let p_prepare_clock_batched = HydroNode::Batch {
        inner: Box::new(p_prepare_clock),
        metadata: metadata_at(p_tick, bounded_stream(syn::parse_quote!(usize)), bt),
    };
    state.p_prepare_clock_streams.push(p_prepare_clock_batched);
    // Ack is handled centrally in build_partition_state

    // 6. Coordinator keeps un-released values in the buffer
    let c_buffered_for_rebuffer = HydroNode::Tee {
        inner: SharedNode(all_buffered_tee),
        metadata: metadata_at(
            coord_tick,
            bounded_stream(syn::parse_quote!((usize, #element_type))),
            bt,
        ),
    };
    let c_prepare_for_rebuffer = HydroNode::Tee {
        inner: SharedNode(state.c_prepare_clock_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(coord_tick, bounded_optional(syn::parse_quote!(usize)), bt),
    };
    let c_remaining_buffered = HydroNode::AntiJoin {
        pos: Box::new(c_buffered_for_rebuffer),
        neg: Box::new(c_prepare_for_rebuffer),
        metadata: metadata_at(
            coord_tick,
            bounded_stream(syn::parse_quote!((usize, #element_type))),
            bt,
        ),
    };
    state.new_cycle_sinks.push(HydroRoot::CycleSink {
        cycle_id: value_buffer_cycle,
        input: Box::new(c_remaining_buffered),
        op_metadata: op_meta(bt),
    });
}

/// Per partitioned input: buffer during Prepare, passthrough when Idle.
///
/// 1. Chain new messages with buffered messages
/// 2. If is_idle → passthrough buffered messages, !is_idle → buffer to next tick
fn handle_partitioned_input(
    node: &mut HydroNode,
    state: &mut PartialPartitionState,
    builder: &mut FlowBuilder,
) {
    let original_node = std::mem::replace(node, HydroNode::Placeholder);
    let node_metadata = original_node.metadata().clone();
    let element_type: syn::Type =
        (*collection_kind_to_debug_type(&node_metadata.collection_kind).0).clone();
    let p_tick = &state.partition_tick.clone();
    let bt = &node_metadata;

    // 1. Chain incoming messages with buffered messages
    let p_input_batched = HydroNode::Batch {
        inner: Box::new(original_node),
        metadata: metadata_at(p_tick, bounded_stream(element_type.clone()), bt),
    };
    let buffer_cycle = builder.next_cycle_id();
    let p_all = persisted_stream_with_initial(
        buffer_cycle,
        p_input_batched,
        element_type.clone(),
        p_tick,
        bt,
    );

    // 2. Check current state
    let p_state = HydroNode::Tee {
        inner: SharedNode(state.p_state_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(
            p_tick,
            bounded_singleton(syn::parse_quote!((bool, usize))),
            bt,
        ),
    };
    let with_state_type: syn::Type = syn::parse_quote! { (#element_type, (bool, usize)) };
    let p_with_state = HydroNode::CrossSingleton {
        left: Box::new(p_all),
        right: Box::new(p_state),
        metadata: metadata_at(p_tick, bounded_stream(with_state_type.clone()), bt),
    };
    let shared = Rc::new(RefCell::new(p_with_state));
    let f: syn::Expr = syn::parse_quote!(|(_, (is_idle, _)): &(_, (bool, usize))| *is_idle);
    let p_pass = HydroNode::Partition {
        inner: SharedNode(shared.clone()),
        f: f.clone().into(),
        is_true: true,
        metadata: metadata_at(p_tick, bounded_stream(with_state_type.clone()), bt),
    };
    let p_buf = HydroNode::Partition {
        inner: SharedNode(shared),
        f: f.into(),
        is_true: false,
        metadata: metadata_at(p_tick, bounded_stream(with_state_type), bt),
    };

    // 2. ALlow passthrough if idle, continue buffering otherwise
    let p_output = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|(elem, _)| elem);
            e.into()
        },
        input: Box::new(p_pass),
        metadata: metadata_at(p_tick, bounded_stream(element_type.clone()), bt),
    };
    *node = p_output;
    // Buffer
    let p_buffered = HydroNode::Map {
        f: {
            let e: syn::Expr = syn::parse_quote!(|(elem, _)| elem);
            e.into()
        },
        input: Box::new(p_buf),
        metadata: metadata_at(p_tick, bounded_stream(element_type), bt),
    };
    state.new_cycle_sinks.push(HydroRoot::CycleSink {
        cycle_id: buffer_cycle,
        input: Box::new(p_buffered),
        op_metadata: op_meta(bt),
    });
}

/// Broadcast garbage collection inputs (for this original node) by modifying the network node.
fn handle_garbage_collection_input(node: &mut HydroNode, num_partitions: usize) {
    if let HydroNode::Network { input, .. } = node {
        let input_node = std::mem::replace(input.as_mut(), HydroNode::Placeholder);
        let metadata = input_node.metadata().clone();
        *input.as_mut() = HydroNode::FlatMap {
            f: {
                let e: syn::Expr = syn::parse_quote!(|(dest, payload)| {
                    let base = dest.into_tagless().get_raw_id() as usize * #num_partitions;
                    (0u32..#num_partitions as u32).map(move |i| (
                        hydro_lang::location::MemberId::<()>::from_raw_id(base as u32 + i),
                        payload.clone()
                    ))
                });
                e.into()
            },
            input: Box::new(input_node),
            metadata,
        };
    } else {
        panic!("Expected a Network node for garbage collection input");
    }
}

/// Limitations: Can only partition 1 cluster at a time.
/// Assumes that the coordinator is colocated with the partitioned nodes?
pub fn partial_partition(
    builder: &mut FlowBuilder,
    ir: &mut Vec<HydroRoot>,
    partitioner: PartialPartitioner,
) -> Option<PartialPartitionState> {
    if partitioner.nodes_to_replicate.is_empty() {
        let regular_partitioner = Partitioner {
            nodes_before_partitioned_input: partitioner.nodes_before_partitioned_input,
            num_partitions: partitioner.num_partitions,
            location_id: partitioner.location_id,
            new_cluster_id: partitioner.new_cluster_id,
        };
        partition(ir, &regular_partitioner);
        return None;
    }

    let coordinator_cluster = builder.cluster::<()>();
    let coordinator_location = hydro_lang::location::Location::id(&coordinator_cluster);
    let clock_id = builder.next_clock_id();
    let coordinator_tick = LocationId::Tick(clock_id, Box::new(coordinator_location.clone()));
    let cluster_location = LocationId::Cluster(partitioner.location_id);
    let partition_clock_id = builder.next_clock_id();
    let partition_tick = LocationId::Tick(partition_clock_id, Box::new(cluster_location));
    let c_prepare_ack_cycle = builder.next_cycle_id();
    let backtrace_source = ir[0].input_metadata().clone();

    let mut state = PartialPartitionState {
        new_cycle_sinks: Vec::new(),
        coordinator_location,
        coordinator_tick,
        partition_tick,
        coordinator_state_tee: None,
        max_assigned_clock_tee: None,
        p_state_tee: None,
        p_prepare_signal_cycle: None,
        p_prepare_clock_streams: Vec::new(),
        c_prepare_clock_tee: None,
        c_prepare_keyed_tee: None,
    };

    // 1. Build coordinator on separate single-member Cluster
    let (commit_at_partitions, max_assigned_clock_cycle) = build_coordinator(
        &mut state,
        &partitioner,
        builder,
        &backtrace_source,
        c_prepare_ack_cycle,
    );

    // 2. Build partition state: (is_idle, logical_clock)
    build_partition_state(
        &mut state,
        commit_at_partitions,
        &partitioner,
        builder,
        &backtrace_source,
        c_prepare_ack_cycle,
    );

    // 3. Per-node processing
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            let Some(op_id) = node.op_metadata().id else {
                return;
            };
            if partitioner.nodes_to_replicate.contains(&op_id) {
                handle_replicated_input(node, &mut state, &partitioner, builder);
            } else if partitioner.nodes_after_partitioned_input.contains(&op_id) {
                handle_partitioned_input(node, &mut state, builder);
            } else if partitioner.nodes_for_garbage_collection.contains(&op_id) {
                handle_garbage_collection_input(node, partitioner.num_partitions);
            }
        },
    );

    // 3b. CycleSink for max_assigned_clock (uses the final chained tee after all replicated inputs)
    let final_max = HydroNode::Tee {
        inner: SharedNode(state.max_assigned_clock_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(
            &state.coordinator_tick,
            bounded_singleton(syn::parse_quote!(usize)),
            &backtrace_source,
        ),
    };
    state.new_cycle_sinks.push(HydroRoot::CycleSink {
        cycle_id: max_assigned_clock_cycle,
        input: Box::new(final_max),
        op_metadata: op_meta(&backtrace_source),
    });

    // 3c. Chain all prepare clock streams from replicated inputs into one CycleSink
    if let Some(p_prepare_cycle) = state.p_prepare_signal_cycle {
        let combined =
            state
                .p_prepare_clock_streams
                .drain(..)
                .reduce(|acc, next| HydroNode::Chain {
                    first: Box::new(acc),
                    second: Box::new(next),
                    metadata: metadata_at(
                        &state.partition_tick,
                        bounded_stream(syn::parse_quote!(usize)),
                        &backtrace_source,
                    ),
                });
        if let Some(combined) = combined {
            state.new_cycle_sinks.push(HydroRoot::CycleSink {
                cycle_id: p_prepare_cycle,
                input: Box::new(combined),
                op_metadata: op_meta(&backtrace_source),
            });
        }
    }

    // 4. Add pending roots
    ir.append(&mut state.new_cycle_sinks);

    // 5. Regular partitioning
    let regular_partitioner = Partitioner {
        nodes_before_partitioned_input: partitioner.nodes_before_partitioned_input,
        num_partitions: partitioner.num_partitions,
        location_id: partitioner.location_id,
        new_cluster_id: partitioner.new_cluster_id,
    };
    partition(ir, &regular_partitioner);

    Some(state)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use hydro_lang::compile::ir::{HydroNode, deep_clone, traverse_dfir};
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use stageleft::q;

    use super::{PartialPartitioner, partial_partition};
    use crate::repair::inject_id;
    use crate::rewrites::{NetworkType, get_network_type};

    fn apply_partial_partition<'a>(
        built: &'a hydro_lang::compile::built::BuiltFlow<'a>,
        server_key: hydro_lang::location::LocationKey,
    ) -> FlowBuilder<'a> {
        let mut ir = deep_clone(built.ir());
        inject_id(&mut ir);

        let mut recv_networks: Vec<usize> = vec![];
        let mut send_nodes: Vec<(usize, usize)> = vec![]; // (send-side input op_id, recv Network op_id)
        traverse_dfir(
            &mut ir,
            |_, _| {},
            |node, next_stmt_id| {
                if matches!(get_network_type(node, &server_key), Some(NetworkType::Recv)) {
                    if let HydroNode::Network { input, .. } = node {
                        if let Some(send_id) = input.op_metadata().id {
                            send_nodes.push((send_id, *next_stmt_id));
                        }
                    }
                    recv_networks.push(*next_stmt_id);
                }
            },
        );
        assert_eq!(recv_networks.len(), 2);
        assert_eq!(send_nodes.len(), 2);

        let partitioner = PartialPartitioner {
            nodes_to_replicate: HashSet::from([recv_networks[0]]),
            nodes_before_partitioned_input: HashMap::from([(
                send_nodes[1].0,
                vec!["0".to_string()],
            )]),
            nodes_after_partitioned_input: HashSet::from([send_nodes[1].1]),
            nodes_for_garbage_collection: HashSet::new(),
            num_partitions: 3,
            location_id: server_key,
            new_cluster_id: None,
        };

        let mut new_builder = FlowBuilder::from_built(built);
        partial_partition(&mut new_builder, &mut ir, partitioner);
        new_builder.replace_ir(ir);
        new_builder
    }

    #[test]
    fn test_partial_partition_ir() {
        let mut builder = FlowBuilder::new();
        let client = builder.cluster::<()>();
        let server = builder.cluster::<()>();
        let server_key = hydro_lang::location::Location::id(&server).key();

        let state_input = client
            .source_iter(q!(vec![42usize]))
            .broadcast(&server, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values();
        let kv_input = client
            .source_iter(q!(vec![(1usize, 2usize)]))
            .broadcast(&server, TCP.fail_stop().bincode(), nondet!(/** test */))
            .values();
        let tick = server.tick();

        state_input
            .batch(&tick, nondet!(/** test */))
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .reduce(q!(|prev, new| {
                *prev = new;
            }))
            .all_ticks()
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .for_each(q!(|_: usize| {}));

        kv_input
            .batch(&tick, nondet!(/** test */))
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .into_keyed()
            .reduce(q!(|prev, new| {
                *prev = new;
            }))
            .entries()
            .all_ticks()
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .for_each(q!(|_: (usize, usize)| {}));

        let built = builder.optimize_with(|_| {});
        let new_builder = apply_partial_partition(&built, server_key);
        let _ = new_builder.finalize();
    }

    #[tokio::test]
    async fn test_partial_partition_e2e() {
        run_e2e(true).await;
    }

    #[tokio::test]
    async fn test_e2e_no_partition() {
        run_e2e(false).await;
    }

    async fn run_e2e(apply_partition: bool) {
        use futures::{SinkExt, StreamExt};
        use hydro_deploy::Deployment;

        let mut builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let client = builder.process::<()>();
        let server = builder.cluster::<()>();
        let server_key = hydro_lang::location::Location::id(&server).key();

        let (state_send, state_external) =
            client.source_external_bincode::<_, usize, _, _>(&external);
        let state_input =
            state_external.broadcast(&server, TCP.fail_stop().bincode(), nondet!(/** test */));

        let (kv_send, kv_external) =
            client.source_external_bincode::<_, (usize, usize), _, _>(&external);
        let kv_input =
            kv_external.broadcast(&server, TCP.fail_stop().bincode(), nondet!(/** test */));

        let tick = server.tick();

        let state_out = state_input
            .batch(&tick, nondet!(/** test */))
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .reduce(q!(|prev, new| {
                *prev = new;
            }))
            .all_ticks()
            .send(&client, TCP.fail_stop().bincode())
            .values()
            .send_bincode_external(&external);

        let kv_out = kv_input
            .batch(&tick, nondet!(/** test */))
            .assume_ordering::<TotalOrder>(nondet!(/** test */))
            .assume_retries::<ExactlyOnce>(nondet!(/** test */))
            .into_keyed()
            .reduce(q!(|prev, new| {
                *prev = new;
            }))
            .entries()
            .all_ticks()
            .send(&client, TCP.fail_stop().bincode())
            .values()
            .send_bincode_external(&external);

        let built = builder.optimize_with(|_| {});

        let mut deployment = Deployment::new();
        let num_partitions = 3;
        let nodes = if apply_partition {
            let new_builder = apply_partial_partition(&built, server_key);
            new_builder
                .with_process(&client, deployment.Localhost())
                .with_external(&external, deployment.Localhost())
                .with_remaining_clusters(|| {
                    (0..num_partitions)
                        .map(|_| deployment.Localhost())
                        .collect::<Vec<_>>()
                })
                .deploy(&mut deployment)
        } else {
            built
                .with_process(&client, deployment.Localhost())
                .with_external(&external, deployment.Localhost())
                .with_remaining_clusters(|| vec![deployment.Localhost()])
                .deploy(&mut deployment)
        };

        deployment.deploy().await.unwrap();

        let mut state_send = nodes.connect(state_send).await;
        let mut kv_send = nodes.connect(kv_send).await;
        let mut state_out = nodes.connect(state_out).await;
        let mut kv_out = nodes.connect(kv_out).await;

        deployment.start().await.unwrap();

        // Send: partitioned value, then replicated value, then another partitioned value
        kv_send.send((1usize, 100usize)).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        state_send.send(42usize).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        kv_send.send((1usize, 200usize)).await.unwrap();

        let timeout = std::time::Duration::from_secs(30);

        // Expect state=42
        let state_val = tokio::time::timeout(timeout, state_out.next())
            .await
            .expect("timeout waiting for state")
            .expect("state channel closed");
        assert_eq!(state_val, 42usize);

        // Expect kv output eventually containing (1, 200)
        let mut found_200 = false;
        for _ in 0..10 {
            match tokio::time::timeout(std::time::Duration::from_secs(5), kv_out.next()).await {
                Ok(Some(val)) => {
                    assert_eq!(val.0, 1);
                    if val.1 == 200 {
                        found_200 = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(found_200, "Expected KV output (1, 200)");
    }
}
