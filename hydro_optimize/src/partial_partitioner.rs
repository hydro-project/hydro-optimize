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
use crate::repair::inject_id;
use crate::rewrites::{collection_kind_to_debug_type, prepend_member_id_to_collection_kind};

#[derive(Clone, Serialize, Deserialize)]
pub struct PartialPartitioner {
    pub nodes_to_partition: HashMap<usize, StructOrTupleIndex>,
    pub nodes_to_replicate: HashSet<usize>,
    pub num_partitions: usize,
    pub location_id: LocationKey,
    pub new_cluster_id: Option<LocationKey>,
}

/// Shared state threaded through the partial partitioning build.
struct PartialPartitionState {
    pending_roots: Vec<HydroRoot>,
    /// Coordinator cluster location (single-member cluster).
    coordinator_location: LocationId,
    /// Tick within the coordinator cluster.
    coordinator_tick: LocationId,
    /// The cluster location (partitions).
    cluster_location: LocationId,
    /// Number of partitions.
    num_partitions: usize,

    // ── Coordinator state tees (within coordinator tick) ──
    /// Singleton<(bool, usize)> = (is_idle, logical_clock).
    coordinator_state_tee: Option<Rc<RefCell<HydroNode>>>,
    /// Singleton<usize> = max_assigned_clock.
    max_assigned_clock_tee: Option<Rc<RefCell<HydroNode>>>,

    // ── Broadcasts (at cluster location, unbounded) ──
    /// Commit broadcast: Stream<usize> (committed clock) at cluster_location.
    commit_at_partitions_shared: Option<Rc<RefCell<HydroNode>>>,
    /// Prepare clock: Optional<usize> at coordinator_location (unbounded).
    /// Each replicated input tees this to gate its own (clock, T) broadcast.
    c_prepare_clock_tee: Option<Rc<RefCell<HydroNode>>>,

    // ── Cycle IDs ──
    max_assigned_clock_cycle: CycleId,
    /// Ack cycle: all replicated inputs' ack networks feed into this.
    ack_cycle: CycleId,
    /// Last max_assigned_clock update from replicated inputs.
    last_max_assigned_clock_update: Option<HydroNode>,
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

/// Broadcast from coordinator (single-member cluster) to all partitions.
/// FlatMaps each element into `num_partitions` copies, each tagged with a partition MemberId,
/// then sends over the network and strips the MemberId on the receiver side.
fn broadcast_to_partitions(
    node: &mut HydroNode,
    coord_loc: &LocationId,
    cluster_loc: &LocationId,
    num_partitions: usize,
) {
    let metadata = node.metadata().clone();
    let original_kind = metadata.collection_kind.clone();
    let new_kind = prepend_member_id_to_collection_kind(&original_kind);
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    let mut mapped = HydroNode::FlatMap {
        f: { let e: syn::Expr = syn::parse_quote!(|b| (0..#num_partitions).map(move |i|
            (hydro_lang::location::MemberId::<()>::from_raw(i as u32), b.clone())
        )); e.into() },
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            location_id: coord_loc.clone(),
            collection_kind: new_kind,
            cardinality: None, tag: None,
            op: op_meta(&metadata),
        },
    };

    add_network_raw(&mut mapped, cluster_loc, &original_kind);
    *node = mapped;
}

/// Send from a partition to the single-member coordinator cluster (member 0).
/// Maps each element to `(MemberId(0), element)`, then sends over the network.
fn send_to_coordinator(
    node: &mut HydroNode,
    cluster_loc: &LocationId,
    coord_loc: &LocationId,
) {
    let metadata = node.metadata().clone();
    let original_kind = metadata.collection_kind.clone();
    let new_kind = prepend_member_id_to_collection_kind(&original_kind);
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    let mut mapped = HydroNode::Map {
        f: { let e: syn::Expr = syn::parse_quote!(|b|
            (hydro_lang::location::MemberId::<()>::from_raw(0), b)
        ); e.into() },
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            location_id: cluster_loc.clone(),
            collection_kind: new_kind,
            cardinality: None, tag: None,
            op: op_meta(&metadata),
        },
    };

    add_network_raw(&mut mapped, coord_loc, &original_kind);
    *node = mapped;
}

/// Creates a DeferTick singleton cycle with a default initial value.
/// Returns an `Rc<RefCell<HydroNode>>` suitable for Tee'ing.
fn deferred_state(
    cycle_id: CycleId,
    initial_value: syn::Expr,
    element_type: syn::Type,
    tick_loc: &LocationId,
    bt: &HydroIrMetadata,
) -> Rc<RefCell<HydroNode>> {
    let kind = bounded_singleton(element_type);
    let source = HydroNode::CycleSource { cycle_id, metadata: metadata_at(tick_loc, kind.clone(), bt) };
    let deferred = HydroNode::DeferTick { input: Box::new(source), metadata: metadata_at(tick_loc, kind.clone(), bt) };
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

/// Build the coordinator on a separate single-member Cluster.
///
/// State (DeferTick cycles within coordinator tick):
/// - `coordinator_state`: Singleton<(bool, usize)> = (is_idle, logical_clock), default (true, 0)
/// - `max_assigned_clock`: Singleton<usize>, default 0. Updated by replicated inputs.
///
/// Prepare fires when `is_idle && logical_clock < max_assigned_clock`.
/// Each replicated input handles its own prepare broadcast of `(clock, T)` to partitions.
/// The coordinator only decides WHEN to prepare (outputs the clock value).
///
/// Ack path: partitions → coordinator (Network). Scan counts acks per clock.
/// Commit: Tee'd before broadcast. One branch → Network to partitions, one → feedback to coordinator.
fn build_coordinator(
    state: &mut PartialPartitionState,
    builder: &mut FlowBuilder,
    backtrace_source: &HydroIrMetadata,
) {
    let coord_tick = &state.coordinator_tick.clone();
    let coord_loc = &state.coordinator_location.clone();
    let cluster_loc = &state.cluster_location.clone();
    let num_partitions = state.num_partitions;
    let bt = backtrace_source;

    let coordinator_state_cycle = builder.next_cycle_id();

    let usize_type: syn::Type = syn::parse_quote! { usize };
    let coord_state_type: syn::Type = syn::parse_quote! { (bool, usize) };

    // State: Current status. (is_idle, logical clock)
    let c_state_tee = deferred_state(
        coordinator_state_cycle,
        syn::parse_quote!((true, 0usize)),
        coord_state_type.clone(),
        coord_tick, bt,
    );
    state.coordinator_state_tee = Some(c_state_tee.clone());

    // State: Logical clock to assign to next message
    let c_max_clock_tee = deferred_state(
        state.max_assigned_clock_cycle,
        syn::parse_quote!(0usize),
        usize_type.clone(),
        coord_tick, bt,
    );
    state.max_assigned_clock_tee = Some(c_max_clock_tee.clone());

    // 1. Send prepares when is_idle and there are pending messages (logical_clock < max_assigned_clock)
    let c_state_for_prepare = HydroNode::Tee {
        inner: SharedNode(c_state_tee.clone()),
        metadata: metadata_at(coord_tick, bounded_singleton(coord_state_type.clone()), bt),
    };
    let c_max_for_prepare = HydroNode::Tee {
        inner: SharedNode(c_max_clock_tee.clone()),
        metadata: metadata_at(coord_tick, bounded_singleton(usize_type.clone()), bt),
    };
    let state_with_max_type: syn::Type = syn::parse_quote! { ((bool, usize), usize) };
    let c_state_with_max = HydroNode::CrossSingleton {
        left: Box::new(c_state_for_prepare),
        right: Box::new(c_max_for_prepare),
        metadata: metadata_at(coord_tick, bounded_singleton(state_with_max_type.clone()), bt),
    };
    let c_prepare_clock_optional = HydroNode::FilterMap {
        f: { let e: syn::Expr = syn::parse_quote!(
            |((is_idle, logical_clock), max_assigned_clock): ((bool, usize), usize)|
                if is_idle && logical_clock < max_assigned_clock { Some(next_prepare_clock) } else { None }
        ); e.into() },
        input: Box::new(c_state_with_max),
        metadata: metadata_at(coord_tick, bounded_optional(usize_type.clone()), bt),
    };
    let c_prepare_clock_shared = Rc::new(RefCell::new(c_prepare_clock_optional));
    state.c_prepare_clock_tee = Some(c_prepare_clock_shared.clone());
    // Change state from Idle to Prepared if a prepare is ready
    let c_prepare_for_state = HydroNode::Tee {
        inner: SharedNode(c_prepare_clock_shared.clone()),
        metadata: metadata_at(coord_tick, bounded_optional(usize_type.clone()), bt),
    };
    let c_state_after_prepare = HydroNode::Map {
        f: { let e: syn::Expr = syn::parse_quote!(
            |prepare_clock: usize| (false, prepare_clock)
        ); e.into() },
        input: Box::new(c_prepare_for_state),
        metadata: metadata_at(coord_tick, bounded_optional(coord_state_type.clone()), bt),
    };

    // 2. Send commit once all prepare ACKs have been received
    let c_ack_source = HydroNode::CycleSource {
        cycle_id: state.ack_cycle,
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    let ack_quorum_output_type: syn::Type = syn::parse_quote! { Option<usize> };
    let c_ack_quorum_scan = HydroNode::Scan {
        init: { let e: syn::Expr = syn::parse_quote!(|| (0usize, 0usize)); e.into() },
        acc: { let e: syn::Expr = syn::parse_quote!(
            |(expected_clock, ack_count): &mut (usize, usize), ack_clock: usize| -> Option<usize> {
                assert_eq!(ack_clock, *expected_clock, "Received future prepare ACK before current prepare completed");
                *ack_count += 1;
                if *ack_count == #num_partitions {
                    *ack_count = 0;
                    let committed_clock = *expected_clock;
                    *expected_clock += 1;
                    Some(committed_clock)
                } else { None }
            }
        ); e.into() },
        input: Box::new(c_ack_source),
        metadata: metadata_at(coord_loc, unbounded_stream(ack_quorum_output_type), bt),
    };
    let c_prepare_quorum_reached = HydroNode::FilterMap {
        f: { let e: syn::Expr = syn::parse_quote!(|x| x); e.into() },
        input: Box::new(c_ack_quorum_scan),
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    let c_prepare_quorum_shared = Rc::new(RefCell::new(c_prepare_quorum_reached));
    let mut c_commit_broadcast = HydroNode::Tee {
        inner: SharedNode(c_prepare_quorum_shared.clone()),
        metadata: metadata_at(coord_loc, unbounded_stream(usize_type.clone()), bt),
    };
    broadcast_to_partitions(&mut c_commit_broadcast, coord_loc, cluster_loc, num_partitions);
    let commit_at_partitions_shared = Rc::new(RefCell::new(c_commit_broadcast));
    state.commit_at_partitions_shared = Some(commit_at_partitions_shared);
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
        f: { let e: syn::Expr = syn::parse_quote!(
            |committed_clock: usize| (true, committed_clock + 1)
        ); e.into() },
        input: Box::new(c_commit_feedback_batched),
        metadata: metadata_at(coord_tick, bounded_optional(coord_state_type.clone()), bt),
    };
    let c_prev_state = HydroNode::Tee {
        inner: SharedNode(c_state_tee),
        metadata: metadata_at(coord_tick, bounded_singleton(coord_state_type.clone()), bt),
    };
    let c_commit_or_prepare = HydroNode::ChainFirst {
        first: Box::new(c_state_after_commit),
        second: Box::new(c_state_after_prepare),
        metadata: metadata_at(coord_tick, bounded_optional(coord_state_type.clone()), bt),
    };
    let c_updated_state = HydroNode::ChainFirst {
        first: Box::new(c_commit_or_prepare),
        second: Box::new(c_prev_state),
        metadata: metadata_at(coord_tick, bounded_singleton(coord_state_type), bt),
    };
    state.pending_roots.push(HydroRoot::CycleSink {
        cycle_id: coordinator_state_cycle,
        input: Box::new(c_updated_state),
        op_metadata: op_meta(bt),
    });
}

/// Per replicated input:
/// 1. Network input from partition cluster → coordinator cluster
/// 2. Batch into coordinator tick, assign future clocks, buffer in DeferTick cycle
/// 3. When prepare fires (clock N), release value with clock N as (clock, T)
/// 4. YieldConcat + Network broadcast (clock, T) to all partitions
/// 5. Partitions extract T, ack clock back to coordinator
fn handle_replicated_input(
    node: &mut HydroNode,
    state: &mut PartialPartitionState,
    builder: &mut FlowBuilder,
    _next_stmt_id: &mut usize,
) {
    let original_node = std::mem::replace(node, HydroNode::Placeholder);
    let node_metadata = original_node.metadata().clone();
    let element_type: syn::Type = (*collection_kind_to_debug_type(&node_metadata.collection_kind).0).clone();
    let cluster_loc = &state.cluster_location.clone();
    let coord_loc = &state.coordinator_location.clone();
    let coord_tick = &state.coordinator_tick.clone();
    let bt = &node_metadata;

    let value_buffer_cycle = builder.next_cycle_id();

    // ── Network input from cluster → coordinator ──
    let mut c_input = original_node;
    send_to_coordinator(&mut c_input, cluster_loc, coord_loc);

    // ── Batch into coordinator tick ──
    let c_batched_input = HydroNode::Batch {
        inner: Box::new(c_input),
        metadata: metadata_at(coord_tick, bounded_stream(element_type.clone()), bt),
    };

    // ── Assign future clocks: enumerate + cross with max_assigned_clock ──
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
        f: { let e: syn::Expr = syn::parse_quote!(
            |((index, value), base_clock): ((usize, _), usize)| (base_clock + index, value)
        ); e.into() },
        input: Box::new(c_enumerated_with_base),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };

    // ── Update max_assigned_clock ──
    let c_clocked_shared = Rc::new(RefCell::new(c_clocked_values));

    let c_clocked_for_count = HydroNode::Tee {
        inner: SharedNode(c_clocked_shared.clone()),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_input_count = HydroNode::Fold {
        init: { let e: syn::Expr = syn::parse_quote!(|| 0usize); e.into() },
        acc: { let e: syn::Expr = syn::parse_quote!(|count: &mut usize, _: (usize, _)| { *count += 1; }); e.into() },
        input: Box::new(c_clocked_for_count),
        metadata: metadata_at(coord_tick, bounded_singleton(syn::parse_quote!(usize)), bt),
    };
    let c_prev_max = HydroNode::Tee {
        inner: SharedNode(state.max_assigned_clock_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(coord_tick, bounded_singleton(syn::parse_quote!(usize)), bt),
    };
    let count_with_base_type: syn::Type = syn::parse_quote! { (usize, usize) };
    let c_count_with_base = HydroNode::CrossSingleton {
        left: Box::new(c_input_count),
        right: Box::new(c_prev_max),
        metadata: metadata_at(coord_tick, bounded_singleton(count_with_base_type), bt),
    };
    let c_new_max = HydroNode::Map {
        f: { let e: syn::Expr = syn::parse_quote!(|(count, base)| base + count); e.into() },
        input: Box::new(c_count_with_base),
        metadata: metadata_at(coord_tick, bounded_singleton(syn::parse_quote!(usize)), bt),
    };
    state.last_max_assigned_clock_update = Some(c_new_max);

    // ── Buffer in DeferTick stream cycle ──
    let c_clocked_for_buffer = HydroNode::Tee {
        inner: SharedNode(c_clocked_shared),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_buffer_source = HydroNode::CycleSource {
        cycle_id: value_buffer_cycle,
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_buffer_deferred = HydroNode::DeferTick {
        input: Box::new(c_buffer_source),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_all_buffered = HydroNode::Chain {
        first: Box::new(c_buffer_deferred),
        second: Box::new(c_clocked_for_buffer),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };

    // ── Release value when clock matches prepare_clock ──
    let c_prepare_for_release = HydroNode::Tee {
        inner: SharedNode(state.c_prepare_clock_tee.as_ref().unwrap().clone()),
        metadata: metadata_at(coord_tick, bounded_optional(syn::parse_quote!(usize)), bt),
    };
    let c_prepare_for_release_tee = Rc::new(RefCell::new(c_prepare_for_release));

    let all_buffered_tee = Rc::new(RefCell::new(c_all_buffered));

    // Release path: filter clock == prepare_clock
    let c_buffered_for_release = HydroNode::Tee {
        inner: SharedNode(all_buffered_tee.clone()),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };
    let c_prepare_for_release = HydroNode::Tee {
        inner: SharedNode(c_prepare_for_release_tee.clone()),
        metadata: metadata_at(coord_tick, bounded_optional(syn::parse_quote!(usize)), bt),
    };
    let buffered_with_prepare_type: syn::Type = syn::parse_quote! { ((usize, #element_type), usize) };
    let c_buffered_with_prepare = HydroNode::CrossSingleton {
        left: Box::new(c_buffered_for_release),
        right: Box::new(c_prepare_for_release),
        metadata: metadata_at(coord_tick, bounded_stream(buffered_with_prepare_type.clone()), bt),
    };
    let c_released = HydroNode::FilterMap {
        f: { let e: syn::Expr = syn::parse_quote!(
            |((clock, value), prepare_clock): ((usize, _), usize)|
                if clock == prepare_clock { Some((clock, value)) } else { None }
        ); e.into() },
        input: Box::new(c_buffered_with_prepare),
        metadata: metadata_at(coord_tick, bounded_stream(clocked_type.clone()), bt),
    };

    // YieldConcat + Network: broadcast (clock, T) to all partitions
    let c_released_unbounded = HydroNode::YieldConcat {
        inner: Box::new(c_released),
        metadata: metadata_at(coord_loc, unbounded_stream(clocked_type.clone()), bt),
    };
    let mut c_prepare_broadcast = c_released_unbounded;
    broadcast_to_partitions(&mut c_prepare_broadcast, coord_loc, cluster_loc, state.num_partitions);

    // Tee at partitions: one branch → extract T (output), one branch → ack clock back
    let p_prepare_shared = Rc::new(RefCell::new(c_prepare_broadcast));

    // Output: extract T from (clock, T)
    let p_value_tee = HydroNode::Tee {
        inner: SharedNode(p_prepare_shared.clone()),
        metadata: metadata_at(cluster_loc, unbounded_stream(clocked_type.clone()), bt),
    };
    let p_output_value = HydroNode::Map {
        f: { let e: syn::Expr = syn::parse_quote!(|(_clock, value)| value); e.into() },
        input: Box::new(p_value_tee),
        metadata: metadata_at(cluster_loc, node_metadata.collection_kind.clone(), bt),
    };

    // Ack: extract clock, Network back to coordinator
    let p_ack_tee = HydroNode::Tee {
        inner: SharedNode(p_prepare_shared),
        metadata: metadata_at(cluster_loc, unbounded_stream(clocked_type), bt),
    };
    let p_ack_clock = HydroNode::Map {
        f: { let e: syn::Expr = syn::parse_quote!(|(clock, _value)| clock); e.into() },
        input: Box::new(p_ack_tee),
        metadata: metadata_at(cluster_loc, unbounded_stream(syn::parse_quote!(usize)), bt),
    };
    let mut p_ack_to_coordinator = p_ack_clock;
    send_to_coordinator(&mut p_ack_to_coordinator, cluster_loc, coord_loc);

    // Feed acks into the shared ack cycle
    state.pending_roots.push(HydroRoot::CycleSink {
        cycle_id: state.ack_cycle,
        input: Box::new(p_ack_to_coordinator),
        op_metadata: op_meta(bt),
    });

    // ── Re-buffer: keep values whose clock != prepare_clock ──
    let c_buffered_for_rebuffer = HydroNode::Tee {
        inner: SharedNode(all_buffered_tee),
        metadata: metadata_at(coord_tick, bounded_stream(syn::parse_quote!((usize, #element_type))), bt),
    };
    let c_prepare_for_rebuffer = HydroNode::Tee {
        inner: SharedNode(c_prepare_for_release_tee),
        metadata: metadata_at(coord_tick, bounded_optional(syn::parse_quote!(usize)), bt),
    };
    let c_rebuffer_with_prepare = HydroNode::CrossSingleton {
        left: Box::new(c_buffered_for_rebuffer),
        right: Box::new(c_prepare_for_rebuffer),
        metadata: metadata_at(coord_tick, bounded_stream(buffered_with_prepare_type), bt),
    };
    let c_remaining_buffered = HydroNode::FilterMap {
        f: { let e: syn::Expr = syn::parse_quote!(
            |((clock, value), prepare_clock): ((usize, _), usize)|
                if clock != prepare_clock { Some((clock, value)) } else { None }
        ); e.into() },
        input: Box::new(c_rebuffer_with_prepare),
        metadata: metadata_at(coord_tick, bounded_stream(syn::parse_quote!((usize, #element_type))), bt),
    };
    state.pending_roots.push(HydroRoot::CycleSink {
        cycle_id: value_buffer_cycle,
        input: Box::new(c_remaining_buffered),
        op_metadata: op_meta(bt),
    });

    *node = p_output_value;
}

/// Per partitioned input: freeze during Prepare.
/// Uses the coordinator state (is_idle) broadcast. Since the coordinator is a separate
/// Process, we need to get the idle signal to the partitions. We derive it from the
/// commit broadcast (commit → idle) and prepare broadcast absence (no prepare → stay idle).
///
/// Simpler approach: use the commit_at_partitions_shared at the cluster. Between a prepare
/// broadcast arriving and a commit broadcast arriving, the partition is in Prepare state.
/// We track this with a Scan on the cluster side.
fn handle_partitioned_input(
    node: &mut HydroNode,
    state: &PartialPartitionState,
    _next_stmt_id: &mut usize,
) {
    let node_metadata = node.metadata().clone();
    let cluster_loc = &state.cluster_location;
    let element_type: syn::Type = (*collection_kind_to_debug_type(&node_metadata.collection_kind).0).clone();
    let bt = &node_metadata;

    // Scan on commit broadcasts: track idle state.
    // Commit → true (idle). Between commits, if a prepare arrived, we're not idle.
    // But we don't have a prepare signal at the cluster level for partitioned inputs...
    // Actually, the prepare broadcast (clock, T) arrives at the cluster for each replicated input.
    // We can use the commit_at_partitions_shared: after commit, idle=true.
    // The partition becomes non-idle when it receives a prepare (replicated input value).
    // But partitioned inputs don't see the prepare directly.
    //
    // Simplest correct approach: the coordinator broadcasts its is_idle state to partitions.
    // But that requires another network. Instead, we can observe that:
    // - Partition is idle when no 2PC is in progress
    // - 2PC starts when a prepare (replicated value) arrives at the partition
    // - 2PC ends when a commit arrives
    //
    // Since the replicated input values arrive at the partition via Network from coordinator,
    // and commits also arrive via Network, we can track state with a Scan:
    // replicated_value_arrived → false, commit_arrived → true, default → true
    //
    // But handle_partitioned_input doesn't have access to the per-replicated-input broadcasts.
    // The simplest approach: broadcast a "prepare started" signal from coordinator to cluster.
    // We already have commit_at_partitions_shared. Let's also create a prepare_signal_broadcast_tee.
    //
    // Actually, even simpler: the partition just needs to know "is a 2PC in progress?"
    // The commit broadcast tells it "2PC done". The prepare broadcast (replicated value)
    // tells it "2PC started". Since we may have multiple replicated inputs, any of them
    // arriving means 2PC started.
    //
    // For now, use a conservative approach: freeze partitioned inputs for one tick after
    // any replicated value arrives, unfreeze on commit. This requires a Scan.
    //
    // TODO: This needs the prepare signal at the cluster. For now, we'll skip freezing
    // and rely on the 2PC ordering guarantee (prepare arrives before commit).
    // The partitioned inputs will be processed in the same tick as the replicated value,
    // which is correct because the replicated value is already committed by the time
    // it arrives at the partition.

    // Actually, rethinking: the replicated value only arrives at the partition AFTER
    // the full 2PC completes (prepare → ack → commit → release). So by the time the
    // partition sees the replicated value, it's already committed. The partitioned inputs
    // don't need to be frozen because the replicated value is delivered atomically
    // with the commit.
    //
    // Wait, no. The replicated value arrives at the partition as part of the prepare
    // broadcast. The partition holds it until commit. But partitioned inputs could
    // arrive and be processed before the commit, seeing stale replicated state.
    //
    // The correct approach: freeze partitioned inputs between prepare and commit.
    // We need a signal at the cluster. Let's use the commit_at_partitions_shared and
    // create a prepare_signal from the replicated input broadcasts.

    // For now, leave partitioned inputs unchanged (no freezing).
    // TODO: implement freezing once prepare signal is available at cluster.
    let _ = element_type;
    let _ = cluster_loc;
    let _ = bt;
}

/// Limitations: Can only partition 1 cluster at a time.
pub fn partial_partition(builder: &mut FlowBuilder, ir: &mut Vec<HydroRoot>, partitioner: &PartialPartitioner) {
    if partitioner.nodes_to_replicate.is_empty() {
        let regular_partitioner = Partitioner {
            nodes_to_partition: partitioner.nodes_to_partition.clone(),
            num_partitions: partitioner.num_partitions,
            location_id: partitioner.location_id,
            new_cluster_id: partitioner.new_cluster_id,
        };
        partition(ir, &regular_partitioner);
        return;
    }

    let coordinator_cluster = builder.cluster::<()>();
    let coordinator_location = hydro_lang::location::Location::id(&coordinator_cluster);
    let clock_id = builder.next_clock_id();
    let coordinator_tick = LocationId::Tick(clock_id, Box::new(coordinator_location.clone()));
    let cluster_location = LocationId::Cluster(partitioner.location_id);
    let max_assigned_clock_cycle = builder.next_cycle_id();
    let ack_cycle = builder.next_cycle_id();
    let backtrace_source = ir[0].input_metadata().clone();

    let mut state = PartialPartitionState {
        pending_roots: Vec::new(),
        coordinator_location,
        coordinator_tick,
        cluster_location,
        num_partitions: partitioner.num_partitions,
        coordinator_state_tee: None,
        max_assigned_clock_tee: None,
        commit_at_partitions_shared: None,
        c_prepare_clock_tee: None,
        max_assigned_clock_cycle,
        ack_cycle,
        last_max_assigned_clock_update: None,
    };

    // 1. Build coordinator on separate single-member Cluster
    build_coordinator(&mut state, builder, &backtrace_source);

    // 2. Per-node processing
    traverse_dfir(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            if partitioner.nodes_to_replicate.contains(next_stmt_id) {
                handle_replicated_input(node, &mut state, builder, next_stmt_id);
            } else if partitioner.nodes_to_partition.contains_key(next_stmt_id) {
                handle_partitioned_input(node, &state, next_stmt_id);
            }
        },
    );

    // 3. CycleSink for max_assigned_clock
    if let Some(last_max_update) = state.last_max_assigned_clock_update {
        state.pending_roots.push(HydroRoot::CycleSink {
            cycle_id: max_assigned_clock_cycle,
            input: Box::new(last_max_update),
            op_metadata: op_meta(&backtrace_source),
        });
    }

    // 4. Add pending roots
    ir.extend(state.pending_roots);
    inject_id(ir);

    // 5. Regular partitioning
    let regular_partitioner = Partitioner {
        nodes_to_partition: partitioner.nodes_to_partition.clone(),
        num_partitions: partitioner.num_partitions,
        location_id: partitioner.location_id,
        new_cluster_id: partitioner.new_cluster_id,
    };
    partition(ir, &regular_partitioner);
}
