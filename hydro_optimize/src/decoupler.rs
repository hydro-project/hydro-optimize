use std::cell::RefCell;
use std::{collections::HashMap, rc::Rc};

use hydro_lang::compile::ir::{
    CollectionKind, DebugExpr, DebugInstantiate, HydroNode, HydroRoot, HydroSource, SharedNode,
    traverse_dfir,
};
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::networking::{NetworkingInfo, TcpFault};
use stageleft::quote_type;
use syn::visit_mut::VisitMut;

use crate::debug::print_id;
use crate::partition_syn_analysis::{StructOrTuple, StructOrTupleIndex};
use crate::repair::{cycle_source_to_sink_input, inject_id, inject_location};
use crate::rewrites::{
    ClusterSelfIdReplace, Rewrite, collection_kind_to_debug_type, deserialize_bincode_with_type,
    prepend_member_id_to_collection_kind, serialize_bincode_with_type, tee_to_inner_id,
    unbounded_optional, unbounded_singleton, unbounded_stream,
};

/// Rewrites `ClusterMembers(partitioned_location, ...)` sources so each partitioned replica only
/// observes one canonical member per original cluster member.
pub fn replace_cluster_members_source(
    node: &mut HydroNode,
    partitioned_location: &LocationId,
    num_partitions: usize,
) {
    let location_key = partitioned_location.key();
    if let HydroNode::Source {
        source: HydroSource::ClusterMembers(target_loc, _),
        ..
    } = node
        && target_loc.root().key() == location_key
    {
        let source_node = std::mem::replace(node, HydroNode::Placeholder);
        let source_metadata = source_node.metadata().clone();
        *node = HydroNode::FilterMap {
            f: {
                let e: syn::Expr = syn::parse_quote!(
                    |(id, event): (hydro_lang::location::TaglessMemberId, _)| {
                        let raw = id.get_raw_id();
                        if raw % #num_partitions as u32 == 0 {
                            Some((hydro_lang::location::TaglessMemberId::from_raw_id(raw / #num_partitions as u32), event))
                        } else {
                            None
                        }
                    }
                );
                e.into()
            },
            input: Box::new(source_node),
            metadata: source_metadata,
        };
    }
}

struct NetworkMetadata {
    sender_location: LocationId,
    /// number of partitions on the sender side (1 if not partitioned or is not considered in decoupling/partitioning).
    sender_partitions: usize,
    receiver_location: LocationId,
    /// number of partitions on the receiver side (1 if not partitioned or is not considered in decoupling/partitioning).
    receiver_partitions: usize,
    /// if hash-based partitioning, the field to hash on (None otherwise).
    partition_field: Option<StructOrTupleIndex>,
    /// whether this is for a new network being added during decoupling, or an existing network that needs to be modified to add partition routing.
    new: bool,
}

/// Creates the Map before Network to route it to the correct partition.
fn map_before_network(node: &mut HydroNode, network_metadata: &NetworkMetadata) {
    let metadata = node.metadata().clone();
    let node_content = std::mem::replace(node, HydroNode::Placeholder);
    let element_type: syn::Type =
        (*collection_kind_to_debug_type(&metadata.collection_kind).0).clone();
    let num_receiver_partitions = network_metadata.receiver_partitions;

    let partition_val: syn::Expr = if network_metadata.receiver_partitions > 0 {
        if let Some(field) = &network_metadata.partition_field {
            // If partitioning and there is a field to hash on, use it
            let struct_or_tuple: syn::Expr = syn::parse_quote! { struct_or_tuple };
            let struct_or_tuple_with_fields = StructOrTuple::to_syn_expr(struct_or_tuple, field);
            syn::parse_quote!({
                let mut s = ::std::hash::DefaultHasher::new();
                ::std::hash::Hash::hash(&#struct_or_tuple_with_fields, &mut s);
                ::std::hash::Hasher::finish(&s) as u32
            })
        } else {
            // If partitioning and there is no field to hash on, we must be random partitioning
            syn::parse_quote!(::rand::random::<u32>())
        }
    } else {
        // Otherwise set to 0, so the offset isn't affected
        syn::parse_quote!(0)
    };

    let f: syn::Expr = if network_metadata.new {
        // New network type is just T. We need (receiver MemberId, T)
        let ident = syn::Ident::new(
            &format!(
                "__hydro_lang_cluster_self_id_{}",
                network_metadata.sender_location.key()
            ),
            proc_macro2::Span::call_site(),
        );
        syn::parse_quote!(
            |struct_or_tuple: #element_type| {
                let orig_raw = #ident.get_raw_id();
                let dest = (orig_raw * #num_receiver_partitions as u32) + (#partition_val % #num_receiver_partitions as u32);
                (hydro_lang::location::MemberId::<()>::from_raw_id(dest), struct_or_tuple)
            }
        )
    } else {
        syn::parse_quote!(
            |(orig_dest, struct_or_tuple): #element_type| {
                let orig_raw = orig_dest.into_tagless().get_raw_id();
                let dest = (orig_raw * #num_receiver_partitions as u32) + (#partition_val % #num_receiver_partitions as u32);
                (hydro_lang::location::MemberId::<()>::from_raw_id(dest), struct_or_tuple)
            }
        )
    };

    let mut new_metadata = metadata.clone();
    if network_metadata.new {
        new_metadata.collection_kind =
            prepend_member_id_to_collection_kind(&metadata.collection_kind);
        new_metadata.location_id = network_metadata.sender_location.clone();
    }

    *node = HydroNode::Map {
        f: f.into(),
        input: Box::new(node_content),
        metadata: new_metadata,
    };
}

/// Maps the sender ID on the receiver's side of the network back to the original (unpartitioned) node's ID.
fn map_after_network(node: &mut HydroNode, network_metadata: &NetworkMetadata) {
    if network_metadata.sender_partitions == 0 {
        // If sender isn't partitioned, then there's nothing to remap
        return;
    }

    if let HydroNode::Network {
        input, metadata, ..
    } = node
        && input.metadata().location_id.root() == network_metadata.sender_location.root()
    {
        let metadata = metadata.clone();
        let sender_num_partitions = network_metadata.sender_partitions;
        let node_content = std::mem::replace(node, HydroNode::Placeholder);
        let f: syn::Expr = syn::parse_quote!(|(sender_id, b)| (
            hydro_lang::location::MemberId::<_>::from_raw_id(sender_id.into_tagless().get_raw_id() / #sender_num_partitions as u32),
            b
        ));
        *node = HydroNode::Map {
            f: f.into(),
            input: Box::new(node_content),
            metadata,
        };
    }
}

/// Replaces the node with:
/// 1. Map (to decide where to send to)
/// 2. Network
/// 3. Map (to remove member id)
fn add_new_network(node: &mut HydroNode, network_metadata: NetworkMetadata) {
    assert!(
        network_metadata.new,
        "add_new_network should only be called for new networks being added during decoupling"
    );

    // 1. Map
    map_before_network(node, &network_metadata);

    let metadata = node.metadata().clone();
    let collection_kind = metadata.collection_kind.clone();
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    // 2. Network
    let output_debug_type = collection_kind_to_debug_type(&collection_kind);
    let network_node = HydroNode::Network {
        name: None,
        networking_info: NetworkingInfo::Tcp {
            fault: TcpFault::FailStop,
        },
        serialize_fn: Some(serialize_bincode_with_type(true, &output_debug_type)).map(|e| e.into()),
        instantiate_fn: DebugInstantiate::Building,
        deserialize_fn: Some(deserialize_bincode_with_type(
            Some(&quote_type::<()>()),
            &output_debug_type,
        ))
        .map(|e| e.into()),
        input: Box::new(node_content),
        metadata: network_metadata
            .receiver_location
            .clone()
            .new_node_metadata(collection_kind.clone()),
    };

    // 3. Map
    let f: syn::Expr = syn::parse_quote!(|(_, b)| b);
    *node = HydroNode::Map {
        f: f.into(),
        input: Box::new(network_node),
        metadata: network_metadata
            .receiver_location
            .new_node_metadata(collection_kind),
    };
}

/// Decouples the output of an Optional.
/// # Goal
/// Detect whenever the value of the Optional changes, and only send those changes over the network.
///
/// # Mechanism
/// 1. Find the current value, whether it is Some or None.
/// - Map the Optional to Some(value)
/// - Create a Singleton with None
/// - ChainFirst to either get a Some(value) or None every tick
/// 2. Detect changes.
/// - Creates Scan. Output Some(None) if the value hasn't changed, Some(Some(new)) if it has. (Can't output None since that would terminate the stream.). Note that if the value changed to None, we would output Some(Some(None)). The Scan then unwraps 1 layer on output.
/// 3. Only send on change.
/// - FlatMap to remove 1 layer and reveal the new value (either None or Some(value))
/// - Add networking
/// 4. Turn back into an Optional at the recipient.
/// - Reduce to store the latest value
/// - FilterMap to convert into Optional
fn decouple_optional(node: &mut HydroNode, network_metadata: NetworkMetadata) {
    let node_content = std::mem::replace(node, HydroNode::Placeholder);
    let node_type: syn::Type =
        (*collection_kind_to_debug_type(&node_content.metadata().collection_kind).0).clone();
    let input_location = network_metadata.sender_location.clone();
    let new_location = network_metadata.receiver_location.clone();

    // 1.1 Map to Some(value)
    let map_f: syn::Expr = syn::parse_quote!(|x| Some(x));
    let optional_node_type: syn::Type = syn::parse_quote!(Option<#node_type>);
    let optional_collection_kind = unbounded_optional(optional_node_type.clone());
    let map = HydroNode::Map {
        f: map_f.into(),
        input: Box::new(node_content),
        metadata: input_location
            .clone()
            .new_node_metadata(optional_collection_kind.clone()),
    };

    // 1.2 Create Singleton with None
    let singleton_value: syn::Expr = syn::parse_quote!(None);
    let singleton = HydroNode::SingletonSource {
        value: singleton_value.into(),
        first_tick_only: false,
        metadata: input_location
            .clone()
            .new_node_metadata(unbounded_singleton(optional_node_type.clone())),
    };

    // 1.3 ChainFirst
    // NOTE: We technically need to batch the map and singleton before we chain. We don't for 2 reasons: 1) We can't create the ClockId needed for the Tick, since it is private, and 2) It's technically OK because the singleton is static.
    let chain = HydroNode::ChainFirst {
        first: Box::new(map),
        second: Box::new(singleton),
        metadata: input_location
            .clone()
            .new_node_metadata(optional_collection_kind.clone()),
    };

    // 2.1. Scan to detect changes
    // - If the value hasn't changed, output Some(None)
    // - If the value changed to nothing, output Some(Some(None))
    // - If the value changed to something, output Some(Some(Some(new)))
    // Note that the output type will rip off the outermost Some.
    let scan_output_type: syn::Type = syn::parse_quote!(Option<Option<#node_type>>);
    let scan_init: syn::Expr = syn::parse_quote!(|| None);
    let scan_acc: syn::Expr = syn::parse_quote!(|prev: &mut #optional_node_type, new: #optional_node_type| {
        if *prev == new {
            Some(None)
        } else {
            *prev = new.clone();
            Some(Some(new))
        }
    });
    let scan = HydroNode::Scan {
        init: scan_init.into(),
        acc: scan_acc.into(),
        input: Box::new(chain),
        metadata: input_location
            .clone()
            .new_node_metadata(unbounded_stream(scan_output_type)),
    };

    // 3.1. FlatMap to reveal the new value
    let flat_map_f: syn::Expr = syn::parse_quote!(|x| x);
    let mut flat_map = HydroNode::FlatMap {
        f: flat_map_f.into(),
        input: Box::new(scan),
        metadata: input_location
            .clone()
            .new_node_metadata(unbounded_stream(optional_node_type.clone())),
    };

    // 3.2. Networking
    add_new_network(&mut flat_map, network_metadata);

    // 4.1. Reduce
    // - If there is no value, is an empty stream
    // - If the value was erased, is a stream with None
    // - If the value was changed to something, is a stream with Some(value)
    let reduce_f: syn::Expr = syn::parse_quote!(|prev: &mut #optional_node_type, new: #optional_node_type| {
        *prev = new;
    });
    let reduce = HydroNode::Reduce {
        f: reduce_f.into(),
        input: Box::new(flat_map), // Has been replaced with network
        metadata: new_location
            .clone()
            .new_node_metadata(unbounded_optional(optional_node_type)),
    };

    // 4.2 FilterMap (so Nones disappear)
    let filter_map_f: syn::Expr = syn::parse_quote!(|x| x);
    let filter_map = HydroNode::FilterMap {
        f: filter_map_f.into(),
        input: Box::new(reduce),
        metadata: new_location
            .clone()
            .new_node_metadata(unbounded_optional(node_type)),
    };

    *node = filter_map;
}

/// If a network is to be inserted before this node, insert it.
/// Accounts for special cases for Tee and Optional.
fn insert_network(
    node: &mut HydroNode,
    op_id: usize,
    rewrite: &Rewrite,
    locations_map: &HashMap<usize, LocationId>,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
    tee_to_inner_id_before_rewrites: &HashMap<usize, usize>,
) {
    // Return if we don't need to insert anything
    let Some((sender_location_idx, receiver_location_idx)) =
        rewrite.possible_rewrite.op_to_network.get(&op_id)
    else {
        return;
    };

    // Construct NetworkMetadata
    let sender_location = locations_map.get(sender_location_idx).unwrap().clone();
    let receiver_location = locations_map.get(receiver_location_idx).unwrap();
    let sender_partitions = if rewrite
        .possible_rewrite
        .partitionable
        .contains(sender_location_idx)
    {
        rewrite.num_partitions
    } else {
        0
    };
    let receiver_partitions = if rewrite
        .possible_rewrite
        .partitionable
        .contains(receiver_location_idx)
    {
        rewrite.num_partitions
    } else {
        0
    };
    let partition_field = rewrite
        .possible_rewrite
        .partition_field_choices
        .get(&op_id)
        .cloned();
    let network_metadata = NetworkMetadata {
        sender_location,
        sender_partitions,
        receiver_location: receiver_location.clone(),
        receiver_partitions,
        partition_field,
        new: true,
    };

    match node {
        HydroNode::Partition { .. } => {
            panic!(
                "Partition node (op {}) cannot be decoupled from its inner, because although there are 2 branches of partitioning, emit_core emits the entire block ONCE for both branches",
                op_id
            );
        }
        HydroNode::Tee { .. } => {
            let inner_id = *tee_to_inner_id_before_rewrites.get(&op_id).unwrap();
            let metadata = node.metadata().clone();
            let new_inner = new_inners
                .entry((inner_id, receiver_location.clone()))
                .or_insert_with(|| {
                    println!(
                        "Adding network before Tee to location {:?} after id: {}",
                        receiver_location, inner_id
                    );
                    add_new_network(node, network_metadata);
                    let node_content = std::mem::replace(node, HydroNode::Placeholder);
                    Rc::new(RefCell::new(node_content))
                })
                .clone();

            *node = HydroNode::Tee {
                inner: SharedNode(new_inner),
                metadata,
            };
        }
        _ if matches!(
            node.metadata().collection_kind,
            CollectionKind::Optional { .. }
        ) =>
        {
            // TODO: If a node is simultaneously a Tee and Optional, decoupling may be broken
            decouple_optional(node, network_metadata);
        }
        _ => {
            println!(
                "Creating network to location {:?} after id: {}",
                receiver_location, op_id
            );

            add_new_network(node, network_metadata);
        }
    }
}

/// If the source/destination of an existing Network is now partitioned, we need to recalculate the sending/receiving IDs.
fn repair_existing_network_for_partitioning(
    node: &mut HydroNode,
    op_id: usize,
    rewrite: &Rewrite,
    locations_map: &HashMap<usize, LocationId>,
) {
    let HydroNode::Network {
        input, metadata, ..
    } = node
    else {
        return;
    };
    let sender_op_id = input.op_metadata().id.unwrap();
    let sender_location_idx = rewrite.possible_rewrite.op_to_loc.get(&sender_op_id);
    let sender_location = sender_location_idx
        .map(|idx| locations_map.get(idx).unwrap().clone())
        .unwrap_or_else(|| input.metadata().location_id.clone());
    let sender_partitions = if let Some(idx) = sender_location_idx
        && rewrite.possible_rewrite.partitionable.contains(idx)
    {
        rewrite.num_partitions
    } else {
        0
    };
    let receiver_location_idx = rewrite.possible_rewrite.op_to_loc.get(&op_id);
    let receiver_location = receiver_location_idx
        .map(|idx| locations_map.get(idx).unwrap().clone())
        .unwrap_or_else(|| metadata.location_id.clone());
    let receiver_partitions = if let Some(idx) = receiver_location_idx
        && rewrite.possible_rewrite.partitionable.contains(idx)
    {
        rewrite.num_partitions
    } else {
        0
    };
    let partition_field = rewrite
        .possible_rewrite
        .partition_field_choices
        .get(&op_id)
        .cloned();

    let network_metadata = NetworkMetadata {
        sender_location,
        sender_partitions,
        receiver_location,
        receiver_partitions,
        partition_field,
        new: false,
    };

    // If the receiver is now partitioned, add a Map before the Network to route to the correct partition
    if receiver_partitions > 0 {
        map_before_network(node, &network_metadata);
    }
    // If the source is now partitioned, departition the ID on the receiving end
    if sender_partitions > 0 {
        map_after_network(node, &network_metadata);
    }
}

/// 1. Inserts network between original/decoupled operators
/// 2. Repairs network to/from partitioned operators
/// 3. Places decoupled nodes on their new location
///
/// Note: The location of a node is where its OUTPUT goes; it is NOT where the node is executed.
fn decouple_node(
    node: &mut HydroNode,
    rewrite: &Rewrite,
    locations_map: &HashMap<usize, LocationId>,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
    tee_to_inner_id_before_rewrites: &HashMap<usize, usize>,
) {
    let op_id = node.metadata().op.id.unwrap();

    insert_network(
        node,
        op_id,
        rewrite,
        locations_map,
        new_inners,
        tee_to_inner_id_before_rewrites,
    );

    repair_existing_network_for_partitioning(node, op_id, rewrite, locations_map);

    // Place on new location
    if let Some(new_locatio_idx) = rewrite.possible_rewrite.op_to_loc.get(&op_id) {
        let new_location = locations_map.get(new_locatio_idx).unwrap();
        node.metadata_mut().location_id = new_location.clone();
    }
}

// References to a node's own ID must be adjusted since the name and number of members change
fn replace_cluster_self_id_node(
    visit_debug_expr: &mut impl FnMut(&mut dyn FnMut(&mut DebugExpr)),
    rewrite: &Rewrite,
    orig_location: &LocationId,
    locations_map: &HashMap<usize, LocationId>,
    target_loc_idx: usize,
) {
    let target_location = locations_map.get(&target_loc_idx).unwrap();
    // If the new location is partitioned, then we need to know how many partitions there are to divide
    let num_partitions = if rewrite
        .possible_rewrite
        .partitionable
        .contains(&target_loc_idx)
    {
        rewrite.num_partitions
    } else {
        0
    };

    let orig_key = orig_location.key();
    let new_key = target_location.key();

    // If we're decoupling or partitioning, replace
    if orig_key != new_key || num_partitions > 0 {
        visit_debug_expr(&mut |expr| {
            let mut visitor = ClusterSelfIdReplace {
                orig_cluster_id: orig_key,
                new_cluster_id: new_key,
                num_partitions,
            };
            visitor.visit_expr_mut(&mut expr.0);
        });
    }
}

/// Calls `replace_cluster_self_id_node` on all HydroRoots and HydroNodes.
/// For nodes, also rewrites ClusterMembers sources and receiver sender IDs when partitioning.
fn replace_cluster_self_id(
    ir: &mut [HydroRoot],
    rewrite: &Rewrite,
    locations_map: &HashMap<usize, LocationId>,
    orig_location: &LocationId,
) {
    traverse_dfir(
        ir,
        |root, _| {
            let target_loc_idx = root
                .input_metadata()
                .op
                .id
                .and_then(|id| rewrite.possible_rewrite.op_to_loc.get(&id).copied())
                .unwrap_or(0);
            replace_cluster_self_id_node(
                &mut |f| root.visit_debug_expr(f),
                rewrite,
                orig_location,
                locations_map,
                target_loc_idx,
            );
        },
        |node, _| {
            let Some(op_id) = node.op_metadata().id else {
                return;
            };

            let target_loc_idx = rewrite
                .possible_rewrite
                .op_to_loc
                .get(&op_id)
                .copied()
                .unwrap_or(0);
            replace_cluster_self_id_node(
                &mut |f| node.visit_debug_expr(f),
                rewrite,
                orig_location,
                locations_map,
                target_loc_idx,
            );

            if rewrite
                .possible_rewrite
                .partitionable
                .contains(&target_loc_idx)
            {
                let target_location = locations_map.get(&target_loc_idx).unwrap();
                replace_cluster_members_source(node, target_location, rewrite.num_partitions);
            }
        },
    );
}

/// `locations_map`: Mapping from each location in the rewrites to a (potentially new) LocationId.
pub fn decouple(
    ir: &mut [HydroRoot],
    rewrite: &Rewrite,
    locations_map: &HashMap<usize, LocationId>,
) {
    if rewrite.possible_rewrite.op_to_loc.is_empty()
        && rewrite.possible_rewrite.partitionable.is_empty()
    {
        // No rewriting to do
        println!("No decoupling or partitioning needed");
        return;
    }

    let tee_to_inner_id_before_rewrites = tee_to_inner_id(ir);

    let orig_location = locations_map
        .get(&0)
        .expect("locations_map must contain the original location under key 0");

    // References to a node's own ID must be adjusted since the name and number of members change
    replace_cluster_self_id(ir, rewrite, locations_map, orig_location);

    let mut new_inners = HashMap::new();
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            decouple_node(
                node,
                rewrite,
                locations_map,
                &mut new_inners,
                &tee_to_inner_id_before_rewrites,
            );
        },
    );

    // Fix locations since we changed some
    inject_id(ir);
    let cycles = cycle_source_to_sink_input(ir);
    inject_location(ir, &cycles);
    print_id(ir);
}
