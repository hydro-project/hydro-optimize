use std::cell::RefCell;
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::compile::ir::{
    DebugInstantiate, HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, TeeNode,
    transform_bottom_up, traverse_dfir,
};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Cluster, Location};
use proc_macro2::Span;
use stageleft::quote_type;
use syn::visit_mut::VisitMut;

use crate::repair::{cycle_source_to_sink_input, inject_id, inject_location};
use crate::rewrites::{
    ClusterSelfIdReplace, collection_kind_to_debug_type, deserialize_bincode_with_type, op_id_to_inputs, prepend_member_id_to_collection_kind, serialize_bincode_with_type, tee_to_inner_id
};

/// Mapping from op id to location index, starting from 0.
///
/// Note: The location index is where the output of the node goes, not necessarily where the node is executed.
/// Limitations: Sources can't be decoupled from their children.
pub type DecoupleDecision = HashMap<usize, usize>;

/// Adds networking after `node`.
/// - `send_location`: the source of the network.
/// - `recv_location`: the destination of the network
///
/// Replaces the node with:
/// 1. Map (to add destination member id)
/// 2. Network
/// 3. Map (to remove member id)
fn add_network(node: &mut HydroNode, send_location: &LocationId, recv_location: &LocationId) {
    let metadata = node.metadata().clone();
    // If the decoupled node is a cluster, then we must ensure that after a decoupling,
    // each node sends to its own respective decoupled member.
    // Doesn't matter which location we use; it will be replaced with `ClusterSelfIdReplace`.
    let member_id = send_location.key();
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    // Map from b to (MemberId, b)
    let ident = syn::Ident::new(
        &format!("__hydro_lang_cluster_self_id_{}", member_id),
        Span::call_site(),
    );
    let f: syn::Expr = syn::parse_quote!(|b| (
        hydro_lang::location::MemberId::<()>::from_tagless(#ident.clone()),
        b
    ));

    // Calculate the new CollectionKind
    let original_collection_kind = metadata.collection_kind.clone();
    let new_collection_kind = prepend_member_id_to_collection_kind(&original_collection_kind);

    let mapped_node = HydroNode::Map {
        f: f.into(),
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            location_id: send_location.clone(),
            collection_kind: new_collection_kind.clone(),
            cardinality: None,
            tag: None,
            op: HydroIrOpMetadata {
                backtrace: metadata.op.backtrace.clone(),
                cpu_usage: None,
                network_recv_cpu_usage: None,
                id: None,
            },
        },
    };

    // Set up the network node
    let output_debug_type = collection_kind_to_debug_type(&original_collection_kind);
    let network_node = HydroNode::Network {
        name: None,
        serialize_fn: Some(serialize_bincode_with_type(true, &output_debug_type)).map(|e| e.into()),
        instantiate_fn: DebugInstantiate::Building,
        deserialize_fn: Some(deserialize_bincode_with_type(
            Some(&quote_type::<()>()),
            &output_debug_type,
        ))
        .map(|e| e.into()),
        input: Box::new(mapped_node),
        metadata: HydroIrMetadata {
            location_id: recv_location.clone(),
            collection_kind: new_collection_kind,
            cardinality: None,
            tag: None,
            op: HydroIrOpMetadata {
                backtrace: metadata.op.backtrace.clone(),
                cpu_usage: None,
                network_recv_cpu_usage: None,
                id: None,
            },
        },
    };

    // Map again to remove the member Id
    let f: syn::Expr = syn::parse_quote!(|(_, b)| b);
    let mapped_node = HydroNode::Map {
        f: f.into(),
        input: Box::new(network_node),
        metadata: HydroIrMetadata {
            location_id: recv_location.clone(),
            collection_kind: original_collection_kind,
            cardinality: None,
            tag: None,
            op: HydroIrOpMetadata {
                backtrace: metadata.op.backtrace,
                cpu_usage: None,
                network_recv_cpu_usage: None,
                id: None,
            },
        },
    };
    *node = mapped_node;
}

/// Adds networking between `node` and its inner, assuming `node` is a Tee.
/// If its inner already has networking to the new location, doesn't add
/// networking again; instead, reuses the existing network and tees after it.
///
/// - `node`: Must be a Tee
fn add_tee(
    node: &mut HydroNode,
    new_location: &LocationId,
    inner_id: usize,
    inner_location: &LocationId,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
) {
    assert!(
        matches!(node, HydroNode::Tee { .. }),
        "add_tee called on non-tee node: {}",
        node.print_root()
    );

    let metadata = node.metadata().clone();
    let new_inner = new_inners
        .entry((inner_id, new_location.clone()))
        .or_insert_with(|| {
            println!(
                "Adding network before Tee to location {:?} after id: {}",
                new_location, inner_id
            );
            add_network(node, inner_location, new_location);
            let node_content = std::mem::replace(node, HydroNode::Placeholder);
            Rc::new(RefCell::new(node_content))
        })
        .clone();

    let teed_node = HydroNode::Tee {
        inner: TeeNode(new_inner),
        metadata,
    };
    *node = teed_node;
}

/// Places nodes based on location specified in `decision` and adds networking:
/// 1. If the node is a source, change its location.
/// 2. Otherwise, if the node's location differs from its input's location, then add a
///    network after the node.
///    2.1. If the node is a Tee, the input is the inner, and the network is inserted AFTER the
///    inner. The effect is the same as placing the network after the Tee. The network is only
///    created once per inner, if multiple Tees are sent to the same destination.
///
/// Note: The location of a node is where its OUTPUT goes; it is NOT where the node is executed.
#[allow(clippy::too_many_arguments)]
fn decouple_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    decision: &DecoupleDecision,
    location_map: &HashMap<usize, LocationId>,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
    tee_to_inner_id_before_rewrites: &HashMap<usize, usize>,
    op_id_to_inputs_before_rewrites: &HashMap<usize, Vec<usize>>,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) {
    // Ignore unaffected nodes
    let Some(output_location_idx) = decision.get(op_id) else {
        return;
    };
    let output_location = location_map.get(output_location_idx).unwrap();

    // If this node is not a special case, then get the location_idx of one of its inputs
    let input_location_idx = match node {
        HydroNode::Placeholder
        | HydroNode::Counter { .. } => {
            std::panic!("Decoupling placeholder/counter: {}.", op_id);
        }
        // Replace location of sources
        HydroNode::Source { metadata, .. }
        | HydroNode::SingletonSource { metadata, .. }
        | HydroNode::Network { metadata, .. } => {
            println!(
                "Placing source/network on location {:?}, id: {}",
                output_location, op_id
            );
            metadata.location_id.swap_root(output_location.clone());
            return;
        }
        HydroNode::Tee { .. } => {
            // If this Tee is assigned a location, its inner must have one assigned as
            // well
            let inner_id = tee_to_inner_id_before_rewrites.get(op_id).unwrap();
            let inner_location_idx = decision.get(inner_id).unwrap();

            if inner_location_idx == output_location_idx {
                // No need to add a network if the inner is going to the same location
                return;
            }

            let inner_location = location_map.get(inner_location_idx).unwrap();
            println!(
                "Creating a TEE to location {:?}, id: {}",
                inner_location, op_id
            );
            add_tee(node, output_location, *inner_id, inner_location, new_inners);
            return;
        }
        HydroNode::CycleSource { .. } => {
            // CycleSource doesn't have inputs; check the CycleSink's input.
            let sink_input = cycle_source_to_sink_input.get(op_id).unwrap();
            decision.get(sink_input).unwrap()
        }
        _ => {
            let input_ids = op_id_to_inputs_before_rewrites.get(op_id).unwrap_or_else(|| {
                panic!(
                    "Input op ids of node id {} not found: {}",
                    op_id,
                    node.print_root()
                )
            });
            assert!(
                !input_ids.is_empty(),
                "Node with no inputs assigned location: {}, {}",
                op_id,
                node.print_root()
            );
            // Verify that all inputs have the same output location
            let input_locations_idx = input_ids 
                .iter()
                .map(|input_id| {
                    decision.get(input_id).unwrap_or_else(|| {
                        panic!(
                            "Input id {} of node id {} not found in decision",
                            input_id, op_id
                        )
                    })
                })
                .collect::<HashSet<_>>();
            assert!(
                input_locations_idx.len() == 1,
                "Node with multiple input locations: {}, {}, input location indices: {:?}",
                op_id,
                node.print_root(),
                input_locations_idx
            );
            input_locations_idx.into_iter().next().unwrap()
        }
    };

    if input_location_idx == output_location_idx {
        // No need to add a network if the input is coming from the same location
        return;
    }

    println!(
        "Creating network to location {:?} after id: {}",
        output_location, op_id
    );
    let input_location = location_map.get(input_location_idx).unwrap();
    add_network(node, input_location, output_location);
}

fn fix_cluster_self_id_root(root: &mut HydroRoot, mut locations: ClusterSelfIdReplace) {
    if let ClusterSelfIdReplace::Decouple {
        decoupled_cluster_id,
        ..
    } = locations
        && root.input_metadata().location_id.root().key() == decoupled_cluster_id
    {
        root.visit_debug_expr(|expr| {
            locations.visit_expr_mut(&mut expr.0);
        });
    }
}

fn fix_cluster_self_id_node(node: &mut HydroNode, mut locations: ClusterSelfIdReplace) {
    if let ClusterSelfIdReplace::Decouple {
        decoupled_cluster_id,
        ..
    } = locations
        && node.metadata().location_id.root().key() == decoupled_cluster_id
    {
        node.visit_debug_expr(|expr| {
            locations.visit_expr_mut(&mut expr.0);
        });
    }
}

/// Apply the decoupling decision to the IR.
/// Creates new cluster locations as needed via the FlowBuilder and returns them.
pub fn decouple<'a>(
    ir: &mut [HydroRoot],
    decision: DecoupleDecision,
    orig_location: &LocationId,
    builder: &mut FlowBuilder<'a>,
) -> Vec<Cluster<'a, ()>> {
    // Preprocess: count the number of unique locations.
    // If there is only 1, no decoupling is necessary.
    let unique_locations = decision.values().collect::<HashSet<_>>();
    if unique_locations.len() <= 1 {
        return vec![];
    }

    // Create clusters for each new location index (1..last)
    let mut new_clusters = Vec::new();
    let mut locations_map = HashMap::new();
    for location_idx in unique_locations {
        // Special case for id 0. This is the original location
        if *location_idx == 0 {
            locations_map.insert(0, orig_location.clone());
        } else {
            // Otherwise create a new cluster for this location index
            let cluster = builder.cluster::<()>();
            locations_map.insert(*location_idx, cluster.id().clone());
            new_clusters.push(cluster);
        }
    }

    let cycles = cycle_source_to_sink_input(ir);
    let tee_to_inner_id_before_rewrites = tee_to_inner_id(ir);
    let op_id_to_input_before_rewrites = op_id_to_inputs(ir, None, &cycles);
    let mut new_inners = HashMap::new();
    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, op_id| {
            decouple_node(
                node,
                op_id,
                &decision,
                &locations_map,
                &mut new_inners,
                &tee_to_inner_id_before_rewrites,
                &op_id_to_input_before_rewrites,
                &cycles,
            );
        },
    );

    // Fix locations since we changed some
    inject_id(ir);
    // print_id(ir);
    let cycles = cycle_source_to_sink_input(ir);
    inject_location(ir, &cycles);

    // Fix CLUSTER_SELF_ID for each new location
    for cluster in &new_clusters {
        let locations = ClusterSelfIdReplace::Decouple {
            orig_cluster_id: orig_location.key(),
            decoupled_cluster_id: cluster.id().key(),
        };
        transform_bottom_up(
            ir,
            &mut |leaf| {
                fix_cluster_self_id_root(leaf, locations);
            },
            &mut |node| {
                fix_cluster_self_id_node(node, locations);
            },
            true,
        );
    }

    new_clusters
}
