use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

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

use crate::repair::{cycle_source_to_sink_input, inject_location};
use crate::rewrites::{
    ClusterSelfIdReplace, collection_kind_to_debug_type, deserialize_bincode_with_type,
    op_id_to_inputs, prepend_member_id_to_collection_kind, serialize_bincode_with_type,
    tee_to_inner_id,
};

/// Each index represents a location. Index 0 is always the original location.
/// Higher indices are new locations created by the decoupler.
/// Each HashSet contains the op_ids assigned to that location.
pub type DecoupleDecision = Vec<HashSet<usize>>;

fn add_network(node: &mut HydroNode, new_location: &LocationId) {
    let metadata = node.metadata().clone();
    let parent_id = metadata.location_id.root().key();
    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    // Map from b to (MemberId, b), where MemberId is the id of the decoupled (or original) node we're sending to
    let ident = syn::Ident::new(
        &format!("__hydro_lang_cluster_self_id_{}", parent_id),
        Span::call_site(),
    );
    let f: syn::Expr = syn::parse_quote!(|b| (
        ::hydro_lang::location::MemberId::<()>::from_tagless(#ident.clone()),
        b
    ));

    // Calculate the new CollectionKind
    let original_collection_kind = metadata.collection_kind.clone();
    let new_collection_kind = prepend_member_id_to_collection_kind(&original_collection_kind);

    let mapped_node = HydroNode::Map {
        f: f.into(),
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            location_id: metadata.location_id.root().clone(), // Remove any ticks
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
            location_id: new_location.clone(),
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

    // Map again to remove the member Id (mimicking send_anonymous)
    let f: syn::Expr = syn::parse_quote!(|(_, b)| b);
    let mapped_node = HydroNode::Map {
        f: f.into(),
        input: Box::new(network_node),
        metadata: HydroIrMetadata {
            location_id: new_location.clone(),
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

fn add_tee(
    node: &mut HydroNode,
    new_location: &LocationId,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
    tee_to_inner_id_before_rewrites: &HashMap<usize, usize>,
) {
    let metadata = node.metadata().clone();
    let inner_id = tee_to_inner_id_before_rewrites
        .get(&metadata.op.id.unwrap())
        .unwrap();

    let new_inner = new_inners
        .entry((*inner_id, new_location.clone()))
        .or_insert_with(|| {
            println!(
                "Adding network before Tee to location {:?} after id: {}",
                new_location, inner_id
            );
            add_network(node, new_location);
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

/// Determine the location index for an op. Returns 0 (original) if not in the decision.
/// Find which location index an op is assigned to. Returns 0 (original) if not found.
fn op_location(decision: &DecoupleDecision, op_id: usize) -> usize {
    decision
        .iter()
        .position(|ops| ops.contains(&op_id))
        .unwrap_or(0)
}

/// Derive the network insertion and source placement actions from the decision
/// by comparing each op's location to its inputs' locations.
struct NetworkDecisions {
    /// op_id -> target LocationId: insert network sending output of this op to target
    output_to: HashMap<usize, LocationId>,
    /// op_ids whose source/network nodes should be relocated
    place_on: HashMap<usize, LocationId>,
}

fn calc_network_decisions(
    decision: &DecoupleDecision,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    locations: &[LocationId],
) -> NetworkDecisions {
    let mut output_to = HashMap::new();
    let mut place_on = HashMap::new();

    for (loc_idx, ops) in decision.iter().enumerate() {
        for op_id in ops {
            if let Some(inputs) = op_id_to_inputs.get(op_id) {
                if let Some(first_input) = inputs.first() {
                    let input_loc_idx = op_location(decision, *first_input);
                    if input_loc_idx != loc_idx {
                        // The input is on a different location than this op,
                        // so we need to insert a network after the input sending to this op's location
                        output_to.insert(*op_id, locations[loc_idx].clone());
                    }
                } else {
                    // No inputs (source node) — if assigned to non-original location, place it there
                    if loc_idx != 0 {
                        place_on.insert(*op_id, locations[loc_idx].clone());
                    }
                }
            } else {
                // Not in op_id_to_inputs — treat as source
                if loc_idx != 0 {
                    place_on.insert(*op_id, locations[loc_idx].clone());
                }
            }
        }
    }

    NetworkDecisions {
        output_to,
        place_on,
    }
}

fn decouple_node(
    node: &mut HydroNode,
    network_decisions: &NetworkDecisions,
    next_stmt_id: &mut usize,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
    tee_to_inner_id_before_rewrites: &HashMap<usize, usize>,
) {
    // Replace location of sources, if necessary
    if let Some(target_location) = network_decisions.place_on.get(next_stmt_id) {
        match node {
            HydroNode::Source { metadata, .. }
            | HydroNode::SingletonSource { metadata, .. }
            | HydroNode::Network { metadata, .. } => {
                println!(
                    "Changing source/network destination from {:?} to location {:?}, id: {}",
                    metadata.location_id, target_location, next_stmt_id
                );
                metadata.location_id.swap_root(target_location.clone());
            }
            _ => {
                std::panic!(
                    "Decoupler placing non-source/network node on decoupled machine: {}",
                    node.print_root()
                );
            }
        }
        return;
    }

    // Otherwise, check if we need to insert a network
    let Some(target_location) = network_decisions.output_to.get(next_stmt_id) else {
        return;
    };

    match node {
        HydroNode::Placeholder | HydroNode::Network { .. } => {
            std::panic!(
                "Decoupler modifying placeholder node or incorrectly handling network node: {}",
                next_stmt_id
            );
        }
        HydroNode::Tee { .. } => {
            println!(
                "Creating a TEE to location {:?}, id: {}",
                target_location, next_stmt_id
            );
            add_tee(
                node,
                target_location,
                new_inners,
                tee_to_inner_id_before_rewrites,
            );
        }
        _ => {
            println!(
                "Creating network to location {:?} after node {}, id: {}",
                target_location,
                node.print_root(),
                next_stmt_id
            );
            add_network(node, target_location);
        }
    }
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
    mut decision: DecoupleDecision,
    orig_location: &LocationId,
    builder: &mut FlowBuilder<'a>,
) -> Vec<Cluster<'a, ()>> {
    // Preprocess: remove empty location sets and check if there's anything to decouple
    decision.retain(|ops| !ops.is_empty());
    let num_locations = decision.len();
    if num_locations <= 1 {
        return vec![];
    }

    // Create clusters for each new location index (1..last)
    let mut new_clusters = Vec::new();
    let mut locations: Vec<LocationId> = vec![orig_location.clone()];
    for _ in 1..num_locations {
        let cluster = builder.cluster::<()>();
        locations.push(cluster.id().clone());
        new_clusters.push(cluster);
    }

    // Derive actions from the decision by comparing op locations to input locations
    let cycle_map = cycle_source_to_sink_input(ir);
    let all_op_id_to_inputs = op_id_to_inputs(ir, Some(&orig_location.key()), &cycle_map);
    let actions = calc_network_decisions(&decision, &all_op_id_to_inputs, &locations);

    let tee_to_inner_id_before_rewrites = tee_to_inner_id(ir);
    let mut new_inners = HashMap::new();
    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            decouple_node(
                node,
                &actions,
                next_stmt_id,
                &mut new_inners,
                &tee_to_inner_id_before_rewrites,
            );
        },
    );

    // Fix locations since we changed some
    let cycle_source_to_sink_input_post = cycle_source_to_sink_input(ir);
    inject_location(ir, &cycle_source_to_sink_input_post);

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
