use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use hydro_lang::compile::ir::{
    DebugInstantiate, HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, TeeNode,
    transform_bottom_up, traverse_dfir,
};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::location::dynamic::LocationId;
use proc_macro2::Span;
use serde::{Deserialize, Serialize};
use stageleft::quote_type;
use syn::visit_mut::VisitMut;

use crate::repair::{cycle_source_to_sink_input, inject_location};
use crate::rewrites::{
    ClusterSelfIdReplace, collection_kind_to_debug_type, deserialize_bincode_with_type,
    prepend_member_id_to_collection_kind, serialize_bincode_with_type, tee_to_inner_id,
};

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct DecoupleDecision {
    pub output_to_decoupled_machine_after: Vec<usize>, /* The output of the operator at this index should be sent to the decoupled machine */
    pub output_to_original_machine_after: Vec<usize>, /* The output of the operator at this index should be sent to the original machine */
    pub place_on_decoupled_machine: Vec<usize>, /* This operator should be placed on the decoupled machine. Only for sources */
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Decoupler {
    pub decision: DecoupleDecision,
    pub orig_location: LocationId,
    pub decoupled_location: LocationId,
}

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

fn decouple_node(
    node: &mut HydroNode,
    decoupler: &Decoupler,
    next_stmt_id: &mut usize,
    new_inners: &mut HashMap<(usize, LocationId), Rc<RefCell<HydroNode>>>,
    tee_to_inner_id_before_rewrites: &HashMap<usize, usize>,
) {
    // Replace location of sources, if necessary
    if decoupler
        .decision
        .place_on_decoupled_machine
        .contains(next_stmt_id)
    {
        match node {
            HydroNode::Source { metadata, .. }
            | HydroNode::SingletonSource { metadata, .. }
            | HydroNode::Network { metadata, .. } => {
                println!(
                    "Changing source/network destination from {:?} to location {:?}, id: {}",
                    metadata.location_id,
                    decoupler.decoupled_location.clone(),
                    next_stmt_id
                );
                metadata
                    .location_id
                    .swap_root(decoupler.decoupled_location.clone());
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

    // Otherwise, replace where the outputs go
    let new_location = if decoupler
        .decision
        .output_to_decoupled_machine_after
        .contains(next_stmt_id)
    {
        &decoupler.decoupled_location
    } else if decoupler
        .decision
        .output_to_original_machine_after
        .contains(next_stmt_id)
    {
        &decoupler.orig_location
    } else {
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
                new_location, next_stmt_id
            );
            add_tee(
                node,
                new_location,
                new_inners,
                tee_to_inner_id_before_rewrites,
            );
        }
        _ => {
            println!(
                "Creating network to location {:?} after node {}, id: {}",
                new_location,
                node.print_root(),
                next_stmt_id
            );
            add_network(node, new_location);
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

pub fn decouple(ir: &mut [HydroRoot], decoupler: &Decoupler) {
    let tee_to_inner_id_before_rewrites = tee_to_inner_id(ir);
    let mut new_inners = HashMap::new();
    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, next_stmt_id| {
            decouple_node(
                node,
                decoupler,
                next_stmt_id,
                &mut new_inners,
                &tee_to_inner_id_before_rewrites,
            );
        },
    );

    // Fix locations since we changed some
    let cycle_source_to_sink_input = cycle_source_to_sink_input(ir);
    inject_location(ir, &cycle_source_to_sink_input);
    // Fix CLUSTER_SELF_ID for the decoupled node
    let locations = ClusterSelfIdReplace::Decouple {
        orig_cluster_id: decoupler.orig_location.key(),
        decoupled_cluster_id: decoupler.decoupled_location.key(),
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
