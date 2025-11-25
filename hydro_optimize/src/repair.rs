use std::cell::RefCell;
use std::collections::HashMap;

use hydro_lang::compile::ir::{
    HydroIrOpMetadata, HydroNode, HydroRoot, transform_bottom_up, traverse_dfir,
};
use hydro_lang::location::dynamic::LocationId;
use syn::Ident;

use crate::rewrites::parent_ids;

fn inject_id_metadata(
    metadata: &mut HydroIrOpMetadata,
    new_id: usize,
    new_id_to_old_id: &RefCell<HashMap<usize, usize>>,
) {
    let old_id = metadata.id.replace(new_id);
    if let Some(old_id) = old_id {
        new_id_to_old_id.borrow_mut().insert(new_id, old_id);
    }
}

/// Injects new IDs for nodes based on location. Returns a map from the new IDs to the old IDs, if the old IDs are available.
pub fn inject_id(ir: &mut [HydroRoot]) -> HashMap<usize, usize> {
    let new_id_to_old_id = RefCell::new(HashMap::new());

    traverse_dfir(
        ir,
        |leaf, id| {
            inject_id_metadata(leaf.op_metadata_mut(), *id, &new_id_to_old_id);
        },
        |node, id| {
            inject_id_metadata(node.op_metadata_mut(), *id, &new_id_to_old_id);
        },
    );

    new_id_to_old_id.take()
}

fn link_cycles_root(root: &mut HydroRoot, sink_parents: &mut HashMap<Ident, usize>) {
    if let HydroRoot::CycleSink { ident, input, .. } = root {
        sink_parents.insert(ident.clone(), input.op_metadata().id.unwrap());
    }
}

fn link_cycles_node(node: &mut HydroNode, sources: &mut HashMap<Ident, usize>) {
    if let HydroNode::CycleSource {
        ident, metadata, ..
    } = node
    {
        sources.insert(ident.clone(), metadata.op.id.unwrap());
    }
}

// Returns map from CycleSource id to the parent IDs of the corresponding CycleSink's parent
// Assumes that metadtata.id is set for all nodes
pub fn cycle_source_to_sink_parent(ir: &mut [HydroRoot]) -> HashMap<usize, usize> {
    let mut sources = HashMap::new();
    let mut sink_parents = HashMap::new();

    // Can't use traverse_dfir since that skips CycleSink
    transform_bottom_up(
        ir,
        &mut |leaf| {
            link_cycles_root(leaf, &mut sink_parents);
        },
        &mut |node| {
            link_cycles_node(node, &mut sources);
        },
        false,
    );

    let mut source_to_sink_parent = HashMap::new();
    for (sink_ident, sink_parent_id) in sink_parents {
        if let Some(source_id) = sources.get(&sink_ident) {
            source_to_sink_parent.insert(*source_id, sink_parent_id);
        } else {
            std::panic!(
                "No source found for CycleSink {}, Parent Id {}",
                sink_ident,
                sink_parent_id
            );
        }
    }
    println!("Source to sink parent: {:?}", source_to_sink_parent);
    source_to_sink_parent
}

fn inject_location_persist(persist: &mut Box<HydroNode>, new_location: LocationId) {
    if let HydroNode::Persist {
        metadata: persist_metadata,
        ..
    } = persist.as_mut()
    {
        persist_metadata.location_kind.swap_root(new_location);
    }
}

// Returns whether location was missing for any node and requires another round of calculation (to reach fixpoint)
fn inject_location_node(
    node: &mut HydroNode,
    id_to_location: &mut HashMap<usize, LocationId>,
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
) -> bool {
    if let Some(op_id) = node.op_metadata().id {
        let parents = match node {
            HydroNode::Source { metadata, .. }
            | HydroNode::SingletonSource { metadata, .. }
            | HydroNode::ExternalInput { metadata, .. }
            | HydroNode::Network { metadata, .. } => {
                // Get location sources from the nodes must have it be correct: Source and Network
                id_to_location.insert(op_id, metadata.location_kind.clone());
                return false;
            }
            _ => parent_ids(node, None, cycle_source_to_sink_parent),
        };

        // Otherwise, get it from (either) parent
        let metadata = node.metadata_mut();
        for parent in parents {
            let location = id_to_location.get(&parent).cloned();
            if let Some(location) = location {
                metadata.location_kind.swap_root(location.root().clone());
                id_to_location.insert(op_id, metadata.location_kind.clone());

                match node {
                    // Update Persist's location as well (we won't see it during traversal)
                    HydroNode::CrossProduct { left, right, .. }
                    | HydroNode::Join { left, right, .. } => {
                        inject_location_persist(left, location.root().clone());
                        inject_location_persist(right, location.root().clone());
                    }
                    HydroNode::Difference { pos, neg, .. }
                    | HydroNode::AntiJoin { pos, neg, .. } => {
                        inject_location_persist(pos, location.root().clone());
                        inject_location_persist(neg, location.root().clone());
                    }
                    HydroNode::Fold { input, .. }
                    | HydroNode::FoldKeyed { input, .. }
                    | HydroNode::Reduce { input, .. }
                    | HydroNode::ReduceKeyed { input, .. }
                    | HydroNode::ReduceKeyedWatermark { input, .. }
                    | HydroNode::Scan { input, .. } => {
                        inject_location_persist(input, location.root().clone());
                    }
                    _ => {}
                }
                return false;
            }
        }

        // If the location was not set, let the recursive function know
        println!("Missing location for node: {:?}", node.print_root());
        return true;
    }

    // No op_id, probably can ignore?
    false
}

pub fn inject_location(ir: &mut [HydroRoot], cycle_source_to_sink_parent: &HashMap<usize, usize>) {
    let mut id_to_location = HashMap::new();

    loop {
        println!("Attempting to inject location, looping until fixpoint...");
        let mut missing_location = false;

        transform_bottom_up(
            ir,
            &mut |_| {},
            &mut |node| {
                missing_location |=
                    inject_location_node(node, &mut id_to_location, cycle_source_to_sink_parent);
            },
            false,
        );

        if !missing_location {
            println!("Locations injected!");

            // Check well-formedness here
            transform_bottom_up(ir, &mut |_| {}, &mut |_| {}, true);
            break;
        }
    }
}

fn remove_counter_node(node: &mut HydroNode, _next_stmt_id: &mut usize) {
    if let HydroNode::Counter { input, .. } = node {
        *node = std::mem::replace(input, HydroNode::Placeholder);
    }
}

pub fn remove_counter(ir: &mut [HydroRoot]) {
    traverse_dfir(ir, |_, _| {}, remove_counter_node);
}
