use std::cell::RefCell;
use std::collections::HashMap;

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::{
    HydroIrOpMetadata, HydroNode, HydroRoot, transform_bottom_up, traverse_dfir,
};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::location::dynamic::LocationId;

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

    traverse_dfir::<HydroDeploy>(
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

fn link_cycles_root(root: &mut HydroRoot, sink_inputs: &mut HashMap<CycleId, usize>) {
    if let HydroRoot::CycleSink {
        cycle_id, input, ..
    } = root
    {
        sink_inputs.insert(*cycle_id, input.op_metadata().id.unwrap());
        println!(
            cycle_id.clone(),
            input.op_metadata().id.unwrap()
        );
    }
}

fn link_cycles_node(node: &mut HydroNode, sources: &mut HashMap<CycleId, usize>) {
    if let HydroNode::CycleSource {
        cycle_id, metadata, ..
    } = node
    {
        sources.insert(*cycle_id, metadata.op.id.unwrap());
    }
}

// Returns map from CycleSource id to the input IDs of the corresponding CycleSink's input
// Assumes that metadtata.id is set for all nodes
pub fn cycle_source_to_sink_input(ir: &mut [HydroRoot]) -> HashMap<usize, usize> {
    let mut sources = HashMap::new();
    let mut sink_inputs = HashMap::new();

    // Can't use traverse_dfir since that skips CycleSink
    transform_bottom_up(
        ir,
        &mut |leaf| {
            link_cycles_root(leaf, &mut sink_inputs);
        },
        &mut |node| {
            link_cycles_node(node, &mut sources);
        },
        false,
    );

    let mut source_to_sink_input = HashMap::new();
    for (sink_ident, sink_input_id) in sink_inputs {
        if let Some(source_id) = sources.get(&sink_ident) {
            source_to_sink_input.insert(*source_id, sink_input_id);
        } else {
            std::panic!(
                "No source found for CycleSink {}, Input Id {}",
                sink_ident,
                sink_input_id
            );
        }
    }
    println!("Source to sink input: {:?}", source_to_sink_input);
    source_to_sink_input
}

// Returns whether location was missing for any node and requires another round of calculation (to reach fixpoint)
fn inject_location_node(
    node: &mut HydroNode,
    id_to_location: &mut HashMap<usize, LocationId>,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> bool {
    if let Some(op_id) = node.op_metadata().id {
        let inputs = match node {
            HydroNode::Source { metadata, .. }
            | HydroNode::SingletonSource { metadata, .. }
            | HydroNode::ExternalInput { metadata, .. }
            | HydroNode::Network { metadata, .. } => {
                // Get location sources from the nodes must have it be correct: Source and Network
                id_to_location.insert(op_id, metadata.location_id.clone());
                return false;
            }
            HydroNode::Tee { inner, .. } => {
                vec![inner.0.borrow().op_metadata().id.unwrap()]
            }
            HydroNode::CycleSource { .. } => {
                vec![*cycle_source_to_sink_input.get(&op_id).unwrap()]
            }
            _ => node
                .input_metadata()
                .iter()
                .map(|input_metadata| input_metadata.op.id.unwrap())
                .collect(),
        };

        // Otherwise, get it from (either) input
        let metadata = node.metadata_mut();
        for input in inputs {
            let location = id_to_location.get(&input).cloned();
            if let Some(location) = location {
                metadata.location_id.swap_root(location.root().clone());
                id_to_location.insert(op_id, metadata.location_id.clone());
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

pub fn inject_location(ir: &mut [HydroRoot], cycle_source_to_sink_input: &HashMap<usize, usize>) {
    let mut id_to_location = HashMap::new();

    let mut prev_missing_locations = None;
    let mut missing_locations = 0;
    // Continue
    while prev_missing_locations.is_none_or(|prev| prev != missing_locations) {
        println!("Attempting to inject location, looping until fixpoint...");
        prev_missing_locations = Some(missing_locations);
        missing_locations = 0;

        transform_bottom_up(
            ir,
            &mut |_| {},
            &mut |node| {
                if inject_location_node(node, &mut id_to_location, cycle_source_to_sink_input) {
                    missing_locations += 1;
                }
            },
            false,
        );
    }

    println!("Locations injected!");

    // Check well-formedness here
    transform_bottom_up(ir, &mut |_| {}, &mut |_| {}, true);
}

fn remove_counter_node(node: &mut HydroNode, _next_stmt_id: &mut usize) {
    if let HydroNode::Counter { input, .. } = node {
        *node = std::mem::replace(input, HydroNode::Placeholder);
    }
}

pub fn remove_counter(ir: &mut [HydroRoot]) {
    traverse_dfir::<HydroDeploy>(ir, |_, _| {}, remove_counter_node);
}
