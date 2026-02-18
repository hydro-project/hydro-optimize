use std::collections::HashMap;

use hydro_lang::{
    compile::ir::{HydroNode, HydroRoot, traverse_dfir},
    deploy::HydroDeploy,
    location::dynamic::LocationId,
};

use crate::{
    decoupler::DecoupleDecision, repair::cycle_source_to_sink_input, rewrites::op_id_to_inputs,
};

/// For each `node` executing on `node_to_decouple` (its input's location is `node_to_decouple`), assign it a new location.
/// Also populate `op_to_tick`.
///
/// Concretely, if the node's parent's location is `node_to_decouple`, then the node must be executing on `node_to_decouple`.
/// Respect the following invariant: If the node is part of a tick, then all nodes in the tick must be assigned to the same location.
/// - `next_loc`: the next int that represents an unused location.
/// - `op_to_tick`: map from op_id to tick, if it is in one
fn assign_location_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    next_loc: &mut usize,
    decisions: &mut DecoupleDecision,
    op_to_tick: &mut HashMap<usize, usize>,
    node_to_decouple: &LocationId,
) {
    let location_id = &node.metadata().location_id;

    // Ignore nodes that we aren't decoupling
    if location_id.root() != node_to_decouple.root() {
        return;
    }

    if let LocationId::Tick(tick_id, _) = location_id {
        op_to_tick.insert(*op_id, *tick_id);
    }

    let new_loc = *next_loc;
    *next_loc += 1;

    decisions.insert(*op_id, new_loc);
}

/// Decouples as much as possible; only leaving ticked regions un-decoupled.
pub fn greedy_decouple_analysis(
    ir: &mut [HydroRoot],
    node_to_decouple: &LocationId,
) -> DecoupleDecision {
    let mut decisions = DecoupleDecision::default();
    let mut op_to_tick = HashMap::new();
    let mut next_loc = 0;

    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, op_id| {
            assign_location_node(
                node,
                op_id,
                &mut next_loc,
                &mut decisions,
                &mut op_to_tick,
                node_to_decouple,
            );
        },
    );

    // Constrain tick
    let cycles = cycle_source_to_sink_input(ir);
    let op_id_to_input = op_id_to_inputs(ir, Some(&node_to_decouple.key()), &cycles);
    let mut tick_loc = HashMap::new();
    for (op_id, tick_id) in op_to_tick {
        if let Some(inputs) = op_id_to_input.get(&op_id) {
            if inputs.is_empty() {
                continue;
            }

            // For all ops of the same tick, their inputs all output to the same location
            let location = tick_loc
                .entry(tick_id)
                .or_insert_with(|| *decisions.get(&inputs[0]).unwrap());
            for input in inputs {
                decisions.insert(*input, *location);
            }
        }
    }

    // Constrain inputs to the same op
    for (_op_id, inputs) in op_id_to_input {
        assert!(
            inputs.len() <= 2,
            "Did not expect op with more than 2 inputs"
        );
        if inputs.is_empty() || inputs.len() != 2 {
            continue;
        }

        let location = decisions.get(&inputs[0]).unwrap();
        decisions.insert(inputs[1], *location);
    }

    decisions
}
