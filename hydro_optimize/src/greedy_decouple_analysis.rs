use std::collections::HashMap;

use hydro_lang::{
    compile::ir::{HydroNode, HydroRoot, traverse_dfir},
    deploy::HydroDeploy,
    location::dynamic::LocationId,
};

use crate::decoupler::DecoupleDecision;

fn assign_location_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    next_loc: &mut usize,
    decisions: &mut DecoupleDecision,
    tick_loc: &mut HashMap<usize, usize>,
    node_to_decouple: &LocationId,
) {
    match node {
        // Ignore non-operational nodes
        HydroNode::Placeholder
        | HydroNode::Cast { .. }
        | HydroNode::ObserveNonDet { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Counter { .. } => return,
        _ => {}
    }

    let location_id = &node.metadata().location_id;

    // Ignore nodes that we aren't decoupling
    if location_id.root() != node_to_decouple.root() {
        return;
    }

    // Add a new location unless it's part of a tick that already has an assigned location
    let location = match location_id {
        LocationId::Tick(tick_id, _) => *tick_loc.entry(*tick_id).or_insert_with(|| {
            let loc = *next_loc;
            *next_loc += 1;
            loc
        }),
        _ => {
            let loc = *next_loc;
            *next_loc += 1;
            loc
        }
    };

    decisions.insert(*op_id, location);
}

/// Decouples as much as possible; only leaving ticked regions un-decoupled.
pub fn greedy_decouple_analysis(
    ir: &mut [HydroRoot],
    node_to_decouple: &LocationId,
) -> DecoupleDecision {
    let mut decisions = DecoupleDecision::default();
    let mut tick_loc = HashMap::default();
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
                &mut tick_loc,
                node_to_decouple,
            );
        },
    );

    decisions
}
