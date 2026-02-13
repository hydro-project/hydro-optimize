use std::{cell::RefCell, collections::HashMap};

use hydro_lang::{
    compile::ir::{HydroNode, HydroRoot, traverse_dfir},
    deploy::HydroDeploy,
    location::dynamic::LocationId,
};

use crate::decoupler::DecoupleDecision;

/// `tick_loc`: Mapping from tick ID to location for all ops with that tikc
/// `next_loc`: Next available location ID. Increment once used.
fn assign_location(
    location_id: &LocationId,
    op_id: &mut usize,
    decisions: &RefCell<DecoupleDecision>,
    tick_loc: &RefCell<HashMap<usize, usize>>,
    node_to_decouple: &LocationId,
) {
    // Ignore nodes that we aren't decoupling
    if location_id.root() != node_to_decouple.root() {
        return;
    }
    let mut decisions = decisions.borrow_mut();
    let mut tick_loc = tick_loc.borrow_mut();

    // Add a new location unless it's part of a tick that already has an assigned location
    let location = match location_id {
        LocationId::Tick(tick_id, _) => {
            *tick_loc.entry(*tick_id).or_insert_with(|| decisions.len())
        }
        _ => decisions.len(),
    };

    if location >= decisions.len() {
        decisions.push(Default::default());
    }

    decisions.get_mut(location).unwrap().insert(*op_id);
}

fn assign_location_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    decisions: &RefCell<DecoupleDecision>,
    tick_loc: &RefCell<HashMap<usize, usize>>,
    node_to_decouple: &LocationId,
) {
    // Ignore networks
    if let HydroNode::Network { .. } = node {
        return;
    }
    assign_location(
        &node.metadata().location_id,
        op_id,
        decisions,
        tick_loc,
        node_to_decouple,
    );
}

fn assign_location_root(
    root: &mut HydroRoot,
    op_id: &mut usize,
    decisions: &RefCell<DecoupleDecision>,
    tick_loc: &RefCell<HashMap<usize, usize>>,
    node_to_decouple: &LocationId,
) {
    assign_location(
        &root.input_metadata().location_id,
        op_id,
        decisions,
        tick_loc,
        node_to_decouple,
    );
}

/// Decouples as much as possible; only leaving ticked regions un-decoupled.
pub(crate) fn greedy_decouple_analysis(
    ir: &mut [HydroRoot],
    node_to_decouple: &LocationId,
) -> DecoupleDecision {
    let decisions = RefCell::new(DecoupleDecision::default());
    let tick_loc = RefCell::new(HashMap::default());

    traverse_dfir::<HydroDeploy>(
        ir,
        |root, op_id| {
            assign_location_root(root, op_id, &decisions, &tick_loc, node_to_decouple);
        },
        |node, op_id| {
            assign_location_node(node, op_id, &decisions, &tick_loc, node_to_decouple);
        },
    );

    decisions.into_inner()
}
