use std::collections::HashMap;
use std::collections::HashSet;

use ena::unify::{InPlace, UnificationTable, UnifyKey};
use hydro_lang::{
    compile::{
        builder::ClockId,
        ir::{HydroNode, HydroRoot, traverse_dfir},
    },
    location::dynamic::LocationId,
};

use crate::rewrites::{can_decouple, op_id_to_parents};
use crate::{decouple_analysis::PossibleRewrite, repair::cycle_source_to_sink_input};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnitKey(u32);

impl UnifyKey for UnitKey {
    type Value = ();
    fn index(&self) -> u32 {
        self.0
    }
    fn from_index(u: u32) -> Self {
        UnitKey(u)
    }
    fn tag() -> &'static str {
        "UnitKey"
    }
}

type UnionFind = UnificationTable<InPlace<UnitKey>>;

struct GreedyDecoupleState {
    excluded_locations: HashSet<LocationId>,
    op_to_key: HashMap<usize, UnitKey>,
    key_to_loc: UnionFind,
    tick_to_ops: HashMap<ClockId, HashSet<usize>>,
    do_not_decouple: HashSet<usize>,
    /// op_ids of pre-existing network nodes
    network_ids: HashSet<usize>,
}

impl GreedyDecoupleState {
    fn new(excluded_locations: HashSet<LocationId>) -> Self {
        Self {
            excluded_locations,
            op_to_key: HashMap::new(),
            key_to_loc: UnificationTable::<InPlace<UnitKey>>::default(),
            tick_to_ops: HashMap::new(),
            do_not_decouple: HashSet::new(),
            network_ids: HashSet::new(),
        }
    }

    /// Assigns a union-find key to each node at a non-excluded location.
    /// Also populates `tick_to_ops`, `do_not_decouple`, and `network_ids`.
    fn assign_location_node(&mut self, node: &mut HydroNode, op_id: &mut usize) {
        let location_id = node.metadata().location_id.clone();

        // Ignore nodes at excluded locations
        if self
            .excluded_locations
            .iter()
            .any(|loc| location_id.root() == loc.root())
        {
            return;
        }

        // Track pre-existing network nodes
        if matches!(node, HydroNode::Network { .. }) {
            self.network_ids.insert(*op_id);
        }

        let tick_id = match &location_id {
            LocationId::Tick(tick_id, _) => Some(*tick_id),
            LocationId::Atomic(tick) => match tick.as_ref() {
                LocationId::Tick(tick_id, _) => Some(*tick_id),
                _ => panic!("Expected tick location for atomic node"),
            },
            _ => None,
        };

        if let Some(tick_id) = tick_id {
            self.tick_to_ops.entry(tick_id).or_default().insert(*op_id);
        }

        if !can_decouple(&node.metadata().collection_kind) {
            self.do_not_decouple.insert(*op_id);
        }

        let key = self.key_to_loc.new_key(());
        self.op_to_key.insert(*op_id, key);

        // Partition must share its inner's location because of how emit_core generates DFIR
        if let HydroNode::Partition { inner, .. } = node {
            let inner_id = inner.0.borrow().metadata().op.id.unwrap();
            if let Some(&inner_key) = self.op_to_key.get(&inner_id) {
                self.key_to_loc.union(key, inner_key);
            }
        }
    }
}

/// Decouples as much as possible; only leaving ticked regions un-decoupled.
/// Applies to all locations except those in `excluded_locations`.
pub fn greedy_decouple_analysis(
    ir: &mut [HydroRoot],
    excluded_locations: &HashSet<LocationId>,
) -> PossibleRewrite {
    let mut state = GreedyDecoupleState::new(excluded_locations.clone());

    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            state.assign_location_node(node, op_id);
        },
    );

    // Constrain tick
    let cycles = cycle_source_to_sink_input(ir);
    let op_id_to_input = op_id_to_parents(ir, None, &cycles);
    let mut tick_to_op_inputs = HashMap::new();
    for (tick_id, ops) in state.tick_to_ops {
        for op_id in ops {
            if let Some(inputs) = op_id_to_input.get(&op_id) {
                tick_to_op_inputs
                    .entry(tick_id)
                    .or_insert_with(HashSet::new)
                    .extend(inputs);
            }
        }
    }
    for (_tick_id, op_inputs) in tick_to_op_inputs {
        // Pairwise union
        let op_inputs_vec: Vec<usize> = op_inputs.into_iter().collect();
        for i in 0..op_inputs_vec.len() - 1 {
            let input1_key = state
                .op_to_key
                .get(&op_inputs_vec[i])
                .expect("Input should have a key");
            let input2_key = state
                .op_to_key
                .get(&op_inputs_vec[i + 1])
                .expect("Input should have a key");
            state.key_to_loc.union(*input1_key, *input2_key);
        }
    }

    // Constrain inputs
    for (_op_id, inputs) in op_id_to_input.iter() {
        assert!(
            inputs.len() <= 2,
            "Did not expect op with more than 2 inputs"
        );
        if inputs.len() != 2 {
            continue;
        }

        let input1_key = state
            .op_to_key
            .get(&inputs[0])
            .expect("Input should have a key");
        let input2_key = state
            .op_to_key
            .get(&inputs[1])
            .expect("Input should have a key");
        state.key_to_loc.union(*input1_key, *input2_key);
    }

    // Do not decouple constraints
    for op_id in state.do_not_decouple {
        let op_key = state.op_to_key.get(&op_id).expect("Op should have a key");
        let inputs = op_id_to_input
            .get(&op_id)
            .expect("Op's parent is not in the same location, must be network. But Network must be serializable and unbounded?");
        for input in inputs {
            let input_key = state
                .op_to_key
                .get(input)
                .expect("Input should have a key");
            state.key_to_loc.union(*op_key, *input_key);
        }
    }

    let mut op_to_loc = HashMap::new();
    for (op_id, key) in &state.op_to_key {
        op_to_loc.insert(*op_id, state.key_to_loc.find(*key).0 as usize);
    }

    // Build PossibleRewrite: separate placement from network insertion
    let op_to_parents = op_id_to_parents(ir, None, &cycles);
    let mut result = PossibleRewrite::default();
    for (&op_id, &loc) in &op_to_loc {
        let parent_loc = op_to_parents
            .get(&op_id)
            .and_then(|parents| parents.first()) // Suffices to check 1 parent since both will have the same location
            .and_then(|p| op_to_loc.get(p))
            .copied();

        let is_network = state.network_ids.contains(&op_id);

        // Only insert new networks for non-network ops at location boundaries
        if !is_network
            && let Some(parent_loc) = parent_loc
            && parent_loc != loc
        {
            result.add_network(parent_loc, loc, op_id);
        }

        result.op_to_loc.insert(op_id, loc);
    }

    println!(
        "Greedy decouple result: {} placements, {} new network edges",
        result.op_to_loc.len(),
        result.op_to_network.len()
    );
    result
}
