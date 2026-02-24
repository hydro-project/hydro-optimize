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

use crate::rewrites::can_decouple;
use crate::{
    decoupler::DecoupleDecision, repair::cycle_source_to_sink_input, rewrites::op_id_to_inputs,
};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct UnitKey(u32);

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

/// For each `node` executing on `node_to_decouple` (its input's location is `node_to_decouple`), assign it a new location.
/// Also populate `op_to_key`, `key_to_loc`, `tick_to_ops` and `do_not_decouple`.
///
/// Concretely, if the node's parent's location is `node_to_decouple`, then the node must be executing on `node_to_decouple`.
fn assign_location_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    op_to_key: &mut HashMap<usize, UnitKey>,
    key_to_loc: &mut UnionFind,
    tick_to_ops: &mut HashMap<ClockId, HashSet<usize>>,
    do_not_decouple: &mut HashSet<usize>,
    node_to_decouple: &LocationId,
) {
    let location_id = &node.metadata().location_id;

    // Ignore nodes that we aren't decoupling
    if location_id.root() != node_to_decouple.root() {
        return;
    }

    let tick_id = match location_id {
        LocationId::Tick(tick_id, _) => Some(*tick_id),
        LocationId::Atomic(tick) => match tick.as_ref() {
            LocationId::Tick(tick_id, _) => Some(*tick_id),
            _ => panic!("Expected tick location for atomic node"),
        },
        _ => None,
    };

    if let Some(tick_id) = tick_id {
        tick_to_ops.entry(tick_id).or_default().insert(*op_id);
    }

    if !can_decouple(&node.metadata().collection_kind) {
        do_not_decouple.insert(*op_id);
    }

    let key = key_to_loc.new_key(());
    op_to_key.insert(*op_id, key);
}

/// Decouples as much as possible; only leaving ticked regions un-decoupled.
pub fn greedy_decouple_analysis(
    ir: &mut [HydroRoot],
    node_to_decouple: &LocationId,
) -> DecoupleDecision {
    let mut op_to_key = HashMap::new();
    let mut key_to_loc = UnificationTable::<InPlace<UnitKey>>::default();
    let mut tick_to_ops = HashMap::new();
    let mut do_not_decouple = HashSet::new();

    traverse_dfir(ir,
    |_, _| {},
    |node, op_id| {
        assign_location_node(
            node,
            op_id,
            &mut op_to_key,
            &mut key_to_loc,
            &mut tick_to_ops,
            &mut do_not_decouple,
            node_to_decouple,
        );
    },);

    // Constrain tick
    let cycles = cycle_source_to_sink_input(ir);
    let op_id_to_input = op_id_to_inputs(ir, Some(&node_to_decouple.key()), &cycles);
    let mut tick_to_op_inputs = HashMap::new();
    for (tick_id, ops) in tick_to_ops {
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
            let input1_key = op_to_key
                .get(&op_inputs_vec[i])
                .expect("Input should have a key");
            let input2_key = op_to_key
                .get(&op_inputs_vec[i + 1])
                .expect("Input should have a key");
            key_to_loc.union(*input1_key, *input2_key);
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

        let input1_key = op_to_key.get(&inputs[0]).expect("Input should have a key");
        let input2_key = op_to_key.get(&inputs[1]).expect("Input should have a key");
        key_to_loc.union(*input1_key, *input2_key);
    }

    // Do not decouple constraints
    for op_id in do_not_decouple {
        let op_key = op_to_key.get(&op_id).expect("Op should have a key");
        let inputs = op_id_to_input
            .get(&op_id)
            .expect("Op's parent is not in the same location, must be network. But Network must be serializable and unbounded?");
        for input in inputs {
            let input_key = op_to_key.get(input).expect("Input should have a key");
            key_to_loc.union(*op_key, *input_key);
        }
    }

    let mut decision = DecoupleDecision::default();
    for (op_id, key) in op_to_key {
        decision.insert(op_id, key_to_loc.find(key).0 as usize);
    }
    decision
}
