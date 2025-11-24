use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::partition_ilp_analysis::partition_ilp_analysis;
use crate::rewrites::op_id_to_inputs;
use good_lp::solvers::highs::HighsSolution;
use good_lp::{
    Constraint, Expression, ProblemVariables, Solution, SolverModel, Variable, constraint, highs,
    variable, variables,
};
use hydro_lang::compile::ir::{
    HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, traverse_dfir,
};
use hydro_lang::location::dynamic::LocationId;

use super::rewrites::{NetworkType, get_network_type};

/// Each operator is assigned either 0 or 1
/// 0 means that its output will go to the original node, 1 means that it will go to the decoupled node
/// If there are two operators, map() -> filter(), and they are assigned variables 0 and 1, then that means filter's result ends up on a different machine.
/// The original machine still executes filter(), but pays a serializaton cost for the output of filter.
/// The decoupled machine executes the following operator and pays a deserialization cost for the output of filter.
/// Each operator is executed on the machine indicated by its INPUT's variable.
///
/// Constraints:
/// 1. For binary operators, both inputs must be assigned the same var (output to the same location)
/// 2. For Tee, the serialization/deserialization cost is paid only once if multiple branches are assigned different vars. (instead of sending each branch of the Tee over to a different node, we'll send it once and have the receiver Tee)
///
/// HydroNode::Network:
/// 1. If we are the receiver, then create a var, we pay deserialization
/// 2. If we are the sender (and not the receiver), don't create a var (because the destination is another machine and can't change), but we still pay serialization
/// 3. If we are the sender and receiver, then create a var, pay for both
pub(crate) struct DecoupleILPMetadata {
    // Const fields
    pub(crate) bottleneck: LocationId,
    decoupling_send_overhead: f64, /* CPU usage per cardinality to send, assuming all messages serialize/deserialize similarly */
    decoupling_recv_overhead: f64,
    pub(crate) num_locations: usize,
    // Model variables to construct final cost function
    pub(crate) variables: ProblemVariables,
    pub(crate) constraints: Vec<Constraint>,
    pub(crate) cpu_usages: HashMap<usize, Expression>, // location_id: cpu_usage
    pub(crate) op_id_to_var: HashMap<usize, HashMap<usize, Variable>>, // op_id: (location_id: var). Var = 1 for a location if the op is assigned to that location
    prev_op_input_with_tick: HashMap<usize, usize>, // tick_id: last op_id with that tick_id
    tee_inner_to_decoupled_vars: HashMap<usize, HashMap<usize, (Variable, Variable)>>, /* inner_id: (loc: (send var, recv var)) */
    pub(crate) network_ids: HashMap<usize, NetworkType>,
}

// Penalty for decoupling regardless of cardinality (to prevent decoupling low cardinality operators)
const DECOUPLING_PENALTY: f64 = 0.0001;

/// Returns a map from location to a binary var
/// Lazily creates the var
fn var_from_op_id(
    op_id: usize,
    num_locations: usize,
    op_id_to_var: &mut HashMap<usize, HashMap<usize, Variable>>,
    variables: &mut ProblemVariables,
    constraints: &mut Vec<Constraint>,
) -> HashMap<usize, Variable> {
    op_id_to_var
        .entry(op_id)
        .or_insert_with(|| {
            let mut loc_to_var = HashMap::new();
            let mut sum_expr = Expression::default();
            for loc in 0..num_locations {
                let var = variables.add(variable().binary());
                loc_to_var.insert(loc, var);
                sum_expr = sum_expr + var;
            }
            // Constrain sum to 1 (var is assigned to exactly 1 location)
            constraints.push(constraint!(sum_expr == 1));
            loc_to_var
        })
        .clone()
}

fn add_equality_constr(
    ops: &[usize],
    num_locations: usize,
    op_id_to_var: &mut HashMap<usize, HashMap<usize, Variable>>,
    variables: &mut ProblemVariables,
    constraints: &mut Vec<Constraint>,
) {
    if let Some(prev_op) = ops.first() {
        let mut prev_op_vars = var_from_op_id(
            *prev_op,
            num_locations,
            op_id_to_var,
            variables,
            constraints,
        );

        for op in ops.iter().skip(1) {
            let op_vars = var_from_op_id(*op, num_locations, op_id_to_var, variables, constraints);
            // For each locations, the vars between prev_op and op must be equal
            for (loc, prev_op_var) in &prev_op_vars {
                let op_var = op_vars.get(&loc).unwrap();
                constraints.push(constraint!(*prev_op_var == *op_var));
            }

            prev_op_vars = op_vars;
        }
    }
}

// Store the tick that an op is constrained to
fn add_tick_constraint(
    metadata: &HydroIrMetadata,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        variables,
        constraints,
        num_locations,
        op_id_to_var,
        prev_op_input_with_tick,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    if let LocationId::Tick(tick_id, _) = metadata.location_kind {
        // Set each input = to the last input
        let mut inputs = op_id_to_inputs
            .get(&metadata.op.id.unwrap())
            .unwrap()
            .clone();
        if let Some(prev_input) = prev_op_input_with_tick.get(&tick_id) {
            inputs.push(*prev_input);
        }
        add_equality_constr(
            &inputs,
            *num_locations,
            op_id_to_var,
            variables,
            constraints,
        );

        // Set this op's last input as the last op's input that had this tick
        if let Some(last_input) = inputs.last() {
            prev_op_input_with_tick.insert(tick_id, *last_input);
        }
    }
}

fn add_cpu_usage(
    metadata: &HydroIrOpMetadata,
    network_type: &Option<NetworkType>,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        variables,
        constraints,
        num_locations,
        cpu_usages,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.id.unwrap();

    // Calculate total CPU usage on each node (before overheads). Operators are run on the machine that their inputs send to.
    match network_type {
        Some(NetworkType::Send) | Some(NetworkType::SendRecv) | None => {
            if let Some(inputs) = op_id_to_inputs.get(&op_id) {
                // All inputs must be assigned the same var (by constraints above), so it suffices to check one
                if let Some(first_input) = inputs.first() {
                    let input_vars = var_from_op_id(
                        *first_input,
                        *num_locations,
                        op_id_to_var,
                        variables,
                        constraints,
                    );
                    if let Some(cpu_usage) = metadata.cpu_usage {
                        for (loc, input_var) in input_vars {
                            let node_cpu_usage = cpu_usages.get_mut(&loc).unwrap();
                            let node_cpu_usage_temp = std::mem::take(node_cpu_usage);
                            *node_cpu_usage = node_cpu_usage_temp + cpu_usage * input_var;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    // Special case for network receives: their cpu usage (deserialization) is paid by the receiver, aka the machine they send to.
    match network_type {
        Some(NetworkType::Recv) | Some(NetworkType::SendRecv) => {
            let op_vars =
                var_from_op_id(op_id, *num_locations, op_id_to_var, variables, constraints);
            if let Some(recv_cpu_usage) = metadata.network_recv_cpu_usage {
                for (loc, input_var) in op_vars {
                    let node_cpu_usage = cpu_usages.get_mut(&loc).unwrap();
                    let node_cpu_usage_temp = std::mem::take(node_cpu_usage);
                    *node_cpu_usage = node_cpu_usage_temp + recv_cpu_usage * input_var;
                }
            }
        }
        _ => {}
    }
}

/// For each location, return (send_var, recv_var).
/// send_var = 1 if op1 is on that location and op2 is not (the location is sending), 0 otherwise
/// recv_var = 1 if op1 is not on that location and op2 is (the location is receiving), 0 otherwise
fn add_decouple_vars(
    num_locations: usize,
    variables: &mut ProblemVariables,
    constraints: &mut Vec<Constraint>,
    op1: usize,
    op2: usize,
    op_id_to_var: &mut HashMap<usize, HashMap<usize, Variable>>,
) -> HashMap<usize, (Variable, Variable)> {
    let op1_vars = var_from_op_id(op1, num_locations, op_id_to_var, variables, constraints);
    let op2_vars = var_from_op_id(op2, num_locations, op_id_to_var, variables, constraints);
    let mut decouple_vars = HashMap::new();
    for loc in 0..num_locations {
        let send_var = variables.add(variable().binary());
        let recv_var = variables.add(variable().binary());

        let op1_var = op1_vars.get(&loc).unwrap();
        let op2_var = op2_vars.get(&loc).unwrap();

        // 1 if (op1 is at loc, op2 is not), 0 otherwise
        constraints.push(constraint!(send_var >= *op1_var - *op2_var));
        // 1 if (op1 is not at loc, op2 is), 0 otherwise
        constraints.push(constraint!(recv_var >= *op2_var - *op1_var));

        decouple_vars.insert(loc, (send_var, recv_var));
    }
    decouple_vars
}

fn add_decoupling_overhead(
    node: &HydroNode,
    network_type: &Option<NetworkType>,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        decoupling_send_overhead,
        decoupling_recv_overhead,
        variables,
        constraints,
        num_locations,
        cpu_usages,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let metadata = node.metadata();
    let cardinality = match network_type {
        Some(NetworkType::Send) | Some(NetworkType::SendRecv) => node
            .input_metadata()
            .first()
            .unwrap()
            .cardinality
            .unwrap_or_default(),
        _ => metadata.cardinality.unwrap_or_default(),
    };

    let op_id = metadata.op.id.unwrap();
    if let Some(inputs) = op_id_to_inputs.get(&op_id) {
        // All inputs must be assigned the same var (by constraints above), so it suffices to check one
        if let Some(input) = inputs.first() {
            let decouple_vars = add_decouple_vars(
                *num_locations,
                variables,
                constraints,
                *input,
                op_id,
                op_id_to_var,
            );
            for loc in 0..*num_locations {
                let cpu_usage = cpu_usages.get_mut(&loc).unwrap();
                let (send_var, recv_var) = decouple_vars.get(&loc).unwrap();
                let cpu_usage_temp = std::mem::take(cpu_usage);
                *cpu_usage = cpu_usage_temp
                    + (*decoupling_send_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                        * *send_var
                    + (*decoupling_recv_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                        * *recv_var;
            }
        }
    }
}

/// Only penalize decoupling inner from Tees once per unique location.
/// For example, if a Tee has 2 branches, and both send to the same destination, only penalize the send once.
fn add_tee_decoupling_overhead(
    inner_id: usize,
    metadata: &HydroIrMetadata,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        decoupling_send_overhead,
        decoupling_recv_overhead,
        variables,
        constraints,
        num_locations,
        cpu_usages,
        op_id_to_var,
        tee_inner_to_decoupled_vars,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let cardinality = metadata.cardinality.unwrap_or_default();
    let op_id = metadata.op.id.unwrap();

    println!("Tee {} has inner {}", op_id, inner_id);

    // For each location,
    // send_var is 1 if the inner (at that location) sends to any Tee (including this op),
    // recv_var is 1 if the inner (at a different location) sends to any Tee (including this op)
    let any_send_recv_vars = tee_inner_to_decoupled_vars
        .entry(inner_id)
        .or_insert_with(|| {
            let mut loc_to_vars = HashMap::new();
            for loc in 0..*num_locations {
                let send_var = variables.add(variable().binary());
                let recv_var = variables.add(variable().binary());
                let cpu_usage = cpu_usages.get_mut(&loc).unwrap();

                let cpu_usage_temp = std::mem::take(cpu_usage);
                *cpu_usage = cpu_usage_temp
                    + (*decoupling_send_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                        * send_var
                    + (*decoupling_recv_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                        * recv_var;

                loc_to_vars.insert(loc, (send_var, recv_var));
            }
            loc_to_vars
        });

    // Create decouple vars between the inner and this op, but don't penalize
    let decouple_vars = add_decouple_vars(
        *num_locations,
        variables,
        constraints,
        inner_id,
        op_id,
        op_id_to_var,
    );

    for loc in 0..*num_locations {
        let (send_var, recv_var) = decouple_vars.get(&loc).unwrap();
        let (any_send_var, any_recv_var) = any_send_recv_vars.get(&loc).unwrap();

        constraints.push(constraint!(*any_send_var >= *send_var));
        constraints.push(constraint!(*any_recv_var >= *recv_var));
    }
}

fn decouple_analysis_root(
    root: &mut HydroRoot,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    // Ignore nodes that are not in the cluster to decouple
    if decoupling_metadata.borrow().bottleneck != *root.input_metadata().location_kind.root() {
        return;
    }

    add_tick_constraint(root.input_metadata(), op_id_to_inputs, decoupling_metadata);
}

pub(crate) fn node_is_in_bottleneck(
    node: &HydroNode,
    op_id: usize,
    network_type: &Option<NetworkType>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) -> bool {
    if let HydroNode::Network { .. } = node {
        if let Some(network_type) = network_type {
            decoupling_metadata
                .borrow_mut()
                .network_ids
                .insert(op_id, network_type.clone());
            return true;
        } else {
            return false;
        }
    }

    // If it's not a network, then its location should == the bottleneck
    decoupling_metadata.borrow().bottleneck == *node.metadata().location_kind.root()
}

fn decouple_analysis_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let network_type = get_network_type(node, decoupling_metadata.borrow().bottleneck.root().raw_id());
    if !node_is_in_bottleneck(node, *op_id, &network_type, decoupling_metadata) {
        return;
    }

    // Add decoupling overhead. For Tees of the same inner, even if multiple are decoupled, only penalize decoupling once
    if let HydroNode::Tee {
        inner, metadata, ..
    } = node
    {
        add_tee_decoupling_overhead(
            inner.0.borrow().metadata().op.id.unwrap(),
            metadata,
            decoupling_metadata,
        );
    } else {
        add_decoupling_overhead(node, &network_type, op_id_to_inputs, decoupling_metadata);
    }

    add_cpu_usage(
        node.op_metadata(),
        &network_type,
        op_id_to_inputs,
        decoupling_metadata,
    );
    add_tick_constraint(node.metadata(), op_id_to_inputs, decoupling_metadata);
}

fn solve(decoupling_metadata: &RefCell<DecoupleILPMetadata>) -> HighsSolution {
    let DecoupleILPMetadata {
        variables,
        constraints,
        cpu_usages,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    // Need values to be taken instead of &mut
    let mut vars = std::mem::take(variables);
    let mut constrs = std::mem::take(constraints);

    // Which node has the highest CPU usage?
    let highest_cpu = vars.add_variable();
    for cpu_usage in cpu_usages.values() {
        constrs.push(constraint!(highest_cpu >= cpu_usage.clone()));
    }

    // Minimize the CPU usage of that node
    let problem = vars.minimise(highest_cpu);
    let solution = highs(problem).with_all(constrs).solve().unwrap();

    for (loc, cpu_usage) in cpu_usages {
        println!(
            "Projected CPU usage at location {}: {}",
            loc,
            solution.eval(cpu_usage.clone())
        );
    }

    solution
}

fn op_loc(op_vars: &HashMap<usize, Variable>, solution: &HighsSolution) -> usize {
    for (loc, var) in op_vars {
        let value = solution.value(*var).round();
        if value == 1.0 {
            return *loc;
        }
    }
    panic!("No location assigned to op");
}

/// Returns:
/// - `new_networks`: map from (src_loc, dest_loc) to ops before which a Network should be inserted
/// - `place_on_loc`: map from loc to set of op_ids that should be placed on that loc
pub(crate) fn decouple_analysis(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    send_overhead: f64,
    recv_overhead: f64,
    num_locations: usize,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
    consider_partitioning: bool,
) -> (
    HashMap<(usize, usize), HashSet<usize>>,
    HashMap<usize, HashSet<usize>>,
) {
    if num_locations < 2 {
        panic!("Must decouple to at least 2 locations (original location, decoupled location)");
    }

    let decoupling_metadata = RefCell::new(DecoupleILPMetadata {
        bottleneck: bottleneck.clone(),
        decoupling_send_overhead: send_overhead,
        decoupling_recv_overhead: recv_overhead,
        num_locations,
        variables: variables! {},
        constraints: vec![],
        cpu_usages: HashMap::from_iter(
            (0..num_locations).map(|loc_id| (loc_id, Expression::default())),
        ),
        op_id_to_var: HashMap::new(),
        prev_op_input_with_tick: HashMap::new(),
        tee_inner_to_decoupled_vars: HashMap::new(),
        network_ids: HashMap::new(),
    });
    let op_id_to_inputs = op_id_to_inputs(ir, Some(bottleneck), cycle_source_to_sink_input);

    traverse_dfir(
        ir,
        |root, _| {
            decouple_analysis_root(root, &op_id_to_inputs, &decoupling_metadata);
        },
        |node, next_op_id| {
            decouple_analysis_node(node, next_op_id, &op_id_to_inputs, &decoupling_metadata);
        },
    );
    for inputs in op_id_to_inputs.values() {
        let DecoupleILPMetadata {
            op_id_to_var,
            variables,
            constraints,
            ..
        } = &mut *decoupling_metadata.borrow_mut();
        // Add input constraints. All inputs of an op must output to the same machine (be assigned the same var)
        add_equality_constr(inputs, num_locations, op_id_to_var, variables, constraints);
    }

    // Consider partitioning after all variables for decoupling have been created
    if consider_partitioning {
        partition_ilp_analysis(
            ir,
            cycle_source_to_sink_input,
            &decoupling_metadata,
        );
    }

    let solution = solve(&decoupling_metadata);
    let DecoupleILPMetadata {
        op_id_to_var,
        network_ids,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let mut ops_on_loc: HashMap<usize, HashSet<usize>> =
        HashMap::from_iter((0..num_locations).map(|loc_id| (loc_id, HashSet::new())));
    let mut new_networks = HashMap::new(); // (src loc, dest loc): [op_id]
    let mut place_on_loc = HashMap::new();

    for (op_id, inputs) in op_id_to_inputs {
        if let Some(op_vars) = op_id_to_var.get(&op_id) {
            let op_location = op_loc(op_vars, &solution);
            let input_location = if let Some(input) = inputs.first()
                && let Some(input_vars) = op_id_to_var.get(input)
            {
                Some(op_loc(input_vars, &solution))
            } else {
                None
            };

            // Don't insert network if this is Source or already a Network
            let network_type = network_ids.get(&op_id);

            #[expect(clippy::collapsible_else_if, reason = "code symmetry")]
            if network_type.is_none()
                && let Some(input_unwrapped) = input_location
            {
                // Figure out if we should insert Network nodes
                if input_unwrapped != op_location {
                    new_networks
                        .entry((input_unwrapped, op_location))
                        .or_insert(HashSet::new())
                        .insert(op_id);
                }
                // Record where the op is, based on its input's location
                ops_on_loc.get_mut(&input_unwrapped).unwrap().insert(op_id);
            } else {
                // If this is a network node, or there's no input (Source), then the op's location is its own location
                ops_on_loc.get_mut(&op_location).unwrap().insert(op_id);
                // If this is not a network node (Source), or it's a Network Recv/SendRecv node (with an unknown source), then place it on whatever node it is assigned to
                if !network_type.is_some_and(|t| *t == NetworkType::Send) {
                    place_on_loc
                        .entry(op_location)
                        .or_insert(HashSet::new())
                        .insert(op_id);
                }
            }
        }
    }

    for (loc, ops) in ops_on_loc {
        let mut sorted_ops = Vec::from_iter(ops);
        sorted_ops.sort();
        println!("Ops on location {}: {:?}", loc, sorted_ops);
    }

    for ((src_loc, dest_loc), ops) in &new_networks {
        let mut sorted_ops = Vec::from_iter(ops);
        sorted_ops.sort();
        println!(
            "{} outputting to {} after: {:?}",
            src_loc, dest_loc, sorted_ops
        );
    }

    for (loc, ops) in &place_on_loc {
        let mut sorted_ops = Vec::from_iter(ops.iter());
        sorted_ops.sort();
        println!("Placing on location {}: {:?}", loc, sorted_ops);
    }

    (new_networks, place_on_loc)
}
