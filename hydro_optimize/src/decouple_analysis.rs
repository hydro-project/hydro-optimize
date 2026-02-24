use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::decoupler::DecoupleDecision;
use crate::rewrites::{can_decouple, op_id_to_inputs};
use good_lp::solvers::highs::HighsSolution;
use good_lp::{
    Constraint, Expression, ProblemVariables, Solution, SolverModel, Variable, constraint, highs,
    variable, variables,
};
use hydro_lang::compile::builder::ClockId;
use hydro_lang::compile::ir::{
    HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, traverse_dfir,
};
use hydro_lang::deploy::HydroDeploy;
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
struct ModelMetadata {
    // Const fields
    cluster_to_decouple: LocationId,
    decoupling_send_overhead: f64, /* CPU usage per cardinality to send, assuming all messages serialize/deserialize similarly */
    decoupling_recv_overhead: f64,
    // Model variables to construct final cost function
    variables: ProblemVariables,
    constraints: Vec<Constraint>,
    orig_node_cpu_usage: Expression,
    decoupled_node_cpu_usage: Expression,
    op_id_to_var: HashMap<usize, Variable>,
	do_not_decouple: HashSet<usize>,
    prev_op_input_with_tick: HashMap<ClockId, usize>, // tick_id: last op_id with that tick_id
    tee_inner_to_decoupled_vars: HashMap<usize, (Variable, Variable)>, /* inner_id: (orig_to_decoupled, decoupled_to_orig) */
    network_ids: HashMap<usize, NetworkType>,
}

// Penalty for decoupling regardless of cardinality (to prevent decoupling low cardinality operators)
const DECOUPLING_PENALTY: f64 = 0.0001;

// Lazily creates the var
fn var_from_op_id(
    op_id: usize,
    op_id_to_var: &mut HashMap<usize, Variable>,
    variables: &mut ProblemVariables,
) -> Variable {
    *op_id_to_var
        .entry(op_id)
        .or_insert_with(|| variables.add(variable().binary()))
}

fn add_equality_constr(
    ops: &[usize],
    op_id_to_var: &mut HashMap<usize, Variable>,
    variables: &mut ProblemVariables,
    constraints: &mut Vec<Constraint>,
) {
    if let Some(mut prev_op) = ops.first() {
        for op in ops.iter().skip(1) {
            let prev_op_var = var_from_op_id(*prev_op, op_id_to_var, variables);
            let op_var = var_from_op_id(*op, op_id_to_var, variables);
            constraints.push(constraint!(prev_op_var == op_var));
            prev_op = op;
        }
    }
}

// Store the tick that an op is constrained to
fn add_tick_constraint(
    metadata: &HydroIrMetadata,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    model_metadata: &RefCell<ModelMetadata>,
) {
    let ModelMetadata {
        variables,
        constraints,
        op_id_to_var,
        prev_op_input_with_tick,
        ..
    } = &mut *model_metadata.borrow_mut();

    if let LocationId::Tick(tick_id, _) = metadata.location_id {
        // Set each input = to the last input
        let mut inputs = op_id_to_inputs
            .get(&metadata.op.id.unwrap())
            .unwrap()
            .clone();
        if let Some(prev_input) = prev_op_input_with_tick.get(&tick_id) {
            inputs.push(*prev_input);
        }
        add_equality_constr(&inputs, op_id_to_var, variables, constraints);

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
    model_metadata: &RefCell<ModelMetadata>,
) {
    let ModelMetadata {
        variables,
        orig_node_cpu_usage,
        decoupled_node_cpu_usage,
        op_id_to_var,
        ..
    } = &mut *model_metadata.borrow_mut();

    let op_id = metadata.id.unwrap();

    // Calculate total CPU usage on each node (before overheads). Operators are run on the machine that their inputs send to.
    match network_type {
        Some(NetworkType::Send) | Some(NetworkType::SendRecv) | None => {
            if let Some(inputs) = op_id_to_inputs.get(&op_id) {
                // All inputs must be assigned the same var (by constraints above), so it suffices to check one
                if let Some(first_input) = inputs.first() {
                    let input_var = var_from_op_id(*first_input, op_id_to_var, variables);
                    if let Some(cpu_usage) = metadata.cpu_usage {
                        let og_usage_temp = std::mem::take(orig_node_cpu_usage);
                        *orig_node_cpu_usage = og_usage_temp + cpu_usage * (1 - input_var);
                        let decoupled_usage_temp = std::mem::take(decoupled_node_cpu_usage);
                        *decoupled_node_cpu_usage = decoupled_usage_temp + cpu_usage * input_var;
                    }
                }
            }
        }
        _ => {}
    }
    // Special case for network receives: their cpu usage (deserialization) is paid by the receiver, aka the machine they send to.
    match network_type {
        Some(NetworkType::Recv) | Some(NetworkType::SendRecv) => {
            let op_var = var_from_op_id(op_id, op_id_to_var, variables);
            if let Some(recv_cpu_usage) = metadata.network_recv_cpu_usage {
                let og_usage_temp = std::mem::take(orig_node_cpu_usage);
                *orig_node_cpu_usage = og_usage_temp + recv_cpu_usage * (1 - op_var);
                let decoupled_usage_temp = std::mem::take(decoupled_node_cpu_usage);
                *decoupled_node_cpu_usage = decoupled_usage_temp + recv_cpu_usage * op_var;
            }
        }
        _ => {}
    }
}

// Return the variables:
// orig_to_decoupled_var: 1 if (op1 = 0, op2 = 1), 0 otherwise
// decoupled_to_orig_var: 1 if (op1 = 1, op2 = 0), 0 otherwise
fn add_decouple_vars(
    variables: &mut ProblemVariables,
    constraints: &mut Vec<Constraint>,
    op1: usize,
    op2: usize,
    op_id_to_var: &mut HashMap<usize, Variable>,
) -> (Variable, Variable) {
    let op1_var = var_from_op_id(op1, op_id_to_var, variables);
    let op2_var = var_from_op_id(op2, op_id_to_var, variables);
    // 1 if (op1 = 0, op2 = 1), 0 otherwise
    let orig_to_decoupled_var = variables.add(variable().binary());
    // Technically unnecessary since we're using binvar, but future proofing
    constraints.push(constraint!(orig_to_decoupled_var >= 0));
    constraints.push(constraint!(orig_to_decoupled_var >= op2_var - op1_var));
    // 1 if (op1 = 1, op2 = 0), 0 otherwise
    let decoupled_to_orig_var = variables.add(variable().binary());
    constraints.push(constraint!(decoupled_to_orig_var >= 0));
    constraints.push(constraint!(decoupled_to_orig_var >= op1_var - op2_var));
    (orig_to_decoupled_var, decoupled_to_orig_var)
}

fn add_decoupling_overhead(
    node: &HydroNode,
    network_type: &Option<NetworkType>,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    model_metadata: &RefCell<ModelMetadata>,
) {
    let ModelMetadata {
        decoupling_send_overhead,
        decoupling_recv_overhead,
        variables,
        constraints,
        orig_node_cpu_usage,
        decoupled_node_cpu_usage,
        op_id_to_var,
        ..
    } = &mut *model_metadata.borrow_mut();

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
            let (orig_to_decoupled_var, decoupled_to_orig_var) =
                add_decouple_vars(variables, constraints, *input, op_id, op_id_to_var);
            let og_usage_temp = std::mem::take(orig_node_cpu_usage);
            *orig_node_cpu_usage = og_usage_temp
                + (*decoupling_send_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * orig_to_decoupled_var
                + (*decoupling_recv_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * decoupled_to_orig_var;
            let decoupled_usage_temp = std::mem::take(decoupled_node_cpu_usage);
            *decoupled_node_cpu_usage = decoupled_usage_temp
                + (*decoupling_recv_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * orig_to_decoupled_var
                + (*decoupling_send_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * decoupled_to_orig_var;
        }
    }
}

fn add_tee_decoupling_overhead(
    inner_id: usize,
    metadata: &HydroIrMetadata,
    model_metadata: &RefCell<ModelMetadata>,
) {
    let ModelMetadata {
        decoupling_send_overhead,
        decoupling_recv_overhead,
        variables,
        constraints,
        orig_node_cpu_usage,
        decoupled_node_cpu_usage,
        op_id_to_var,
        tee_inner_to_decoupled_vars,
        ..
    } = &mut *model_metadata.borrow_mut();

    let cardinality = metadata.cardinality.unwrap_or_default();
    let op_id = metadata.op.id.unwrap();

    println!("Tee {} has inner {}", op_id, inner_id);

    // 1 if any of the Tees are decoupled from the inner, 0 otherwise, and vice versa
    let (any_orig_to_decoupled_var, any_decoupled_to_orig_var) = *tee_inner_to_decoupled_vars
        .entry(inner_id)
        .or_insert_with(|| {
            let any_orig_to_decoupled_var = variables.add(variable().binary());
            let any_decoupled_to_orig_var = variables.add(variable().binary());
            let og_usage_temp = std::mem::take(orig_node_cpu_usage);
            *orig_node_cpu_usage = og_usage_temp
                + (*decoupling_send_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * any_orig_to_decoupled_var
                + (*decoupling_recv_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * any_decoupled_to_orig_var;
            let decoupled_usage_temp = std::mem::take(decoupled_node_cpu_usage);
            *decoupled_node_cpu_usage = decoupled_usage_temp
                + (*decoupling_recv_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * any_orig_to_decoupled_var
                + (*decoupling_send_overhead * cardinality as f64 + DECOUPLING_PENALTY)
                    * any_decoupled_to_orig_var;
            (any_orig_to_decoupled_var, any_decoupled_to_orig_var)
        });

    let (orig_to_decoupled_var, decoupled_to_orig_var) =
        add_decouple_vars(variables, constraints, inner_id, op_id, op_id_to_var);
    // If any Tee has orig_to_decoupled, then set any_orig_to_decoupled to 1, vice versa
    constraints.push(constraint!(
        any_orig_to_decoupled_var >= orig_to_decoupled_var
    ));
    constraints.push(constraint!(
        any_decoupled_to_orig_var >= decoupled_to_orig_var
    ));
}

fn decouple_analysis_root(
    root: &mut HydroRoot,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    model_metadata: &RefCell<ModelMetadata>,
) {
    // Ignore nodes that are not in the cluster to decouple
    if model_metadata.borrow().cluster_to_decouple != *root.input_metadata().location_id.root() {
        return;
    }

    add_tick_constraint(root.input_metadata(), op_id_to_inputs, model_metadata);
}

fn decouple_analysis_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    op_id_to_inputs: &HashMap<usize, Vec<usize>>,
    model_metadata: &RefCell<ModelMetadata>,
) {
    let network_type = get_network_type(
        node,
        &model_metadata.borrow().cluster_to_decouple.root().key(),
    );
    if let HydroNode::Network { .. } = node {
        // If this is a network and we're not involved, ignore
        if network_type.is_none() {
            return;
        }
        model_metadata
            .borrow_mut()
            .network_ids
            .insert(*op_id, network_type.clone().unwrap());
    } else if model_metadata.borrow().cluster_to_decouple != *node.metadata().location_id.root() {
        // If it's not a network and the operator isn't on the cluster, ignore
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
            model_metadata,
        );
    } else {
        add_decoupling_overhead(node, &network_type, op_id_to_inputs, model_metadata);
    }

    add_cpu_usage(
        node.op_metadata(),
        &network_type,
        op_id_to_inputs,
        model_metadata,
    );
    add_tick_constraint(node.metadata(), op_id_to_inputs, model_metadata);

    if !can_decouple(&node.metadata().collection_kind) {
        model_metadata
            .borrow_mut()
            .do_not_decouple
            .insert(*op_id);
    }
}

fn solve(model_metadata: &RefCell<ModelMetadata>) -> HighsSolution {
    let ModelMetadata {
        variables,
        constraints,
        orig_node_cpu_usage,
        decoupled_node_cpu_usage,
        ..
    } = &mut *model_metadata.borrow_mut();

    // Need values to be taken instead of &mut
    let mut vars = std::mem::take(variables);
    let mut constrs = std::mem::take(constraints);
    let orig_usage = std::mem::take(orig_node_cpu_usage);
    let decoupled_usage = std::mem::take(decoupled_node_cpu_usage);

    // Which node has the highest CPU usage?
    let highest_cpu = vars.add_variable();
    constrs.push(constraint!(highest_cpu >= orig_usage.clone()));
    constrs.push(constraint!(highest_cpu >= decoupled_usage.clone()));

    // Minimize the CPU usage of that node
    let problem = vars.minimise(highest_cpu);
    let solution = highs(problem).with_all(constrs).solve().unwrap();

    println!(
        "Projected orig_usage after decoupling: {}",
        solution.eval(orig_usage)
    );
    println!(
        "Projected decoupled_usage after decoupling: {}",
        solution.eval(decoupled_usage)
    );

    solution
}

pub(crate) fn decouple_analysis(
    ir: &mut [HydroRoot],
    cluster_to_decouple: &LocationId,
    send_overhead: f64,
    recv_overhead: f64,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> DecoupleDecision {
    let model_metadata = RefCell::new(ModelMetadata {
        cluster_to_decouple: cluster_to_decouple.clone(),
        decoupling_send_overhead: send_overhead,
        decoupling_recv_overhead: recv_overhead,
        variables: variables! {},
        constraints: vec![],
        orig_node_cpu_usage: Expression::default(),
        decoupled_node_cpu_usage: Expression::default(),
        op_id_to_var: HashMap::new(),
        do_not_decouple: HashSet::new(),
        prev_op_input_with_tick: HashMap::new(),
        tee_inner_to_decoupled_vars: HashMap::new(),
        network_ids: HashMap::new(),
    });
    let op_id_to_inputs = op_id_to_inputs(
        ir,
        Some(&cluster_to_decouple.key()),
        cycle_source_to_sink_input,
    );

    traverse_dfir::<HydroDeploy>(
        ir,
        |root, _| {
            decouple_analysis_root(root, &op_id_to_inputs, &model_metadata);
        },
        |node, next_op_id| {
            decouple_analysis_node(node, next_op_id, &op_id_to_inputs, &model_metadata);
        },
    );

    for (op_id, inputs) in op_id_to_inputs.iter() {
        let ModelMetadata {
            op_id_to_var,
            variables,
            constraints,
            do_not_decouple,
            ..
        } = &mut *model_metadata.borrow_mut();

        // Add input constraints. All inputs of an op must output to the same machine (be assigned the same var)
        let mut same_loc_ops = inputs.clone();
    		// If this op cannot be decoupled, then make sure it ahs the same location as its input
    		if do_not_decouple.contains(op_id) {
    		    same_loc_ops.push(*op_id);
    		}
        add_equality_constr(&same_loc_ops, op_id_to_var, variables, constraints);
    }

    let solution = solve(&model_metadata);
    let ModelMetadata { op_id_to_var, .. } = &mut *model_metadata.borrow_mut();

    // Build decision: index 0 = original, index 1 = decoupled
    let mut decision = DecoupleDecision::default();
    let mut orig_machine = vec![];
    let mut decoupled_machine = vec![];

    for (op_id, _inputs) in op_id_to_inputs {
        if let Some(op_var) = op_id_to_var.get(&op_id) {
            let loc_idx = solution.value(*op_var).round() as usize;
            decision.insert(op_id, loc_idx);

            if loc_idx == 0 {
                orig_machine.push(op_id);
            } else {
                decoupled_machine.push(op_id);
            }
        }
    }

    orig_machine.sort();
    decoupled_machine.sort();
    println!("Original: {:?}", orig_machine);
    println!("Decoupling: {:?}", decoupled_machine);

    decision
}
