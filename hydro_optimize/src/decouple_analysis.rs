use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::deploy::{AWS_IO_TPS, AWS_NETWORK_BYTES_PER_SEC};
use crate::deploy_and_analyze::{
    MAX_BUDGET_PER_CLUSTER, MAX_OPTIMIZATION_ITERATIONS_PAST_NO_IMPROVEMENT,
};
use crate::parse_results::{NetworkCostTable, SarStats};
use crate::partition_ilp_analysis::{apply_budget_constraints, partition_ilp_analysis};
use crate::partition_syn_analysis::StructOrTupleIndex;
use crate::rewrites::{get_tick_id, is_serializable, is_syntactic_sugar, op_id_to_parents};
use good_lp::solvers::lp_solvers::{GurobiSolver, LpSolution, LpSolver};
use good_lp::{
    Constraint, Expression, ProblemVariables, Solution, SolverModel, Variable, constraint,
    variable, variables,
};
use hydro_lang::compile::builder::ClockId;

use hydro_lang::compile::ir::{
    HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, traverse_dfir,
};
use hydro_lang::location::dynamic::LocationId;

use super::rewrites::{NetworkType, get_network_type};

pub fn num_to_alpha(n: usize) -> String {
    const DIGITS: [&str; 10] = [
        "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
    ];
    n.to_string()
        .chars()
        .map(|c| DIGITS[c as usize - '0' as usize])
        .collect::<Vec<_>>()
        .join("")
}

// Penalty for decoupling regardless of cardinality (to prevent decoupling low cardinality operators)
const DECOUPLING_PENALTY: f64 = 0.0001;
const IMPROVEMENT_THRESHOLD: f64 = 0.01; // Minimum improvement to keep increasing budget
const LEXICOGRAPHIC_EPSILON: f64 = 0.0001; // Tiebreaker weight to minimize non-bottleneck locations

/// Each operator is assigned either 0 or 1
/// 0 means that its output will go to the original node, 1 means that it will go to the decoupled node
/// If there are two operators, map() -> filter(), and they are assigned variables 0 and 1, then that means filter's result ends up on a different machine.
/// The original machine still executes filter(), but pays a serializaton cost for the output of filter.
/// The decoupled machine executes the following operator and pays a deserialization cost for the output of filter.
/// Each operator is executed on the machine indicated by its INPUT's variable.
///
/// Constraints:
/// 1. For binary operators, both parents must be assigned the same var (output to the same location)
/// 2. For Tee, the serialization/deserialization cost is paid only once if multiple branches are assigned different vars. (instead of sending each branch of the Tee over to a different node, we'll send it once and have the receiver Tee)
///
/// HydroNode::Network:
/// 1. If we are the receiver, then create a var, we pay deserialization
/// 2. If we are the sender (and not the receiver), don't create a var (because the destination is another machine and can't change), but we still pay serialization
/// 3. If we are the sender and receiver, then create a var, pay for both
pub(crate) struct DecoupleILPMetadata {
    // Const fields
    pub(crate) bottleneck: LocationId,
    pub(crate) op_counts: HashMap<usize, usize>,
    pub(crate) op_sizes: HashMap<usize, u64>,
    pub(crate) network_cost_table: NetworkCostTable,
    pub(crate) per_op_load: HashMap<usize, SarStats>,
    pub(crate) max_num_locations: usize,
    // Model variables to construct final cost function
    pub(crate) variables: ProblemVariables,
    pub(crate) constraints: Vec<Constraint>,
    /// Per-location resource usage expressions (each field accumulates independently).
    pub(crate) resource_usages: HashMap<usize, ResourceExpressions>,
    pub(crate) op_id_to_var: HashMap<usize, HashMap<usize, Variable>>, // op_id: (location_id: var). Var = 1 for a location if the op is assigned to that location
    prev_op_parent_with_tick: HashMap<ClockId, usize>, // tick_id: last op_id with that tick_id
    tee_inner_to_decoupled_vars: HashMap<usize, HashMap<usize, (Variable, Variable)>>, /* inner_id: (loc: (send var, recv var)) */
    pub(crate) network_ids: HashMap<usize, NetworkType>,
    /// Network ops with their cardinality, size, and per-location assignment variables.
    pub(crate) network_op_loc_vars: Vec<NetworkOpEntry>,
    /// Total cardinality of original (pre-decoupling) network ops, for socket scaling.
    pub(crate) original_network_cardinality: usize,
}

/// A network op's contribution to the discretized socket/CPU model.
pub(crate) struct NetworkOpEntry {
    pub count: usize,
    pub msg_size: u64,
    /// loc -> assignment_var (1 if this op is active on that location)
    pub loc_vars: HashMap<usize, Variable>,
    /// For decoupled send entries: the receiver's loc vars. Used to multiply
    /// socket contribution by receiver's partition count.
    pub recv_loc_vars: Option<HashMap<usize, Variable>>,
}

/// Per-location ILP expressions for each resource type.
#[derive(Clone, Default)]
pub(crate) struct ResourceExpressions {
    pub cpu: Expression,
    pub memory: Expression,
    pub network: Expression,
    pub io: Expression,
}

impl ResourceExpressions {
    pub fn iter(&self) -> impl Iterator<Item = (&Expression, f64)> {
        [
            (&self.cpu, 100.0),
            (&self.memory, 100.0),
            (&self.network, AWS_NETWORK_BYTES_PER_SEC),
            (&self.io, AWS_IO_TPS),
        ]
        .into_iter()
    }
}

/// All inputs needed by the ILP decoupling/partitioning solver.
/// Contains pre-computed averages rather than time series.
#[derive(Clone, Debug)]
pub struct IlpInputs {
    /// Per-op cardinality (messages/sec, averaged over measurement window)
    pub op_counts: HashMap<usize, usize>,
    /// Per-op serialized message size in bytes (median)
    pub op_output_sizes: HashMap<usize, u64>,
    /// Network cost lookup table from calibration
    pub network_cost_table: NetworkCostTable,
    /// Per-op resource load (CPU from perf, memory/IO from blow-up)
    pub per_op_load: HashMap<usize, SarStats>,
    /// Whether to consider partitioning in the ILP
    pub consider_partitioning: bool,
    /// Number of replicas in the bottleneck cluster
    pub cluster_size: usize,
    /// Total avg active sockets (sources + sinks) on the bottleneck location
    pub baseline_sockets: usize,
}

/// Returns a map from location to a binary var
/// Lazily creates the var
fn var_from_op_id(
    op_id: usize,
    num_locations: usize,
    op_id_to_var: &mut HashMap<usize, HashMap<usize, Variable>>,
    variables: &mut ProblemVariables,
    constraints: &mut Vec<Constraint>,
) -> HashMap<usize, Variable> {
    let result = op_id_to_var
        .entry(op_id)
        .or_insert_with(|| {
            let mut loc_to_var = HashMap::new();
            let mut sum_expr = Expression::default();
            for loc in 0..num_locations {
                let var = variables.add(variable().binary().name(format!(
                    "op{}loc{}",
                    num_to_alpha(op_id),
                    num_to_alpha(loc)
                )));
                loc_to_var.insert(loc, var);
                sum_expr += var;
            }
            // Constrain sum to 1 (var is assigned to exactly 1 location)
            constraints.push(constraint!(sum_expr == 1));
            loc_to_var
        })
        .clone();
    result
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
                let op_var = op_vars.get(loc).unwrap();
                constraints.push(constraint!(*prev_op_var == *op_var));
            }

            prev_op_vars = op_vars;
        }
    }
}

// Store the tick that an op is constrained to
fn add_tick_constraint(
    metadata: &HydroIrMetadata,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        variables,
        constraints,
        max_num_locations: num_locations,
        op_id_to_var,
        prev_op_parent_with_tick,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    if let Some(tick_id) = get_tick_id(&metadata.location_id) {
        // Set each parent = to the last parent
        let mut parents: Vec<usize> = op_id_to_parents
            .get(&metadata.op.id.unwrap())
            .unwrap()
            .clone();
        if let Some(prev_parent) = prev_op_parent_with_tick.get(&tick_id) {
            parents.push(*prev_parent);
        }
        add_equality_constr(
            &parents,
            *num_locations,
            op_id_to_var,
            variables,
            constraints,
        );

        // Set this op's last parent as the last op's parent that had this tick
        if let Some(last_parent) = parents.last() {
            prev_op_parent_with_tick.insert(tick_id, *last_parent);
        }
    }
}

fn add_op_resource_usage(
    metadata: &HydroIrOpMetadata,
    network_type: &Option<NetworkType>,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        per_op_load,
        op_counts,
        op_sizes,
        network_cost_table,
        variables,
        constraints,
        max_num_locations: num_locations,
        resource_usages,
        op_id_to_var,
        network_op_loc_vars,
        original_network_cardinality,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.id.unwrap();

    // Network ops: CPU cost handled by discretized socket model in solve().
    // Here we just record the op's assignment vars and add non-CPU costs directly.
    if network_type.is_some() {
        let parent_id = op_id_to_parents
            .get(&op_id)
            .and_then(|p| p.first().copied());
        let count = op_counts
            .get(&parent_id.unwrap_or(op_id))
            .copied()
            .unwrap_or(0);
        let size = op_sizes.get(&op_id).copied().unwrap_or(0);
        if count > 0 && size > 0 {
            let bandwidth_per_msg = network_cost_table.cost_per_message(size, 1).network;

            let mut loc_vars: HashMap<usize, Variable> = HashMap::new();
            match network_type {
                Some(NetworkType::Send) | Some(NetworkType::SendRecv) | None => {
                    if let Some(parents) = op_id_to_parents.get(&op_id)
                        && let Some(first_parent) = parents.first()
                    {
                        let parent_vars = var_from_op_id(
                            *first_parent,
                            *num_locations,
                            op_id_to_var,
                            variables,
                            constraints,
                        );
                        add_bandwidth_cost(
                            &parent_vars,
                            bandwidth_per_msg * count as f64,
                            resource_usages,
                            &mut loc_vars,
                        );
                    }
                }
                _ => {}
            }
            match network_type {
                Some(NetworkType::Recv) | Some(NetworkType::SendRecv) => {
                    let op_vars =
                        var_from_op_id(op_id, *num_locations, op_id_to_var, variables, constraints);
                    add_bandwidth_cost(
                        &op_vars,
                        bandwidth_per_msg * count as f64,
                        resource_usages,
                        &mut loc_vars,
                    );
                }
                _ => {}
            }
            network_op_loc_vars.push(NetworkOpEntry {
                count,
                msg_size: size,
                loc_vars,
                recv_loc_vars: None,
            });
            *original_network_cardinality += count;
        }
        return;
    }

    let Some(op_load) = per_op_load.get(&op_id) else {
        return;
    };
    if op_load.cpu <= 0.0 && op_load.memory <= 0.0 && op_load.network <= 0.0 && op_load.io <= 0.0 {
        return;
    }

    // Non-network ops: cost is paid at the parent's location
    if let Some(parents) = op_id_to_parents.get(&op_id)
        && let Some(first_parent) = parents.first()
    {
        let parent_vars = var_from_op_id(
            *first_parent,
            *num_locations,
            op_id_to_var,
            variables,
            constraints,
        );
        for (loc, parent_var) in parent_vars {
            let res = resource_usages.get_mut(&loc).unwrap();
            let cpu_temp = std::mem::take(&mut res.cpu);
            res.cpu = cpu_temp + op_load.cpu * parent_var;
            let mem_temp = std::mem::take(&mut res.memory);
            res.memory = mem_temp + op_load.memory * parent_var;
            let net_temp = std::mem::take(&mut res.network);
            res.network = net_temp + op_load.network * parent_var;
            let io_temp = std::mem::take(&mut res.io);
            res.io = io_temp + op_load.io * parent_var;
        }
    }
}

/// Adds network bandwidth cost to resource usages and collects loc_vars for each location.
fn add_bandwidth_cost(
    vars: &HashMap<usize, Variable>,
    total_bandwidth: f64,
    resource_usages: &mut HashMap<usize, ResourceExpressions>,
    loc_vars: &mut HashMap<usize, Variable>,
) {
    for (loc, var) in vars {
        loc_vars.insert(*loc, *var);
        let res = resource_usages.get_mut(loc).unwrap();
        let net_temp = std::mem::take(&mut res.network);
        res.network = net_temp + total_bandwidth * *var;
    }
}

/// Adds network bandwidth + decoupling penalty for a pair of send/recv vars per location,
/// and registers them in the discretized socket model.
fn register_decoupled_network(
    send_recv_vars: &HashMap<usize, (Variable, Variable)>,
    num_locations: usize,
    total_bandwidth: f64,
    cardinality: usize,
    msg_size: u64,
    resource_usages: &mut HashMap<usize, ResourceExpressions>,
    network_op_loc_vars: &mut Vec<NetworkOpEntry>,
) {
    let mut send_loc_vars: HashMap<usize, Variable> = HashMap::new();
    let mut recv_loc_vars: HashMap<usize, Variable> = HashMap::new();
    for loc in 0..num_locations {
        let (send_var, recv_var) = send_recv_vars.get(&loc).unwrap();
        send_loc_vars.insert(loc, *send_var);
        recv_loc_vars.insert(loc, *recv_var);
        let res = resource_usages.get_mut(&loc).unwrap();
        let net_temp = std::mem::take(&mut res.network);
        res.network = net_temp + total_bandwidth * *send_var + total_bandwidth * *recv_var;
        let cpu_temp = std::mem::take(&mut res.cpu);
        res.cpu = cpu_temp + DECOUPLING_PENALTY * *send_var + DECOUPLING_PENALTY * *recv_var;
    }
    network_op_loc_vars.push(NetworkOpEntry {
        count: cardinality,
        msg_size,
        loc_vars: send_loc_vars,
        recv_loc_vars: Some(recv_loc_vars.clone()),
    });
    network_op_loc_vars.push(NetworkOpEntry {
        count: cardinality,
        msg_size,
        loc_vars: recv_loc_vars,
        recv_loc_vars: None,
    });
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
        let send_var = variables.add(variable().min(0).name(format!(
            "decouplesend{}x{}loc{}",
            num_to_alpha(op1),
            num_to_alpha(op2),
            num_to_alpha(loc)
        )));
        let recv_var = variables.add(variable().min(0).name(format!(
            "decouplerecv{}x{}loc{}",
            num_to_alpha(op1),
            num_to_alpha(op2),
            num_to_alpha(loc)
        )));

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
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        op_counts,
        op_sizes,
        network_cost_table,
        variables,
        constraints,
        max_num_locations: num_locations,
        resource_usages,
        op_id_to_var,
        network_op_loc_vars,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let metadata = node.metadata();
    let op_id = metadata.op.id.unwrap();
    let cardinality = op_counts.get(&op_id).copied().unwrap_or(0);
    let size = op_sizes.get(&op_id).copied().unwrap_or(0);
    if cardinality == 0 || size == 0 {
        return;
    }
    let bandwidth_per_msg = network_cost_table.cost_per_message(size, 1).network;
    let total_bandwidth = bandwidth_per_msg * cardinality as f64;

    if let Some(parents) = op_id_to_parents.get(&op_id)
        && let Some(parent) = parents.first()
    {
        let decouple_vars = add_decouple_vars(
            *num_locations,
            variables,
            constraints,
            *parent,
            op_id,
            op_id_to_var,
        );

        register_decoupled_network(
            &decouple_vars,
            *num_locations,
            total_bandwidth,
            cardinality,
            size,
            resource_usages,
            network_op_loc_vars,
        );
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
        op_counts,
        op_sizes,
        network_cost_table,
        variables,
        constraints,
        max_num_locations: num_locations,
        resource_usages,
        op_id_to_var,
        tee_inner_to_decoupled_vars,
        network_op_loc_vars,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.op.id.unwrap();
    let cardinality = op_counts.get(&op_id).copied().unwrap_or(0);
    let size = op_sizes.get(&op_id).copied().unwrap_or(0);
    if cardinality == 0 || size == 0 {
        return;
    }
    let total_bandwidth = network_cost_table.cost_per_message(size, 1).network * cardinality as f64;

    // For each location, send_var/recv_var are created once per unique inner_id
    let any_send_recv_vars = tee_inner_to_decoupled_vars
        .entry(inner_id)
        .or_insert_with(|| {
            let mut loc_to_vars = HashMap::new();
            for loc in 0..*num_locations {
                let send_var = variables.add(variable().binary().name(format!(
                    "teesend{}loc{}",
                    num_to_alpha(inner_id),
                    num_to_alpha(loc)
                )));
                let recv_var = variables.add(variable().binary().name(format!(
                    "teerecv{}loc{}",
                    num_to_alpha(inner_id),
                    num_to_alpha(loc)
                )));
                loc_to_vars.insert(loc, (send_var, recv_var));
            }
            register_decoupled_network(
                &loc_to_vars,
                *num_locations,
                total_bandwidth,
                cardinality,
                size,
                resource_usages,
                network_op_loc_vars,
            );
            loc_to_vars
        });

    // Create decouple vars between the inner and this op
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
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    // Ignore nodes that are not in the cluster to decouple
    if decoupling_metadata.borrow().bottleneck != *root.input_metadata().location_id.root() {
        return;
    }

    add_tick_constraint(root.input_metadata(), op_id_to_parents, decoupling_metadata);
}

fn decouple_analysis_node(
    node: &mut HydroNode,
    op_id: &mut usize,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    // Store network type for later (when checking the solution)
    let network_type = get_network_type(node, &decoupling_metadata.borrow().bottleneck);
    if let Some(network_type) = network_type.clone() {
        decoupling_metadata
            .borrow_mut()
            .network_ids
            .insert(*op_id, network_type);
    } else if *node.metadata().location_id.root() != decoupling_metadata.borrow().bottleneck {
        // If this isn't a network to/from the bottleneck, and the node isn't on the bottleneck, then it is irrelevant
        return;
    }

    if let HydroNode::Partition { inner, .. } = node {
        // Partition must share its inner's location because of how emit_core generates DFIR
        let inner_id = inner.0.borrow().metadata().op.id.unwrap();
        let DecoupleILPMetadata {
            variables,
            constraints,
            max_num_locations: num_locations,
            op_id_to_var,
            ..
        } = &mut *decoupling_metadata.borrow_mut();
        add_equality_constr(
            &[inner_id, *op_id],
            *num_locations,
            op_id_to_var,
            variables,
            constraints,
        );
    } else if is_syntactic_sugar(node) || !is_serializable(&node.metadata().collection_kind) {
        // Syntactic sugar nodes (Cast, Batch, YieldConcat, etc.) must stay with their parent
        // So do nodes that we do not know how to serialize
        if let Some(parents) = op_id_to_parents.get(op_id)
            && let Some(&parent_id) = parents.first()
        {
            let DecoupleILPMetadata {
                variables,
                constraints,
                max_num_locations: num_locations,
                op_id_to_var,
                ..
            } = &mut *decoupling_metadata.borrow_mut();
            add_equality_constr(
                &[parent_id, *op_id],
                *num_locations,
                op_id_to_var,
                variables,
                constraints,
            );
        }
    } else if let HydroNode::Tee {
        inner, metadata, ..
    } = node
    {
        // Add decoupling overhead. For Tees of the same inner, even if multiple are decoupled, only penalize decoupling once
        add_tee_decoupling_overhead(
            inner.0.borrow().metadata().op.id.unwrap(),
            metadata,
            decoupling_metadata,
        );
    } else {
        add_decoupling_overhead(node, op_id_to_parents, decoupling_metadata);
    }

    add_op_resource_usage(
        node.op_metadata(),
        &network_type,
        op_id_to_parents,
        decoupling_metadata,
    );
    add_tick_constraint(node.metadata(), op_id_to_parents, decoupling_metadata);
}

fn solve(
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    baseline_sockets: usize,
    is_n_partitions: &[Vec<Variable>],
) -> LpSolution {
    let DecoupleILPMetadata {
        variables,
        constraints,
        resource_usages,
        network_op_loc_vars,
        max_num_locations,
        network_cost_table,
        original_network_cardinality,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let mut vars = std::mem::take(variables);
    let mut constrs = std::mem::take(constraints);

    // Symmetry breaking: order locations by number of assigned ops (descending).
    // Location 0 gets the most ops, location 1 the second most, etc.
    for loc in 1..*max_num_locations {
        let mut ops_at_prev = Expression::default();
        let mut ops_at_curr = Expression::default();
        for op_vars in op_id_to_var.values() {
            if let Some(&v) = op_vars.get(&(loc - 1)) {
                ops_at_prev += v;
            }
            if let Some(&v) = op_vars.get(&loc) {
                ops_at_curr += v;
            }
        }
        constrs.push(constraint!(ops_at_prev >= ops_at_curr));
    }

    // Discretized socket-aware network CPU cost.
    // For each location, enumerate possible socket counts and use big-M to select the correct cost.
    let max_sockets = (baseline_sockets * 2).max(1);

    if *original_network_cardinality > 0 {
        let big_m = 200.0; // Network CPU can't exceed 200% per location
        let sockets_per_cardinality =
            baseline_sockets as f64 / *original_network_cardinality as f64;

        for loc in 0..*max_num_locations {
            // For decoupled sends, precompute z = send_var AND has_n for each (entry, recv_loc, n).
            // These are reused for both socket-level scaling and CPU cost scaling.
            // entry_index -> vec of (n, z_var)
            let mut decoupled_send_z_vars: HashMap<usize, Vec<(usize, Variable)>> = HashMap::new();

            // Socket contribution for this location: sum of count_i * assignment_var[i][loc]
            // For decoupled sends, scale by receiver's partition count (more partitions = more sockets).
            let mut location_total_network_cardinality = Expression::default();
            for (entry_idx, entry) in network_op_loc_vars.iter().enumerate() {
                let Some(&var) = entry.loc_vars.get(&loc) else {
                    continue;
                };
                if let Some(recv_locs) = &entry.recv_loc_vars {
                    // Decoupled send: contribution = count * n * is_sending_to_n_partitions
                    let mut z_vars_for_entry = Vec::new();
                    for (&recv_loc, &_recv_var) in recv_locs {
                        for (n, &has_n) in is_n_partitions[recv_loc].iter().enumerate() {
                            if n == 0 {
                                continue;
                            }
                            let is_sending_to_n_partitions =
                                vars.add(variable().min(0).max(1).name(format!(
                                    "sendton{}loc{}rloc{}e{}",
                                    num_to_alpha(n),
                                    num_to_alpha(loc),
                                    num_to_alpha(recv_loc),
                                    num_to_alpha(entry_idx)
                                )));
                            constrs.push(constraint!(is_sending_to_n_partitions <= var));
                            constrs.push(constraint!(is_sending_to_n_partitions <= has_n));
                            constrs
                                .push(constraint!(is_sending_to_n_partitions >= var + has_n - 1));
                            location_total_network_cardinality +=
                                entry.count as f64 * n as f64 * is_sending_to_n_partitions;
                            z_vars_for_entry.push((n, is_sending_to_n_partitions));
                        }
                    }
                    decoupled_send_z_vars.insert(entry_idx, z_vars_for_entry);
                } else {
                    location_total_network_cardinality += entry.count as f64 * var;
                }
            }

            // Binary indicators for socket level S = 1..max_sockets
            let mut is_s_vars: Vec<Variable> = Vec::new();
            let mut sum_indicators = Expression::default();
            let mut num_sockets_int = Expression::default();
            for s in 2..=max_sockets {
                let is_s = vars.add(variable().binary().name(format!(
                    "issocket{}loc{}",
                    num_to_alpha(s),
                    num_to_alpha(loc)
                )));
                sum_indicators += is_s;
                num_sockets_int += s as f64 * is_s;
                is_s_vars.push(is_s);
            }
            constrs.push(constraint!(sum_indicators == 1));
            constrs.push(constraint!(
                num_sockets_int.clone()
                    >= location_total_network_cardinality.clone() * sockets_per_cardinality
            ));

            // effective_network_cpu: continuous var, lower-bounded by the active level's cost
            let effective_network_cpu = vars.add(
                variable()
                    .min(0)
                    .name(format!("netcpuloc{}", num_to_alpha(loc))),
            );

            for (idx, &is_s) in is_s_vars.iter().enumerate() {
                let s = idx + 1;
                let mut cpu_at_s = Expression::default();
                for (entry_idx, entry) in network_op_loc_vars.iter().enumerate() {
                    let Some(&var) = entry.loc_vars.get(&loc) else {
                        continue;
                    };
                    let cpu_per_msg = network_cost_table.cost_per_message(entry.msg_size, s).cpu;
                    if let Some(sending_to_partitions_vars) = decoupled_send_z_vars.get(&entry_idx)
                    {
                        // Decoupled send: CPU = count * n * cpu_per_msg * is_sending_to_n_partitions
                        for &(n, is_sending_to_n_partitions) in sending_to_partitions_vars {
                            cpu_at_s += entry.count as f64
                                * n as f64
                                * cpu_per_msg
                                * is_sending_to_n_partitions;
                        }
                    } else {
                        cpu_at_s += entry.count as f64 * cpu_per_msg * var;
                    }
                }
                constrs.push(constraint!(
                    effective_network_cpu >= cpu_at_s - big_m * (1 - is_s)
                ));
            }

            // Divide effective_network_cpu by partition count for this location
            let divided_network_cpu = vars.add(
                variable()
                    .min(0)
                    .name(format!("divnetcpuloc{}", num_to_alpha(loc))),
            );
            for (n, &has_n) in is_n_partitions[loc].iter().enumerate() {
                if n == 0 {
                    continue;
                }
                let scale = 1.0 / n as f64;
                constrs.push(constraint!(
                    divided_network_cpu >= effective_network_cpu * scale - big_m * (1 - has_n)
                ));
            }

            // Add divided network CPU to this location's CPU resource usage
            let res = resource_usages.get_mut(&loc).unwrap();
            let cpu_temp = std::mem::take(&mut res.cpu);
            res.cpu = cpu_temp + divided_network_cpu;
        }
    }

    // Minimize the highest saturation across all locations and resources,
    // with a small tiebreaker to also minimize non-bottleneck locations.
    let highest_saturation = vars.add(variable().name("maxsaturation"));
    let mut sum_saturations = Expression::default();
    for res in resource_usages.values() {
        for (usage, capacity) in res.iter() {
            let sat = usage.clone() * (1.0 / capacity);
            constrs.push(constraint!(highest_saturation >= sat.clone()));
            sum_saturations += sat;
        }
    }

    let objective = highest_saturation + LEXICOGRAPHIC_EPSILON * sum_saturations;

    println!(
        "  Solving ILP: {} vars, {} constraints",
        vars.len(),
        constrs.len()
    );

    let problem = vars.minimise(objective);
    let solve_start = Instant::now();
    let solution = problem
        .using(LpSolver(GurobiSolver::new()))
        .with_all(constrs)
        .solve()
        .unwrap();
    println!("  Solver finished in {:.2?}", solve_start.elapsed());

    solution
}

fn op_loc(op_vars: &HashMap<usize, Variable>, solution: &LpSolution) -> usize {
    for (loc, var) in op_vars {
        let value = solution.value(*var).round();
        if value == 1.0 {
            return *loc;
        }
    }
    panic!("No location assigned to op");
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rewrite {
    /// The original `LocationId` that this rewrite is splitting (index 0 in the loc maps below).
    pub original_location: LocationId,
    /// Size of the original cluster; new clusters created by this rewrite inherit this size.
    pub cluster_size: usize,
    /// The machine budget used for this rewrite (max_num_locations).
    pub budget: usize,
    /// Projected max saturation per location (0-1, across all resources).
    pub location_costs: HashMap<usize, f64>,
    /// Number of partitions per location (0 or absent = no partitioning).
    pub num_partitions: HashMap<usize, usize>,
    /// op_id → its new location (where its output streams to)
    pub op_to_loc: HashMap<usize, usize>,
    /// op_id → (src_loc, dst_loc). A new network should be inserted AFTER this op,
    /// sending its output from src_loc to dst_loc. The op itself executes at src_loc
    /// (its parent's location).
    pub op_to_network: HashMap<usize, (usize, usize)>,
    /// Location indices that can be arbitrarily partitioned because it does not persist.
    pub stateless_partitionable: HashSet<usize>,
    /// Location indices that are partitionable via a field
    pub field_partitionable: HashSet<usize>,
    pub partitionable: HashSet<usize>, // Union of stateless_partitionable and field_partitionable
    /// For each op, what field it should be partitioned on
    pub partition_field_choices: HashMap<usize, StructOrTupleIndex>,
}

impl Rewrite {
    pub fn new(original_location: LocationId, cluster_size: usize, budget: usize) -> Self {
        Self {
            original_location,
            cluster_size,
            budget,
            location_costs: HashMap::new(),
            num_partitions: HashMap::new(),
            op_to_loc: HashMap::new(),
            op_to_network: HashMap::new(),
            stateless_partitionable: HashSet::new(),
            field_partitionable: HashSet::new(),
            partitionable: HashSet::new(),
            partition_field_choices: HashMap::new(),
        }
    }

    /// All unique location indices referenced.
    pub fn locations(&self) -> HashSet<usize> {
        let mut locs: HashSet<usize> = self.op_to_loc.values().copied().collect();
        for (s, d) in self.op_to_network.values() {
            locs.insert(*s);
            locs.insert(*d);
        }
        locs
    }

    pub fn num_locations(&self) -> usize {
        self.locations().len()
    }

    /// Maximum cost across all locations (the bottleneck cost).
    pub fn max_cost(&self) -> f64 {
        self.location_costs.values().copied().fold(0.0f64, f64::max)
    }
}

pub(crate) fn decouple_analysis(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    inputs: IlpInputs,
    max_num_locations: usize,
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
) -> Rewrite {
    println!("  Building ILP for budget={}...", max_num_locations);
    assert!(
        max_num_locations >= 2,
        "Must decouple to at least 2 locations (original location, decoupled location)"
    );

    let IlpInputs {
        op_counts,
        op_output_sizes: op_sizes,
        network_cost_table,
        per_op_load,
        consider_partitioning,
        cluster_size,
        baseline_sockets,
    } = inputs;

    let decoupling_metadata = RefCell::new(DecoupleILPMetadata {
        bottleneck: bottleneck.clone(),
        op_counts: op_counts.clone(),
        op_sizes: op_sizes.clone(),
        network_cost_table: network_cost_table.clone(),
        per_op_load: per_op_load.clone(),
        max_num_locations,
        variables: variables! {},
        constraints: vec![],
        resource_usages: HashMap::from_iter(
            (0..max_num_locations).map(|loc_id| (loc_id, ResourceExpressions::default())),
        ),
        op_id_to_var: HashMap::new(),
        prev_op_parent_with_tick: HashMap::new(),
        tee_inner_to_decoupled_vars: HashMap::new(),
        network_ids: HashMap::new(),
        network_op_loc_vars: Vec::new(),
        original_network_cardinality: 0,
    });
    let op_id_to_parents = op_id_to_parents(ir, Some(bottleneck), cycle_source_to_sink_parent);

    let bottleneck_ops: RefCell<HashSet<usize>> = RefCell::new(HashSet::new());
    traverse_dfir(
        ir,
        |root, _| {
            decouple_analysis_root(root, &op_id_to_parents, &decoupling_metadata);
        },
        |node, next_op_id| {
            if *node.metadata().location_id.root() == decoupling_metadata.borrow().bottleneck
                || get_network_type(node, &decoupling_metadata.borrow().bottleneck).is_some()
            {
                bottleneck_ops.borrow_mut().insert(*next_op_id);
            }
            decouple_analysis_node(node, next_op_id, &op_id_to_parents, &decoupling_metadata);
        },
    );
    for parents in op_id_to_parents.values() {
        let DecoupleILPMetadata {
            op_id_to_var,
            variables,
            constraints,
            ..
        } = &mut *decoupling_metadata.borrow_mut();
        // Add parent constraints. All parents of an op must output to the same machine (be assigned the same var)
        add_equality_constr(
            parents,
            max_num_locations,
            op_id_to_var,
            variables,
            constraints,
        );
    }

    // Consider partitioning after all variables for decoupling have been created
    let partition_metadata = if consider_partitioning {
        Some(partition_ilp_analysis(
            ir,
            &op_id_to_parents,
            &decoupling_metadata,
        ))
    } else {
        None
    };

    // Apply resource-budget constraints (location cap + CPU division)
    let is_n_partitions = apply_budget_constraints(
        max_num_locations,
        partition_metadata.as_ref(),
        &decoupling_metadata,
    );

    let solution = solve(&decoupling_metadata, baseline_sockets, &is_n_partitions);

    // Build the DecoupleDecision by separating placement ops from network-insertion ops.
    let DecoupleILPMetadata {
        op_id_to_var,
        network_ids,
        resource_usages,
        ..
    } = &*decoupling_metadata.borrow();

    let mut result = Rewrite::new(bottleneck.clone(), cluster_size, max_num_locations);

    // Populate per-location costs (max saturation) from the solution
    for (loc, res) in resource_usages {
        let max_sat = res
            .iter()
            .map(|(expr, cap)| solution.eval(expr.clone()) / cap)
            .fold(0.0_f64, f64::max);
        result.location_costs.insert(*loc, max_sat);
    }

    for (&op_id, parents) in &op_id_to_parents {
        let Some(op_vars) = op_id_to_var.get(&op_id) else {
            continue;
        };
        let op_location = op_loc(op_vars, &solution);
        let parent_location = parents
            .first()
            .and_then(|p| op_id_to_var.get(p))
            .map(|pv| op_loc(pv, &solution));

        let network_type = network_ids.get(&op_id);

        if network_type.is_none()
            && let Some(parent_loc) = parent_location
        {
            // Non-network op: if parent is at a different location, record a new network edge
            if parent_loc != op_location {
                result
                    .op_to_network
                    .insert(op_id, (parent_loc, op_location));
            }
        }

        // Only place ops that are actually on the bottleneck or are network
        // boundaries into it. Skip Send-only ops and ops from other clusters.
        if bottleneck_ops.borrow().contains(&op_id)
            && !network_type.is_some_and(|t| *t == NetworkType::Send)
        {
            result.op_to_loc.insert(op_id, op_location);
        }
    }

    // Evaluate per-location partitionability
    if let Some(pm) = partition_metadata {
        for loc in 0..max_num_locations {
            let num_relevant = solution
                .eval(
                    pm.num_relevant_operators
                        .get(&loc)
                        .cloned()
                        .unwrap_or_default(),
                )
                .round() as i64;
            let num_partitionable = solution
                .eval(
                    pm.partitionable_operators
                        .get(&loc)
                        .cloned()
                        .unwrap_or_default(),
                )
                .round() as i64;
            let num_persists = solution
                .eval(
                    pm.num_persist_operators
                        .get(&loc)
                        .cloned()
                        .unwrap_or_default(),
                )
                .round() as i64;
            println!(
                "  Location {}: num_relevant={}, num_partitionable={}, num_persists={}",
                loc, num_relevant, num_partitionable, num_persists
            );
            if num_persists == 0 {
                result.stateless_partitionable.insert(loc);
            } else if num_relevant == num_partitionable {
                result.field_partitionable.insert(loc);
            }
        }

        // Resolve per-op field choices from the ILP solution. For each op that has field
        // variables, find which field the solver set to 1 (if any).
        for (op_id, fields) in &pm.op_id_to_field_vars {
            if let Some((name, _)) = fields
                .iter()
                .find(|(_, var)| solution.value(**var).round() == 1.0)
            {
                result.partition_field_choices.insert(*op_id, name.clone());
            }
        }
    }

    result.partitionable = result
        .stateless_partitionable
        .union(&result.field_partitionable)
        .copied()
        .collect();

    // Read per-location partition counts from the ILP solution
    for loc in 0..max_num_locations {
        for (n, &var) in is_n_partitions[loc].iter().enumerate() {
            if solution.value(var).round() == 1.0 && n > 1 {
                result.num_partitions.insert(loc, n);
                break;
            }
        }
    }

    result
}

/// Tries increasing machine budgets (2, 3, ...) for the given bottleneck.
/// For each budget the ILP decides the optimal split between locations and
/// partitions internally, so only one ILP run is needed per budget level.
/// Stops early if max_cost fails to decrease for 2 consecutive iterations.
pub fn find_optimal_budget(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    inputs: &IlpInputs,
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
) -> Vec<Rewrite> {
    let mut results = Vec::new();
    let mut no_improvement = 0;

    for budget in 2..=MAX_BUDGET_PER_CLUSTER {
        let start = Instant::now();
        let rewrite = decouple_analysis(
            ir,
            bottleneck,
            inputs.clone(),
            budget,
            cycle_source_to_sink_parent,
        );
        let cost = rewrite.max_cost();
        println!(
            "  budget={}: max_saturation={:.4}, {} locations, partitions={:?}, solved in {:.2?}",
            budget,
            cost,
            rewrite.num_locations(),
            rewrite.num_partitions,
            start.elapsed()
        );

        let improved = results
            .last()
            .is_none_or(|prev: &Rewrite| prev.max_cost() - cost > IMPROVEMENT_THRESHOLD);

        if improved {
            no_improvement = 0;
            results.push(rewrite);
        } else {
            no_improvement += 1;
            if no_improvement >= MAX_OPTIMIZATION_ITERATIONS_PAST_NO_IMPROVEMENT {
                break;
            }
        }
    }

    results
}
