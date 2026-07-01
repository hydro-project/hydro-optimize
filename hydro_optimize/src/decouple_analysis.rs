use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::deploy::{AWS_IO_TPS, AWS_NETWORK_BYTES_PER_SEC};
use crate::deploy_and_analyze::MAX_BUDGET_PER_CLUSTER;
use crate::parse_results::{NetworkCostTable, SarStats};
use crate::partition_ilp_analysis::{apply_budget_constraints, partition_ilp_analysis};
use crate::partition_syn_analysis::StructOrTupleIndex;
use crate::rewrites::{
    all_inputs, get_tick_id, is_serializable, is_syntactic_sugar, nodes_dependent_on_inputs,
    op_id_to_parents,
};
use good_lp::solvers::lp_solvers::{GurobiSolver, LpSolution, LpSolver};
use good_lp::{
    Constraint, Expression, ProblemVariables, Solution, SolverModel, Variable, constraint,
    variable, variables,
};
use hydro_lang::compile::builder::ClockId;

use hydro_lang::compile::ir::{HydroIrMetadata, HydroNode, HydroRoot, traverse_dfir};
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
const LEXICOGRAPHIC_EPSILON: f64 = 0.0001; // Tiebreaker weight to minimize non-bottleneck locations
const FIELD_SPECIFICITY_EPSILON: f64 = LEXICOGRAPHIC_EPSILON * 0.001; // Smaller tiebreaker to prefer broader partition fields

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
    // Add small penalties for partitioning on too-specific fields, since those are less likely to be uniformly distributed
    pub(crate) field_specificity_penalty: Expression,
}

/// Per-location ILP expressions for each resource type.
#[derive(Clone, Default)]
pub struct ResourceExpressions {
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
    op_id_to_var
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

/// Adds `load * var` (per resource) at each variable's location.
fn add_load_to_locations(
    resource_usages: &mut HashMap<usize, ResourceExpressions>,
    vars: &HashMap<usize, Variable>,
    load: &SarStats,
) {
    for (loc, var) in vars {
        let res = resource_usages.get_mut(loc).unwrap();
        let cpu_temp = std::mem::take(&mut res.cpu);
        res.cpu = cpu_temp + load.cpu * *var;
        // let mem_temp = std::mem::take(&mut res.memory);
        // res.memory = mem_temp + load.memory * *var;
        // let net_temp = std::mem::take(&mut res.network);
        // res.network = net_temp + load.network * *var;
        // let io_temp = std::mem::take(&mut res.io);
        // res.io = io_temp + load.io * *var;
    }
}

fn add_op_resource_usage(
    node: &HydroNode,
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
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = node.op_metadata().id.unwrap();
    let size = op_sizes.get(&op_id).copied().unwrap_or(0);

    // Send / produce side: charged at the location its parent sends to. Non-network
    // ops also land here — their measured compute load is paid where their parent runs.
    match network_type {
        Some(NetworkType::Send) | Some(NetworkType::SendRecv) | None => {
            let (load, charge_at) = if network_type.is_some() {
                // Send cardinality = cardinality of Network's parent. Will be larger than the Network's cardinality if it is a broadcast
                let input_id = node.input_metadata()[0].op.id;
                let send_count = input_id
                    .and_then(|iid| op_counts.get(&iid).copied())
                    .unwrap_or(0);
                (network_cost_table.network_cost(send_count, size), input_id)
            } else {
                // Non-network op: its measured per-op load, charged at its parent.
                let load = per_op_load.get(&op_id).copied().unwrap_or_default();
                let parent = op_id_to_parents
                    .get(&op_id)
                    .and_then(|p| p.first().copied());
                (load, parent)
            };
            if !load.is_zero()
                && let Some(charge_at) = charge_at
            {
                let vars = var_from_op_id(
                    charge_at,
                    *num_locations,
                    op_id_to_var,
                    variables,
                    constraints,
                );
                add_load_to_locations(resource_usages, &vars, &load);
            }
        }
        _ => {}
    }
    // Receive side: deserialization is paid by the receiver (this op's location). The
    // count is singular — the receiver's own counter already reflects the fan-in.
    match network_type {
        Some(NetworkType::Recv) | Some(NetworkType::SendRecv) => {
            let load =
                network_cost_table.network_cost(op_counts.get(&op_id).copied().unwrap_or(0), size);
            if !load.is_zero() {
                let vars =
                    var_from_op_id(op_id, *num_locations, op_id_to_var, variables, constraints);
                add_load_to_locations(resource_usages, &vars, &load);
            }
        }
        _ => {}
    }
}

fn network_cost_for_decoupling_op(
    op_id: usize,
    op_counts: &HashMap<usize, usize>,
    op_sizes: &HashMap<usize, u64>,
    network_cost_table: &NetworkCostTable,
) -> SarStats {
    let cardinality = op_counts.get(&op_id).copied().unwrap_or(0);
    match op_sizes.get(&op_id) {
        Some(&bytes) => network_cost_table.network_cost(cardinality, bytes),
        None => SarStats::default(),
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
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let metadata = node.metadata();
    let op_id = metadata.op.id.unwrap();
    let net_cost = network_cost_for_decoupling_op(op_id, op_counts, op_sizes, network_cost_table);

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
        for loc in 0..*num_locations {
            let res = resource_usages.get_mut(&loc).unwrap();
            let (send_var, recv_var) = decouple_vars.get(&loc).unwrap();
            add_resource_cost(res, &net_cost, *send_var);
            add_resource_cost(res, &net_cost, *recv_var);
        }
    }
}

/// Adds `(cost_field + DECOUPLING_PENALTY) * var` to each resource expression.
fn add_resource_cost(res: &mut ResourceExpressions, cost: &SarStats, var: Variable) {
    let cpu_temp = std::mem::take(&mut res.cpu);
    res.cpu = cpu_temp + (cost.cpu + DECOUPLING_PENALTY) * var;
    // let mem_temp = std::mem::take(&mut res.memory);
    // res.memory = mem_temp + cost.memory * var;
    // let net_temp = std::mem::take(&mut res.network);
    // res.network = net_temp + cost.network * var;
    // let io_temp = std::mem::take(&mut res.io);
    // res.io = io_temp + cost.io * var;
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
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.op.id.unwrap();
    let net_cost = network_cost_for_decoupling_op(op_id, op_counts, op_sizes, network_cost_table);

    // For each location,
    // send_var is 1 if the inner (at that location) sends to any Tee (including this op),
    // recv_var is 1 if the inner (at a different location) sends to any Tee (including this op)
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
                let res = resource_usages.get_mut(&loc).unwrap();
                add_resource_cost(res, &net_cost, send_var);
                add_resource_cost(res, &net_cost, recv_var);
                loc_to_vars.insert(loc, (send_var, recv_var));
            }
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

    add_op_resource_usage(node, &network_type, op_id_to_parents, decoupling_metadata);
    add_tick_constraint(node.metadata(), op_id_to_parents, decoupling_metadata);
}

fn solve(decoupling_metadata: &RefCell<DecoupleILPMetadata>) -> LpSolution {
    let DecoupleILPMetadata {
        variables,
        constraints,
        resource_usages,
        op_id_to_var,
        max_num_locations,
        field_specificity_penalty,
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

    let objective = highest_saturation
        + LEXICOGRAPHIC_EPSILON * sum_saturations
        + FIELD_SPECIFICITY_EPSILON * field_specificity_penalty.clone();

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
        field_specificity_penalty: Expression::default(),
    });
    let op_id_to_parents = op_id_to_parents(ir, Some(bottleneck), cycle_source_to_sink_parent);
    let inputs = all_inputs(ir, &decoupling_metadata.borrow().bottleneck);
    let idbs = nodes_dependent_on_inputs(
        ir,
        &decoupling_metadata.borrow().bottleneck,
        &inputs,
        &op_id_to_parents,
    );

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
    for (op_id, parents) in &op_id_to_parents {
        let DecoupleILPMetadata {
            op_id_to_var,
            variables,
            constraints,
            ..
        } = &mut *decoupling_metadata.borrow_mut();

        let mut same_output_loc = parents.clone();
        if !idbs.contains(op_id) {
            // No decoupling EDBs, otherwise broadcast cluster members might be decoupled from the broadcast itself, and the membership might not arrive in time for the broadcast
            same_output_loc.push(*op_id);
        }

        // Add parent constraints. All parents of an op must output to the same machine (be assigned the same var)
        add_equality_constr(
            &same_output_loc,
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
            idbs,
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

    let solution = solve(&decoupling_metadata);

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

        // DEBUG: chosen partition count and per-resource effective saturation for this location.
        let chosen_n = is_n_partitions
            .get(*loc)
            .and_then(|vars| vars.iter().position(|&v| solution.value(v).round() == 1.0))
            .unwrap_or(0);
        let per_resource_sat: Vec<f64> = res
            .iter()
            .map(|(expr, cap)| solution.eval(expr.clone()) / cap)
            .collect();
        let per_resource_usage: Vec<f64> = res
            .iter()
            .map(|(expr, _)| solution.eval(expr.clone()))
            .collect();
        println!(
            "ILP cost loc {}: chosen_n={}, max_sat={:.4}, effective_usage(cpu,mem,net,io)={:?}, sat={:?}",
            loc, chosen_n, max_sat, per_resource_usage, per_resource_sat
        );
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
    if let Some(partition_metadata) = partition_metadata {
        for loc in 0..max_num_locations {
            let num_relevant = solution
                .eval(
                    partition_metadata
                        .num_relevant_operators
                        .get(&loc)
                        .cloned()
                        .unwrap_or_default(),
                )
                .round() as i64;
            let num_partitionable = solution
                .eval(
                    partition_metadata
                        .partitionable_operators
                        .get(&loc)
                        .cloned()
                        .unwrap_or_default(),
                )
                .round() as i64;
            let num_persists = solution
                .eval(
                    partition_metadata
                        .num_persist_operators
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
        for (op_id, fields) in &partition_metadata.op_id_to_field_vars {
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
    for (loc, partitions) in is_n_partitions.iter().enumerate().take(max_num_locations) {
        for (n, &var) in partitions.iter().enumerate() {
            if solution.value(var).round() == 1.0 && n > 1 {
                result.num_partitions.insert(loc, n);
                break;
            }
        }
    }

    project_location_costs(
        ir,
        bottleneck,
        &op_counts,
        &op_sizes,
        &network_cost_table,
        &per_op_load,
        &result,
    );

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
        results.push(rewrite);
    }

    results
}

/// Calculate the total CPU usage using the calibrated network cost.
/// If this exceeds 100%, then our calibration assumes network costs too much.
/// If this is under 100%, then our calibration assumes network costs too little.
///
/// The return type is `Vec<Rewrite>` just to match the signature of `find_optimal_budget`
#[allow(dead_code)]
pub(crate) fn project_total_cpu(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    inputs: &IlpInputs,
    _cycle_source_to_sink_parent: &HashMap<usize, usize>,
) -> Vec<Rewrite> {
    let mut total_cpu = 0f64;

    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            let network_type = get_network_type(node, bottleneck);
            if network_type.is_none() && node.metadata().location_id.root() != bottleneck {
                return;
            }

            let op_id = node.op_metadata().id.unwrap();

            if let Some(network_type) = network_type {
                let size = inputs.op_output_sizes.get(&op_id).copied().unwrap_or(0);
                let net_cpu = |count: usize| {
                    if count > 0 && size > 0 {
                        inputs.network_cost_table.network_cost(count, size).cpu
                    } else {
                        0.0
                    }
                };

                // Send cardinality = cardinality of Network's parent. Will be larger than the Network's cardinality if it is a broadcast
                let send_count =
                    if matches!(network_type, NetworkType::Send | NetworkType::SendRecv) {
                        let input_id = node.input_metadata()[0].op.id.unwrap();
                        inputs.op_counts.get(&input_id).copied().unwrap_or(0)
                    } else {
                        0
                    };

                // Receive cardinality = cardinality of Network
                let recv_count =
                    if matches!(network_type, NetworkType::Recv | NetworkType::SendRecv) {
                        inputs.op_counts.get(&op_id).copied().unwrap_or(0)
                    } else {
                        0
                    };

                let cpu_cost = net_cpu(send_count) + net_cpu(recv_count);
                if cpu_cost > 0.0 {
                    println!(
                        "Network op {}: send_count={}, recv_count={}, size={}, cpu_cost={:.2}",
                        op_id, send_count, recv_count, size, cpu_cost
                    );
                    total_cpu += cpu_cost;
                }
            } else {
                if let Some(load) = inputs.per_op_load.get(&op_id) {
                    println!(
                        "Load for op {}: cpu={}, mem={}, net={}, io={}",
                        op_id, load.cpu, load.memory, load.network, load.io
                    );
                    total_cpu += load.cpu;
                }
            }
        },
    );

    println!("Projected total CPU usage at bottleneck: {:.2}%", total_cpu);
    std::process::exit(0);
}

/// Like `project_total_cpu`, but attributes cost to each location AFTER a rewrite.
///
/// Rules:
/// - An operator executes on the location its parent is assigned to (which equals its own
///   assigned location for non-decoupled ops). Its compute load is charged there.
/// - Networking is special-cased: an existing network charges its send cost (parent's
///   cardinality, larger for broadcasts) to the sender's location and its receive cost
///   (the network's own cardinality) to the receiver's location. A new decoupling edge
///   (`op_to_network`) charges the op's output cost to both the sender and receiver locations.
pub(crate) fn project_location_costs(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    op_counts: &HashMap<usize, usize>,
    op_sizes: &HashMap<usize, u64>,
    network_cost_table: &NetworkCostTable,
    per_op_load: &HashMap<usize, SarStats>,
    rewrite: &Rewrite,
) {
    let mut loc_cpu: HashMap<usize, f64> = HashMap::new();

    let net_cpu = |count: usize, size: u64| -> f64 {
        if count > 0 && size > 0 {
            network_cost_table.network_cost(count, size).cpu
        } else {
            0.0
        }
    };

    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            let Some(op_id) = node.op_metadata().id else {
                return;
            };
            // The op executes where its (first) parent is assigned; fall back to its own assignment.
            let parent_id = node.input_metadata().first().and_then(|m| m.op.id);
            let exec_loc = parent_id
                .and_then(|p| rewrite.op_to_loc.get(&p).copied())
                .or_else(|| rewrite.op_to_loc.get(&op_id).copied());

            let network_type = get_network_type(node, bottleneck);
            if let Some(network_type) = network_type {
                // Existing network: send cost on the sender, receive cost on the receiver.
                let size = op_sizes.get(&op_id).copied().unwrap_or(0);
                if matches!(network_type, NetworkType::Send | NetworkType::SendRecv) {
                    let send_count = parent_id
                        .and_then(|p| op_counts.get(&p).copied())
                        .unwrap_or(0);
                    let send_loc = parent_id
                        .and_then(|p| rewrite.op_to_loc.get(&p).copied())
                        .or(exec_loc);
                    if let Some(l) = send_loc {
                        *loc_cpu.entry(l).or_default() += net_cpu(send_count, size);
                    }
                }
                if matches!(network_type, NetworkType::Recv | NetworkType::SendRecv) {
                    let recv_count = op_counts.get(&op_id).copied().unwrap_or(0);
                    let recv_loc = rewrite.op_to_loc.get(&op_id).copied().or(exec_loc);
                    if let Some(l) = recv_loc {
                        *loc_cpu.entry(l).or_default() += net_cpu(recv_count, size);
                    }
                }
                return;
            }

            if node.metadata().location_id.root() != bottleneck {
                return;
            }

            // New decoupling edge on this op's output: charge serialization to both ends.
            if let Some(&(from_loc, to_loc)) = rewrite.op_to_network.get(&op_id) {
                let size = op_sizes.get(&op_id).copied().unwrap_or(0);
                let count = op_counts.get(&op_id).copied().unwrap_or(0);
                let c = net_cpu(count, size);
                *loc_cpu.entry(from_loc).or_default() += c;
                *loc_cpu.entry(to_loc).or_default() += c;
            }

            // Compute load, charged to the op's execution location.
            if let Some(load) = per_op_load.get(&op_id)
                && let Some(l) = exec_loc
            {
                *loc_cpu.entry(l).or_default() += load.cpu;
            }
        },
    );

    let mut locs: Vec<usize> = loc_cpu.keys().copied().collect();
    locs.sort_unstable();
    println!(
        "=== Predicted per-location CPU after rewrite ({} locations) ===",
        rewrite.num_locations()
    );
    for loc in locs {
        println!("  Location {}: predicted_cpu={:.2}%", loc, loc_cpu[&loc]);
    }
}
