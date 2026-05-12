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
use crate::rewrites::{is_syntactic_sugar, op_id_to_parents};
use good_lp::solvers::highs::HighsSolution;
use good_lp::{
    Constraint, Expression, ProblemVariables, Solution, SolverModel, Variable, constraint, highs,
    variable, variables,
};
use hydro_lang::compile::builder::ClockId;

use hydro_lang::compile::ir::{
    HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot, traverse_dfir,
};
use hydro_lang::location::dynamic::LocationId;

use super::rewrites::{NetworkType, get_network_type};

// Penalty for decoupling regardless of cardinality (to prevent decoupling low cardinality operators)
const DECOUPLING_PENALTY: f64 = 0.0001;
const IMPROVEMENT_THRESHOLD: f64 = 0.01; // Minimum improvement to keep increasing budget

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
    pub(crate) cluster_size: usize,
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
                let var = variables.add(variable().binary());
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

    if let LocationId::Tick(tick_id, _) = metadata.location_id {
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
        cluster_size,
        variables,
        constraints,
        max_num_locations: num_locations,
        resource_usages,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.id.unwrap();
    let op_load = if let Some(load) = per_op_load.get(&op_id) {
        *load
    } else if network_type.is_some() {
        // Network ops aren't in per_op_load; derive cost from calibration table
        // Use parent's count (total messages sent) and the network op's own size
        let parent_id = op_id_to_parents
            .get(&op_id)
            .and_then(|p| p.first().copied());
        let count = op_counts
            .get(&parent_id.unwrap_or(op_id))
            .copied()
            .unwrap_or(0);
        let size = op_sizes.get(&op_id).copied().unwrap_or(0);
        if count > 0 && size > 0 {
            network_cost_table.network_cost(count, size, *cluster_size)
        } else {
            return;
        }
    } else {
        return;
    };
    if op_load.cpu <= 0.0 && op_load.memory <= 0.0 && op_load.network <= 0.0 && op_load.io <= 0.0 {
        return;
    }

    // Operators are run on the machine that their parents send to.
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
        _ => {}
    }
    // Special case for network receives: their cost (deserialization) is paid by the receiver.
    match network_type {
        Some(NetworkType::Recv) | Some(NetworkType::SendRecv) => {
            let op_vars =
                var_from_op_id(op_id, *num_locations, op_id_to_var, variables, constraints);
            for (loc, parent_var) in op_vars {
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

/// Resource cost of sending this operator's output over the network (for new decoupled networks).
/// Uses num_sockets=1 since a newly decoupled network has a single source.
/// Returns zero SarStats if byte size was never recorded.
fn network_cost_for_decoupling_op(
    op_id: usize,
    op_counts: &HashMap<usize, usize>,
    op_sizes: &HashMap<usize, u64>,
    network_cost_table: &NetworkCostTable,
) -> SarStats {
    let cardinality = op_counts.get(&op_id).copied().unwrap_or(0);
    match op_sizes.get(&op_id) {
        Some(&bytes) => network_cost_table.network_cost(cardinality, bytes, 1),
        None => SarStats::default(),
    }
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
    let mem_temp = std::mem::take(&mut res.memory);
    res.memory = mem_temp + cost.memory * var;
    let net_temp = std::mem::take(&mut res.network);
    res.network = net_temp + cost.network * var;
    let io_temp = std::mem::take(&mut res.io);
    res.io = io_temp + cost.io * var;
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
                let send_var = variables.add(variable().binary());
                let recv_var = variables.add(variable().binary());
                let res = resource_usages.get_mut(&loc).unwrap();
                add_resource_cost(res, &net_cost, send_var);
                add_resource_cost(res, &net_cost, recv_var);
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
    } else if is_syntactic_sugar(node) {
        // Syntactic sugar nodes (Cast, Batch, YieldConcat, etc.) must stay with their parent
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

fn solve(decoupling_metadata: &RefCell<DecoupleILPMetadata>) -> (HighsSolution, f64) {
    let DecoupleILPMetadata {
        variables,
        constraints,
        resource_usages,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let mut vars = std::mem::take(variables);
    let mut constrs = std::mem::take(constraints);

    // Minimize the highest saturation across all locations and resources
    let highest_saturation = vars.add_variable();
    for res in resource_usages.values() {
        for (usage, capacity) in res.iter() {
            constrs.push(constraint!(
                highest_saturation >= usage.clone() * (1.0 / capacity)
            ));
        }
    }

    println!(
        "  Solving ILP: {} vars, {} constraints",
        vars.len(),
        constrs.len()
    );
    let problem = vars.minimise(highest_saturation);
    let solve_start = Instant::now();
    let solution = highs(problem).with_all(constrs).solve().unwrap();
    println!("  Solver finished in {:.2?}", solve_start.elapsed());

    let mut max_cost = 0.0f64;
    for (loc, res) in resource_usages {
        for (usage, capacity) in res.iter() {
            let saturation = solution.eval(usage.clone()) / capacity;
            if saturation > 0.01 {
                println!(
                    "  Location {} saturation: {:.4} ({:.1}%)",
                    loc,
                    saturation,
                    saturation * 100.0
                );
            }
            max_cost = max_cost.max(saturation);
        }
    }

    (solution, max_cost)
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
        ..
    } = inputs;

    let decoupling_metadata = RefCell::new(DecoupleILPMetadata {
        bottleneck: bottleneck.clone(),
        op_counts,
        op_sizes,
        network_cost_table,
        per_op_load,
        cluster_size,
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
    });
    let op_id_to_parents = op_id_to_parents(ir, Some(bottleneck), cycle_source_to_sink_parent);

    traverse_dfir(
        ir,
        |root, _| {
            decouple_analysis_root(root, &op_id_to_parents, &decoupling_metadata);
        },
        |node, next_op_id| {
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

    let (solution, _max_cost) = solve(&decoupling_metadata);

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

        // Place sources, network recv/sendrecv, and non-network ops.
        // Do NOT place network send-only nodes (they live on other clusters).
        if !network_type.is_some_and(|t| *t == NetworkType::Send) {
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

    // Compute actual per-location costs from op assignments
    let metadata = decoupling_metadata.borrow();
    let mut actual_costs: HashMap<usize, SarStats> = HashMap::new();

    // Op costs (from per_op_load + network ops via calibration)
    for &op_id in op_id_to_parents.keys() {
        let Some(op_vars) = metadata.op_id_to_var.get(&op_id) else {
            continue;
        };
        let loc = op_loc(op_vars, &solution);

        // Determine cost: per_op_load or calibration for network ops
        let cost = if let Some(load) = metadata.per_op_load.get(&op_id) {
            *load
        } else if metadata.network_ids.contains_key(&op_id) {
            let parent_id = op_id_to_parents
                .get(&op_id)
                .and_then(|p| p.first().copied())
                .unwrap_or(op_id);
            let count = metadata.op_counts.get(&parent_id).copied().unwrap_or(0);
            let size = metadata.op_sizes.get(&op_id).copied().unwrap_or(0);
            if count > 0 && size > 0 {
                metadata.network_cost_table.network_cost(count, size, metadata.cluster_size)
            } else {
                continue;
            }
        } else {
            continue;
        };

        actual_costs.entry(loc).or_default().add(cost);
    }

    // New network boundary costs (num_sockets=1 for newly created networks)
    for (&op_id, &(src_loc, dst_loc)) in &result.op_to_network {
        let count = metadata.op_counts.get(&op_id).copied().unwrap_or(0);
        let size = metadata.op_sizes.get(&op_id).copied().unwrap_or(0);
        if count > 0 && size > 0 {
            let cost = metadata.network_cost_table.network_cost(count, size, 1);
            actual_costs.entry(src_loc).or_default().add(cost);
            actual_costs.entry(dst_loc).or_default().add(cost);
        }
    }

    println!("  Actual per-location costs:");
    for loc in 0..max_num_locations {
        let cost = actual_costs.get(&loc).copied().unwrap_or_default();
        let p = result.num_partitions.get(&loc).copied().unwrap_or(1);
        if cost.cpu > 0.0 {
            println!(
                "    Loc {}: cpu={:.1}% (effective={:.1}%), net={:.0} B/s, p={}",
                loc,
                cost.cpu,
                cost.cpu / p as f64,
                cost.network,
                p
            );
        }
    }

    result
}

/// Diagnostic: estimates the total resource cost on the bottleneck location assuming
/// no decoupling (everything stays on the bottleneck). Mirrors the per-op cost
/// derivation in [`add_op_resource_usage`]: non-network ops draw from `per_op_load`,
/// network ops draw from the calibration table. Send contributions are charged to the
/// sender (bottleneck when the op's parent is on the bottleneck), Recv contributions
/// to the receiver, and SendRecv to both.
///
/// Prints the per-op breakdown (sorted by CPU contribution) and a total. Useful for
/// sanity-checking that the calibration + perf inputs sum to ~100% CPU when the
/// bottleneck is saturated.
/// TODO: Clean up this function
pub fn estimate_bottleneck_cost(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    inputs: &IlpInputs,
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
    clusters: &crate::deploy_and_analyze::ReusableClusters,
) -> SarStats {
    let IlpInputs {
        op_counts,
        op_output_sizes: op_sizes,
        network_cost_table,
        per_op_load,
        ..
    } = inputs;

    let op_parents = op_id_to_parents(ir, Some(bottleneck), cycle_source_to_sink_parent);
    // Unfiltered parent map so we can walk to the sender-side parent of a Recv network op
    // (whose parent lives on the sender cluster, not the bottleneck).
    let op_parents_unfiltered = op_id_to_parents(ir, None, cycle_source_to_sink_parent);

    // Identify network ops relative to the bottleneck, exactly like decouple_analysis does.
    // Also record the peer cluster size for each network op (used as num_sockets).
    let network_ids: RefCell<HashMap<usize, NetworkType>> = RefCell::new(HashMap::new());
    let network_sockets: RefCell<HashMap<usize, usize>> = RefCell::new(HashMap::new());
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            if let Some(id) = node.metadata().op.id
                && let Some(nt) = get_network_type(node, bottleneck)
            {
                // Determine peer cluster size: for Send, peer is the receiver (node's location).
                // For Recv, peer is the sender (input's location).
                let peer_loc = match &nt {
                    NetworkType::Send | NetworkType::SendRecv => {
                        // Bottleneck is sender; peer is receiver = node's own location
                        node.metadata().location_id.root().clone()
                    }
                    NetworkType::Recv => {
                        // Bottleneck is receiver; peer is sender = input's location
                        if let HydroNode::Network { input, .. } = node {
                            input.metadata().location_id.root().clone()
                        } else {
                            bottleneck.root().clone()
                        }
                    }
                };
                let peer_size = clusters
                    .location_name_and_num(&peer_loc)
                    .map(|(_, n)| n)
                    .unwrap_or(1);
                network_ids.borrow_mut().insert(id, nt);
                network_sockets.borrow_mut().insert(id, peer_size);
            }
        },
    );
    let network_ids = network_ids.into_inner();
    let network_sockets = network_sockets.into_inner();

    // Resolve op_id → cost, replicating add_op_resource_usage's op_load lookup.
    // `use_parent_cardinality`:
    //   - true  → use the parent's op_count (sender-side: what the parent produced)
    //   - false → use the op's own op_count (receiver-side: what actually arrived here,
    //             which may be less if the sender demuxes to multiple recipients)
    let lookup_op_load = |op_id: usize, is_network: bool, use_parent_cardinality: bool| -> Option<SarStats> {
        if let Some(load) = per_op_load.get(&op_id) {
            return Some(*load);
        }
        if is_network {
            let count_key = if use_parent_cardinality {
                // For senders the bottleneck's parent is on the bottleneck, so either
                // filtered or unfiltered gives the same answer. For receivers the parent
                // lives on the sender cluster, so we must use the unfiltered map.
                op_parents_unfiltered
                    .get(&op_id)
                    .and_then(|p| p.first().copied())
                    .unwrap_or(op_id)
            } else {
                op_id
            };
            let count = op_counts.get(&count_key).copied().unwrap_or(0);
            let size = op_sizes.get(&op_id).copied().unwrap_or(0);
            if count > 0 && size > 0 {
                let num_sockets = network_sockets.get(&op_id).copied().unwrap_or(1);
                return Some(network_cost_table.network_cost(count, size, num_sockets));
            }
        }
        None
    };

    let mut total = SarStats::default();
    // (op_id, role label, contribution)
    let mut contributions: Vec<(usize, &'static str, SarStats)> = Vec::new();

    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            let Some(op_id) = node.metadata().op.id else {
                return;
            };

            let network_type = network_ids.get(&op_id).cloned();
            let on_bottleneck_non_network = network_type.is_none()
                && node.metadata().location_id.root() == bottleneck.root();

            // Skip anything unrelated to the bottleneck — matches the early return in
            // decouple_analysis_node.
            if network_type.is_none() && !on_bottleneck_non_network {
                return;
            }

            // Sender-side contribution: non-network ops (executed at parent's loc == bottleneck)
            // and Send / SendRecv network boundaries where the bottleneck is the sender.
            // Use parent's cardinality (what the sender produced).
            let sender_charged = matches!(
                network_type,
                None | Some(NetworkType::Send) | Some(NetworkType::SendRecv)
            );
            if sender_charged
                && let Some(op_load) = lookup_op_load(op_id, network_type.is_some(), true)
                && !(op_load.cpu <= 0.0
                    && op_load.memory <= 0.0
                    && op_load.network <= 0.0
                    && op_load.io <= 0.0)
            {
                if network_type.is_some() {
                    let parent_id = op_parents_unfiltered
                        .get(&op_id)
                        .and_then(|p| p.first().copied())
                        .unwrap_or(op_id);
                    let parent_count = op_counts.get(&parent_id).copied().unwrap_or(0);
                    let own_count = op_counts.get(&op_id).copied().unwrap_or(0);
                    let size = op_sizes.get(&op_id).copied().unwrap_or(0);
                    eprintln!(
                        "  [send-debug] op={} parent={} parent_count={} own_count={} size={} sockets={}",
                        op_id, parent_id, parent_count, own_count, size,
                        network_sockets.get(&op_id).copied().unwrap_or(0)
                    );
                }
                let label = match network_type {
                    None => "op",
                    Some(NetworkType::Send) => "net-send",
                    Some(NetworkType::SendRecv) => "net-sendrecv(send)",
                    _ => unreachable!(),
                };
                contributions.push((op_id, label, op_load));
                total.add(op_load);
            }

            // Receiver-side contribution: Recv / SendRecv network boundaries where the
            // bottleneck is the receiver (deserialization cost). Use the op's own
            // cardinality, since the sender may demux across multiple recipients.
            let receiver_charged = matches!(
                network_type,
                Some(NetworkType::Recv) | Some(NetworkType::SendRecv)
            );
            if receiver_charged {
                // Debug: compare parent (sender-side) vs own (receiver-side) cardinality
                let parent_id = op_parents_unfiltered
                    .get(&op_id)
                    .and_then(|p| p.first().copied())
                    .unwrap_or(op_id);
                let parent_count = op_counts.get(&parent_id).copied().unwrap_or(0);
                let own_count = op_counts.get(&op_id).copied().unwrap_or(0);
                eprintln!(
                    "  [recv-debug] op={} parent={} parent_count={} own_count={} sockets={}",
                    op_id, parent_id, parent_count, own_count,
                    network_sockets.get(&op_id).copied().unwrap_or(0)
                );

                if let Some(op_load) = lookup_op_load(op_id, true, false)
                    && !(op_load.cpu <= 0.0
                        && op_load.memory <= 0.0
                        && op_load.network <= 0.0
                        && op_load.io <= 0.0)
                {
                    let label = match network_type {
                        Some(NetworkType::Recv) => "net-recv",
                        Some(NetworkType::SendRecv) => "net-sendrecv(recv)",
                        _ => unreachable!(),
                    };
                    contributions.push((op_id, label, op_load));
                    total.add(op_load);
                }
            }
        },
    );

    contributions.sort_by(|a, b| {
        b.2.cpu
            .partial_cmp(&a.2.cpu)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    println!(
        "Estimated cost contributions on bottleneck {:?} (top 40 by CPU):",
        bottleneck
    );
    for (op_id, kind, cost) in contributions.iter().take(40) {
        if cost.cpu <= 0.0 {
            continue;
        }
        println!(
            "  op {:>4} [{}]: cpu={:.3}%  mem={:.2}%  net={:.0} B/s  io={:.3} tps",
            op_id, kind, cost.cpu, cost.memory, cost.network, cost.io
        );
    }
    let shown = contributions.len().min(40);
    let remaining = contributions.len().saturating_sub(40);
    if remaining > 0 {
        let remaining_cpu: f64 = contributions.iter().skip(shown).map(|(_, _, c)| c.cpu).sum();
        println!(
            "  ... {} more contributions omitted (sum cpu={:.3}%)",
            remaining, remaining_cpu
        );
    }
    println!("Total estimated cost on bottleneck {:?}:", bottleneck);
    println!("  CPU:     {:.2}%", total.cpu);
    println!("  Memory:  {:.2}%", total.memory);
    println!("  Network: {:.0} B/s", total.network);
    println!("  IO:      {:.2} tps", total.io);

    total
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
            "  budget={}: max_saturation={:.4}, {} locations, solved in {:.2?}",
            budget,
            cost,
            rewrite.num_locations(),
            start.elapsed()
        );

        let improved = results
            .last()
            .is_none_or(|prev: &Rewrite| prev.max_cost() - cost > IMPROVEMENT_THRESHOLD);
        results.push(rewrite);

        if improved {
            no_improvement = 0;
        } else {
            no_improvement += 1;
            if no_improvement >= MAX_OPTIMIZATION_ITERATIONS_PAST_NO_IMPROVEMENT {
                break;
            }
        }
    }

    results
}
