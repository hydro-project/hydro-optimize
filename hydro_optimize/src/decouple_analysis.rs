use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::parse_results::{NetworkCostTable, OperatorMetrics};
use crate::partition_ilp_analysis::partition_ilp_analysis;
use crate::partition_syn_analysis::StructOrTupleIndex;
use crate::rewrites::op_id_to_parents;
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
    pub(crate) operator_metrics: OperatorMetrics,
    pub(crate) network_cost_table: NetworkCostTable,
    pub(crate) num_locations: usize,
    // Model variables to construct final cost function
    pub(crate) variables: ProblemVariables,
    pub(crate) constraints: Vec<Constraint>,
    pub(crate) cpu_usages: HashMap<usize, Expression>, // location_id: cpu_usage
    pub(crate) op_id_to_var: HashMap<usize, HashMap<usize, Variable>>, // op_id: (location_id: var). Var = 1 for a location if the op is assigned to that location
    prev_op_parent_with_tick: HashMap<ClockId, usize>, // tick_id: last op_id with that tick_id
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
        num_locations,
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

fn add_cpu_usage(
    metadata: &HydroIrOpMetadata,
    network_type: &Option<NetworkType>,
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        operator_metrics,
        variables,
        constraints,
        num_locations,
        cpu_usages,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.id.unwrap();
    let op_cost = operator_metrics
        .op_to_cost
        .get(&op_id)
        .map(|c| c.cpu)
        .unwrap_or(0.0);

    // Calculate total CPU usage on each node (before overheads). Operators are run on the machine that their parents send to.
    match network_type {
        Some(NetworkType::Send) | Some(NetworkType::SendRecv) | None => {
            if let Some(parents) = op_id_to_parents.get(&op_id) {
                // All parents must be assigned the same var (by constraints above), so it suffices to check one
                if let Some(first_parent) = parents.first() {
                    let parent_vars = var_from_op_id(
                        *first_parent,
                        *num_locations,
                        op_id_to_var,
                        variables,
                        constraints,
                    );
                    if op_cost > 0.0 {
                        for (loc, parent_var) in parent_vars {
                            let node_cpu_usage = cpu_usages.get_mut(&loc).unwrap();
                            let node_cpu_usage_temp = std::mem::take(node_cpu_usage);
                            *node_cpu_usage = node_cpu_usage_temp + op_cost * parent_var;
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
            if op_cost > 0.0 {
                for (loc, parent_var) in op_vars {
                    let node_cpu_usage = cpu_usages.get_mut(&loc).unwrap();
                    let node_cpu_usage_temp = std::mem::take(node_cpu_usage);
                    *node_cpu_usage = node_cpu_usage_temp + op_cost * parent_var;
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
    op_id_to_parents: &HashMap<usize, Vec<usize>>,
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
) {
    let DecoupleILPMetadata {
        operator_metrics,
        network_cost_table,
        variables,
        constraints,
        num_locations,
        cpu_usages,
        op_id_to_var,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let metadata = node.metadata();
    let op_id = metadata.op.id.unwrap();
    let cardinality = operator_metrics.op_to_count.get(&op_id).copied().unwrap_or(1);
    let output_bytes = operator_metrics.op_to_output_bytes.get(&op_id).copied().unwrap_or(64);
    let net_cost = network_cost_table.network_cost(cardinality, output_bytes).cpu;

    if let Some(parents) = op_id_to_parents.get(&op_id) {
        // All parents must be assigned the same var (by constraints above), so it suffices to check one
        if let Some(parent) = parents.first() {
            let decouple_vars = add_decouple_vars(
                *num_locations,
                variables,
                constraints,
                *parent,
                op_id,
                op_id_to_var,
            );
            for loc in 0..*num_locations {
                let cpu_usage = cpu_usages.get_mut(&loc).unwrap();
                let (send_var, recv_var) = decouple_vars.get(&loc).unwrap();
                let cpu_usage_temp = std::mem::take(cpu_usage);
                *cpu_usage = cpu_usage_temp
                    + (net_cost + DECOUPLING_PENALTY) * *send_var
                    + (net_cost + DECOUPLING_PENALTY) * *recv_var;
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
        operator_metrics,
        network_cost_table,
        variables,
        constraints,
        num_locations,
        cpu_usages,
        op_id_to_var,
        tee_inner_to_decoupled_vars,
        ..
    } = &mut *decoupling_metadata.borrow_mut();

    let op_id = metadata.op.id.unwrap();
    let cardinality = operator_metrics.op_to_count.get(&op_id).copied().unwrap_or(1);
    let output_bytes = operator_metrics.op_to_output_bytes.get(&op_id).copied().unwrap_or(64);
    let net_cost = network_cost_table.network_cost(cardinality, output_bytes).cpu;

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
                    + (net_cost + DECOUPLING_PENALTY) * send_var
                    + (net_cost + DECOUPLING_PENALTY) * recv_var;

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
            num_locations,
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
        add_decoupling_overhead(node, &network_type, op_id_to_parents, decoupling_metadata);
    }

    add_cpu_usage(
        node.op_metadata(),
        &network_type,
        op_id_to_parents,
        decoupling_metadata,
    );
    add_tick_constraint(node.metadata(), op_id_to_parents, decoupling_metadata);
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

/// Budget for a single decouple/partition analysis run.
#[derive(Clone, Debug)]
pub struct DecouplePartitionConfig {
    /// Number of distinct locations operators can be assigned to (≥ 2).
    pub num_locations: usize,
    /// Number of partitions per partitionable location (0 = no partitioning).
    pub num_partitions: usize,
    /// Whether to consider partitioning in the ILP.
    pub consider_partitioning: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PossibleRewrite {
    /// op_id → its new location (where its output streams to)
    pub op_to_loc: HashMap<usize, usize>,
    /// op_id → (src_loc, dst_loc). A new network should be inserted before this op
    /// from src_loc to dst_loc. The network's location is dst_loc.
    pub op_to_network: HashMap<usize, (usize, usize)>,
    /// Location indices that can be arbitrarily partitioned because it does not persist.
    pub stateless_partitionable: HashSet<usize>,
    /// Location indices that are partitionable via a field
    pub field_partitionable: HashSet<usize>,
    pub partitionable: HashSet<usize>, // Union of stateless_partitionable and field_partitionable
    /// For each op, what field it should be partitioned on
    pub partition_field_choices: HashMap<usize, StructOrTupleIndex>,
}

impl PossibleRewrite {
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

    pub fn add_network(&mut self, src: usize, dst: usize, op_id: usize) {
        self.op_to_network.insert(op_id, (src, dst));
    }
}

pub(crate) fn decouple_analysis(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    operator_metrics: OperatorMetrics,
    network_cost_table: NetworkCostTable,
    config: &DecouplePartitionConfig,
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
) -> PossibleRewrite {
    let num_locations = config.num_locations;
    if num_locations < 2 {
        panic!("Must decouple to at least 2 locations (original location, decoupled location)");
    }

    let decoupling_metadata = RefCell::new(DecoupleILPMetadata {
        bottleneck: bottleneck.clone(),
        operator_metrics,
        network_cost_table,
        num_locations,
        variables: variables! {},
        constraints: vec![],
        cpu_usages: HashMap::from_iter(
            (0..num_locations).map(|loc_id| (loc_id, Expression::default())),
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
        add_equality_constr(parents, num_locations, op_id_to_var, variables, constraints);
    }

    // Consider partitioning after all variables for decoupling have been created
    let partition_metadata = if config.consider_partitioning {
        Some(partition_ilp_analysis(
            ir,
            &op_id_to_parents,
            &decoupling_metadata,
        ))
    } else {
        None
    };

    let solution = solve(&decoupling_metadata);

    // Build the DecoupleDecision by separating placement ops from network-insertion ops.
    let DecoupleILPMetadata {
        op_id_to_var,
        network_ids,
        ..
    } = &*decoupling_metadata.borrow();

    let mut result = PossibleRewrite::default();
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
                result.add_network(parent_loc, op_location, op_id);
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
        for loc in 0..num_locations {
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

    result
}

/// Result of a single configuration trial in the machine budget search.
#[derive(Debug, Clone)]
pub struct ConfigResult {
    pub config: DecouplePartitionConfig,
    pub rewrite: PossibleRewrite,
    /// Maximum resource usage across all locations (the bottleneck cost).
    pub max_cost: f64,
}

/// Evaluates the max CPU cost across locations for a given solution.
fn evaluate_max_cost(
    decoupling_metadata: &RefCell<DecoupleILPMetadata>,
    solution: &HighsSolution,
) -> f64 {
    let metadata = decoupling_metadata.borrow();
    let mut max_cost = 0.0f64;
    for (_loc, expr) in &metadata.cpu_usages {
        let cost = solution.eval(expr.clone());
        max_cost = max_cost.max(cost);
    }
    max_cost
}

/// Tries increasing machine budgets (2, 3, ...) for the given bottleneck,
/// enumerating decouple-only and decouple+partition configurations at each level.
/// Stops when the min resource usage across nodes no longer decreases.
/// Returns all configurations sorted by max_cost ascending.
pub fn find_optimal_budget(
    ir: &mut [HydroRoot],
    bottleneck: &LocationId,
    operator_metrics: &OperatorMetrics,
    network_cost_table: &NetworkCostTable,
    cycle_source_to_sink_parent: &HashMap<usize, usize>,
    max_machines: usize,
) -> Vec<ConfigResult> {
    let mut results = Vec::new();
    let mut prev_best_cost = f64::MAX;

    for total_machines in 2..=max_machines {
        // Configuration 1: pure decoupling (all machines as separate locations)
        let decouple_config = DecouplePartitionConfig {
            num_locations: total_machines,
            num_partitions: 0,
            consider_partitioning: false,
        };
        let rewrite = decouple_analysis(
            ir,
            bottleneck,
            operator_metrics.clone(),
            network_cost_table.clone(),
            &decouple_config,
            cycle_source_to_sink_parent,
        );
        // TODO: evaluate_max_cost requires access to the solved ILP state;
        // for now, store the rewrite and let the caller evaluate externally.
        results.push(ConfigResult {
            config: decouple_config,
            rewrite,
            max_cost: 0.0, // placeholder
        });

        // Configuration 2+: decouple some, partition the rest
        // With N machines: try (N-k) decoupled + k partitions for k in 1..N
        for num_partitions in 2..=total_machines {
            let num_decoupled = total_machines / num_partitions;
            if num_decoupled < 1 {
                continue;
            }
            let config = DecouplePartitionConfig {
                num_locations: num_decoupled.max(2),
                num_partitions,
                consider_partitioning: true,
            };
            let rewrite = decouple_analysis(
                ir,
                bottleneck,
                operator_metrics.clone(),
                network_cost_table.clone(),
                &config,
                cycle_source_to_sink_parent,
            );
            results.push(ConfigResult {
                config,
                rewrite,
                max_cost: 0.0, // placeholder
            });
        }
    }

    results
}
