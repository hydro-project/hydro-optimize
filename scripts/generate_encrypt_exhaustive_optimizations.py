#!/usr/bin/env python3
"""Generate exhaustive Encrypt decoupling/partitioning rewrites."""

from __future__ import annotations

import argparse
import itertools
import json
import re
from pathlib import Path


def load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def parse_id_file(path: Path) -> tuple[dict[int, int], dict[int, str]]:
    parents: dict[int, int] = {}
    kinds: dict[int, str] = {}
    node_re = re.compile(r"Some\((\d+)\) Node ([A-Za-z]+)")
    inputs_re = re.compile(r"Inputs: \[(.*)\]\s*$")

    with path.open() as f:
        for line in f:
            node_match = node_re.search(line)
            if node_match is None:
                continue

            op_id = int(node_match.group(1))
            kinds[op_id] = node_match.group(2)

            inputs_match = inputs_re.search(line)
            if inputs_match is None:
                continue
            input_ids = [int(raw) for raw in re.findall(r"Some\((\d+)\)", inputs_match.group(1))]
            if input_ids:
                parents[op_id] = input_ids[0]

    return parents, kinds


def op_sets_in_order(ops: list[int]):
    yield ()
    for size in range(1, len(ops) + 1):
        yield from itertools.combinations(ops, size)


def op_assignments_in_order(ops: list[int], locations: tuple[int, ...]):
    zero = {str(op): locations[0] for op in ops}
    yield zero

    for assignment in itertools.product(locations, repeat=len(ops)):
        op_to_loc = {str(op): loc for op, loc in zip(ops, assignment, strict=True)}
        if op_to_loc != zero:
            yield op_to_loc


def canonicalize(
    ops: list[int], op_to_loc: dict[str, int], partition_loc: int | None = None
) -> tuple[dict[str, int], int | None]:
    loc_map: dict[int, int] = {}

    def canonical_loc(loc: int) -> int:
        if loc not in loc_map:
            loc_map[loc] = len(loc_map)
        return loc_map[loc]

    canonical = {str(op): canonical_loc(op_to_loc[str(op)]) for op in ops}
    canonical_partition_loc = (
        None if partition_loc is None else canonical_loc(partition_loc)
    )
    return canonical, canonical_partition_loc


def candidate_key(
    ops: list[int], op_to_loc: dict[str, int], partition_loc: int | None = None
) -> tuple[tuple[int, ...], int | None]:
    canonical, canonical_partition_loc = canonicalize(ops, op_to_loc, partition_loc)
    return tuple(canonical[str(op)] for op in ops), canonical_partition_loc


def valid_syntactic_sugar_assignment(
    op_to_loc: dict[str, int], parents: dict[int, int], kinds: dict[int, str]
) -> bool:
    syntactic_sugar_kinds = {
        "Cast",
        "ObserveNonDet",
        "BeginAtomic",
        "EndAtomic",
        "Batch",
        "YieldConcat",
    }

    for op, kind in kinds.items():
        if kind not in syntactic_sugar_kinds:
            continue
        parent = parents.get(op)
        grandparent = parents.get(parent) if parent is not None else None
        if parent is None or grandparent is None:
            continue
        if str(parent) not in op_to_loc or str(grandparent) not in op_to_loc:
            continue
        if op_to_loc[str(parent)] != op_to_loc[str(grandparent)]:
            return False

    return True


def network_edges(op_to_loc: dict[str, int], parents: dict[int, int]) -> dict[str, list[int]]:
    edges: dict[str, list[int]] = {}
    int_locs = {int(op): loc for op, loc in op_to_loc.items()}
    for op, loc in int_locs.items():
        parent = parents.get(op)
        if parent not in int_locs:
            continue
        parent_loc = int_locs[parent]
        if parent_loc != loc:
            edges[str(op)] = [parent_loc, loc]
    return edges


def partitionable_locations(op_to_loc: dict[str, int], scan_op: int) -> list[int]:
    loc_to_ops: dict[int, set[int]] = {}
    for raw_op, loc in op_to_loc.items():
        loc_to_ops.setdefault(loc, set()).add(int(raw_op))

    return sorted(
        loc
        for loc, loc_ops in loc_to_ops.items()
        if loc_ops and scan_op not in loc_ops
    )


def candidate(
    *,
    name: str,
    template: dict,
    budget: int,
    op_to_loc: dict[str, int],
    parents: dict[int, int],
    scan_op: int,
    partition_loc: int | None,
) -> dict:
    partitionable = partitionable_locations(op_to_loc, scan_op)
    num_partitions = {} if partition_loc is None else {str(partition_loc): 2}

    return {
        "name": name,
        "original_location": template["original_location"],
        "cluster_size": template["cluster_size"],
        "budget": budget,
        "location_costs": {},
        "num_partitions": num_partitions,
        "op_to_loc": op_to_loc,
        "op_to_network": network_edges(op_to_loc, parents),
        "stateless_partitionable": partitionable,
        "field_partitionable": [],
        "partitionable": partitionable,
        "partition_field_choices": {},
    }


def assignment_name(op_to_loc: dict[str, int]) -> str:
    loc_to_ops: dict[int, list[str]] = {}
    for op, loc in op_to_loc.items():
        loc_to_ops.setdefault(loc, []).append(op)

    pieces = []
    for loc in sorted(loc_to_ops):
        pieces.append(f"loc{loc}_" + "_".join(sorted(loc_to_ops[loc], key=int)))
    return "__".join(pieces)


def machine_count(op_to_loc: dict[str, int], partition_loc: int | None) -> int:
    used_locs = set(op_to_loc.values())
    total = 0
    for loc in used_locs:
        total += 2 if loc == partition_loc else 1
    return total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--state-path",
        type=Path,
        default=Path("benchmark_results/Encrypt_optimization_state.json"),
    )
    parser.add_argument(
        "--id-path",
        type=Path,
        default=Path("benchmark_results/Encrypt_default_none/id.txt"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark_results/Encrypt_exhaustive_optimizations.json"),
    )
    parser.add_argument(
        "--scan-op",
        type=int,
        default=None,
        help="Override the Scan operator id if id.txt is unavailable or stale.",
    )
    args = parser.parse_args()

    state = load_json(args.state_path)
    rewrites = state["cluster_rewrites"]["server"]
    template = rewrites[0]
    ops = sorted(int(op) for op in template["op_to_loc"])

    parents, kinds = parse_id_file(args.id_path)
    scan_ops = [op for op in ops if kinds.get(op) == "Scan"]
    scan_op = args.scan_op if args.scan_op is not None else (scan_ops[0] if scan_ops else None)
    if scan_op is None:
        raise SystemExit("Could not identify Scan operator. Pass --scan-op.")

    budget_2 = []
    budget_3 = []
    budget_2_seen = set()
    for ones in op_sets_in_order(ops):
        ones_set = set(ones)
        raw_op_to_loc = {str(op): (1 if op in ones_set else 0) for op in ops}
        op_to_loc, _ = canonicalize(ops, raw_op_to_loc)
        if not valid_syntactic_sugar_assignment(op_to_loc, parents, kinds):
            continue
        key = candidate_key(ops, op_to_loc)
        if key in budget_2_seen:
            continue
        budget_2_seen.add(key)
        base_name = assignment_name(op_to_loc)

        budget_2.append(
            candidate(
                name=f"budget2_{base_name}",
                template=template,
                budget=2,
                op_to_loc=op_to_loc,
                parents=parents,
                scan_op=scan_op,
                partition_loc=None,
            )
        )

    budget_3_seen = set()
    for op_to_loc in op_assignments_in_order(ops, (0, 1, 2)):
        op_to_loc, _ = canonicalize(ops, op_to_loc)
        if not valid_syntactic_sugar_assignment(op_to_loc, parents, kinds):
            continue
        base_name = assignment_name(op_to_loc)
        used_locs = set(op_to_loc.values())

        key = candidate_key(ops, op_to_loc)
        if len(used_locs) == 3 and key not in budget_3_seen:
            budget_3_seen.add(key)
            budget_3.append(
                candidate(
                    name=f"budget3_{base_name}",
                    template=template,
                    budget=3,
                    op_to_loc=op_to_loc,
                    parents=parents,
                    scan_op=scan_op,
                    partition_loc=None,
                )
            )

        for loc in partitionable_locations(op_to_loc, scan_op):
            key = candidate_key(ops, op_to_loc, loc)
            if machine_count(op_to_loc, loc) <= 3 and key not in budget_3_seen:
                budget_3_seen.add(key)
                budget_3.append(
                    candidate(
                        name=f"budget3_{base_name}_partition_loc{loc}",
                        template=template,
                        budget=3,
                        op_to_loc=op_to_loc,
                        parents=parents,
                        scan_op=scan_op,
                        partition_loc=loc,
                    )
                )

    output = {
        "source_state": str(args.state_path),
        "source_id": str(args.id_path),
        "operators": ops,
        "scan_op": scan_op,
        "budget_2": budget_2,
        "budget_3": budget_3,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        f"Wrote {args.output} with "
        f"{len(budget_2)} budget_2 and {len(budget_3)} budget_3 candidates."
    )


if __name__ == "__main__":
    main()
