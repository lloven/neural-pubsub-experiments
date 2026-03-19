"""Phase A.5: Placement algorithm quality benchmark.

For small topologies where brute-force optimal placement is feasible,
compare the algorithm's placement cost against the true minimum.
Reports the optimality gap (algorithm_cost / optimal_cost - 1).

No Docker, no HTTP, pure computation.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Optional

import pytest

from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
    _placement_cost,
    check_feasibility,
    find_placement,
)
from src.pipeline.dag import Edge, PipelineDAG, Stage
from src.pipeline.patterns import funnel_pipeline, map_pipeline


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCENARIO_NAMES = ["homogeneous", "heterogeneous", "funnel", "slice_constrained", "cross_domain"]


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class PlacementResult:
    """Summary of a placement quality comparison."""

    topology: str
    pipeline_type: str
    algorithm_cost: float
    optimal_cost: float
    gap_ratio: float
    constraint_violations: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node(
    nid: str,
    domain: str = "d1",
    sl: str = "eMBB",
    capacity: float = 1.0,
) -> ExecutionUnit:
    return ExecutionUnit(
        node_id=nid, domain_id=domain, slice_id=sl, capacity=capacity
    )


def _full_latency_matrix(
    node_ids: list[str],
    default: float = 2.0,
    overrides: Optional[dict[tuple[str, str], float]] = None,
) -> dict[tuple[str, str], float]:
    """Build a symmetric latency matrix for all pairs."""
    matrix: dict[tuple[str, str], float] = {}
    for i, a in enumerate(node_ids):
        for b in node_ids[i + 1 :]:
            matrix[(a, b)] = default
    if overrides:
        matrix.update(overrides)
    return matrix


def _brute_force_optimal(
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
) -> tuple[float, dict[str, str]]:
    """Enumerate all placements, return (cost, placement) for the cheapest feasible one."""
    stage_ids = list(dag.stages.keys())
    node_ids = [n.node_id for n in topology.nodes]
    n_stages = len(stage_ids)

    best_cost = float("inf")
    best_placement: dict[str, str] = {}

    for combo in itertools.product(node_ids, repeat=n_stages):
        placement = dict(zip(stage_ids, combo))
        feasible, violations = check_feasibility(placement, dag, topology, governance)
        if not feasible:
            continue
        cost = _placement_cost(placement, dag, topology, alpha, beta, gamma)
        if cost < best_cost:
            best_cost = cost
            best_placement = placement

    return best_cost, best_placement


def _assert_quality(result: PlacementResult, max_gap: float = 0.10) -> None:
    """Assert that a placement result has no violations and is within the gap tolerance."""
    assert result.constraint_violations == 0, (
        f"{result.topology}: algorithm produced {result.constraint_violations} "
        f"constraint violation(s)"
    )
    assert result.gap_ratio <= max_gap, (
        f"{result.topology}: gap {result.gap_ratio:.4f} exceeds "
        f"{max_gap:.0%} tolerance "
        f"(algo={result.algorithm_cost:.4f}, opt={result.optimal_cost:.4f})"
    )


def _evaluate(
    label: str,
    pipeline_type: str,
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
) -> PlacementResult:
    """Run the algorithm and brute-force, compare."""
    algo_placement = find_placement(dag, topology, governance, alpha, beta, gamma)
    algo_cost = _placement_cost(algo_placement, dag, topology, alpha, beta, gamma)
    _, algo_violations = check_feasibility(algo_placement, dag, topology, governance)

    opt_cost, _ = _brute_force_optimal(dag, topology, governance, alpha, beta, gamma)

    gap = (algo_cost / opt_cost - 1.0) if opt_cost > 1e-12 else 0.0

    return PlacementResult(
        topology=label,
        pipeline_type=pipeline_type,
        algorithm_cost=algo_cost,
        optimal_cost=opt_cost,
        gap_ratio=gap,
        constraint_violations=len(algo_violations),
    )


# ---------------------------------------------------------------------------
# Scenario builder
# ---------------------------------------------------------------------------

_SCENARIOS = [
    pytest.param(name, id=name) for name in SCENARIO_NAMES
]


def _build_scenario(name: str):
    """Build (dag, topology, governance, label, pipeline_type) for a named scenario."""
    if name == "homogeneous":
        nodes = [_node(f"n{i}") for i in range(3)]
        nids = [n.node_id for n in nodes]
        topo = NetworkTopology(nodes=nodes, latency_matrix=_full_latency_matrix(nids, 1.0))
        dag = map_pipeline("transform", 3, computational_demand=0.3, latency_bound=10.0)
        return dag, topo, GovernancePolicy(), "homogeneous_3x3", "linear"

    if name == "heterogeneous":
        nodes = [
            ExecutionUnit("n0", "d1", "eMBB", capacity=2.0),
            ExecutionUnit("n1", "d1", "eMBB", capacity=1.0),
            ExecutionUnit("n2", "d1", "eMBB", capacity=0.5),
        ]
        lats = {("n0", "n1"): 1.0, ("n0", "n2"): 3.0, ("n1", "n2"): 2.0}
        topo = NetworkTopology(nodes=nodes, latency_matrix=lats)
        dag = map_pipeline("transform", 3, computational_demand=0.4, latency_bound=10.0)
        return dag, topo, GovernancePolicy(), "heterogeneous_3x3", "linear"

    if name == "funnel":
        nodes = [ExecutionUnit(f"n{i}", "d1", "eMBB", capacity=2.0) for i in range(5)]
        nids = [n.node_id for n in nodes]
        lats = {}
        for i, a in enumerate(nids):
            for j, b in enumerate(nids):
                if j > i:
                    lats[(a, b)] = 1.0 + abs(i - j) * 0.5
        topo = NetworkTopology(nodes=nodes, latency_matrix=lats)
        dag = funnel_pipeline(n_inputs=3, latency_bound_in=10.0, latency_bound_out=10.0)
        return dag, topo, GovernancePolicy(), "funnel_5x5", "funnel"

    if name == "slice_constrained":
        nodes = [
            ExecutionUnit("n0", "d1", "URLLC", capacity=1.0),
            ExecutionUnit("n1", "d1", "URLLC", capacity=1.0),
            ExecutionUnit("n2", "d1", "eMBB", capacity=1.5),
            ExecutionUnit("n3", "d1", "eMBB", capacity=1.5),
            ExecutionUnit("n4", "d1", "eMBB", capacity=1.0),
        ]
        nids = [n.node_id for n in nodes]
        topo = NetworkTopology(nodes=nodes, latency_matrix=_full_latency_matrix(nids, 2.0))
        dag = PipelineDAG()
        dag.add_stage(Stage("s0", "collect", 0.3, 10.0, slice_requirement="URLLC"))
        dag.add_stage(Stage("s1", "process", 0.5, 5.0))
        dag.add_stage(Stage("s2", "output", 0.2, 1.0))
        dag.add_edge(Edge("s0", "s1", latency_bound=10.0))
        dag.add_edge(Edge("s1", "s2", latency_bound=10.0))
        return dag, topo, GovernancePolicy(), "slice_constrained_3x5", "linear_sliced"

    if name == "cross_domain":
        nodes = [
            ExecutionUnit("n0", "domain1", "eMBB", capacity=1.0),
            ExecutionUnit("n1", "domain1", "eMBB", capacity=1.0),
            ExecutionUnit("n2", "domain2", "eMBB", capacity=1.5),
            ExecutionUnit("n3", "domain2", "eMBB", capacity=1.5),
        ]
        nids = [n.node_id for n in nodes]
        lats = _full_latency_matrix(nids, 5.0, {("n0", "n1"): 1.0, ("n2", "n3"): 1.0})
        topo = NetworkTopology(nodes=nodes, latency_matrix=lats)
        gov = GovernancePolicy(
            local_stage_types={"collect"},
            trust_levels={("domain1", "domain2"): 1.0},
        )
        dag = PipelineDAG()
        dag.add_stage(Stage("s0", "collect", 0.3, 10.0, data_sovereignty_domain="domain1"))
        dag.add_stage(Stage("s1", "process", 0.5, 5.0))
        dag.add_stage(Stage("s2", "output", 0.2, 1.0))
        dag.add_edge(Edge("s0", "s1", latency_bound=20.0))
        dag.add_edge(Edge("s1", "s2", latency_bound=20.0))
        return dag, topo, gov, "cross_domain_3x4", "linear_governed"

    raise ValueError(f"Unknown scenario: {name}")


# ---------------------------------------------------------------------------
# Parametrized quality test (all scenarios)
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
@pytest.mark.parametrize("scenario", _SCENARIOS)
def test_placement_quality_parametrized(scenario):
    """Parametrized quality check: algorithm cost within 10% of brute-force optimal."""
    dag, topo, gov, label, ptype = _build_scenario(scenario)
    result = _evaluate(label, ptype, dag, topo, gov)

    _assert_quality(result)

    # Constraint-specific assertions per scenario
    if scenario == "slice_constrained":
        # URLLC-constrained stages must be placed on URLLC nodes
        algo_placement = find_placement(dag, topo, gov)
        assigned_node = topo.get_node(algo_placement["s0"])
        assert assigned_node.slice_id == "URLLC", (
            f"Stage s0 requires URLLC but placed on {assigned_node.slice_id} node"
        )

    elif scenario == "cross_domain":
        # Governance-constrained stages must stay in their sovereignty domain
        algo_placement = find_placement(dag, topo, gov)
        s0_node = topo.get_node(algo_placement["s0"])
        assert s0_node.domain_id == "domain1", (
            f"Stage s0 must stay in domain1 but placed in {s0_node.domain_id}"
        )
