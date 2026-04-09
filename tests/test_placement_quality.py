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

TREE_SCENARIO_NAMES = ["homogeneous", "heterogeneous", "funnel", "slice_constrained", "cross_domain"]

NON_TREE_SCENARIO_NAMES = [
    "diamond",
    "lattice_2x3",
    "fork_join",
    "shared_resource",
    "series_parallel",
]

SCENARIO_NAMES = TREE_SCENARIO_NAMES + NON_TREE_SCENARIO_NAMES


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

    # ------------------------------------------------------------------
    # Non-tree DAG scenarios (greedy heuristic path)
    # ------------------------------------------------------------------

    if name == "diamond":
        # Diamond: A→B, A→C, B→D, C→D (shared sink, fan-out at A).
        # Adversarial latency: n0 has highest capacity (greedy picks it for A),
        # but n0 is distant from the cluster {n1,n2,n3}. Optimal places A in
        # the cluster despite slightly higher load cost. Demonstrates greedy's
        # myopia: it doesn't account for successor latency when placing A.
        nodes = [
            ExecutionUnit("n0", "d1", "eMBB", capacity=1.0),   # isolated
            ExecutionUnit("n1", "d1", "eMBB", capacity=0.9),   # cluster
            ExecutionUnit("n2", "d1", "eMBB", capacity=0.9),   # cluster
            ExecutionUnit("n3", "d1", "eMBB", capacity=0.9),   # cluster
        ]
        lats = {
            ("n0", "n1"): 4.0, ("n0", "n2"): 4.0, ("n0", "n3"): 4.0,
            ("n1", "n2"): 1.0, ("n1", "n3"): 1.0, ("n2", "n3"): 1.0,
        }
        topo = NetworkTopology(nodes=nodes, latency_matrix=lats)
        dag = PipelineDAG()
        dag.add_stage(Stage("A", "source", 0.4, 5.0))
        dag.add_stage(Stage("B", "process", 0.4, 5.0))
        dag.add_stage(Stage("C", "process", 0.4, 5.0))
        dag.add_stage(Stage("D", "sink", 0.4, 5.0))
        dag.add_edge(Edge("A", "B", latency_bound=20.0))
        dag.add_edge(Edge("A", "C", latency_bound=20.0))
        dag.add_edge(Edge("B", "D", latency_bound=20.0))
        dag.add_edge(Edge("C", "D", latency_bound=20.0))
        return dag, topo, GovernancePolicy(), "diamond_4x4", "diamond"

    if name == "lattice_2x3":
        # 2x3 lattice: two rows of 3, with forward and cross edges
        #   r0c0 → r0c1 → r0c2
        #     ↓  ↘  ↓  ↘  ↓
        #   r1c0 → r1c1 → r1c2
        nodes = [_node(f"n{i}", capacity=1.5) for i in range(5)]
        nids = [n.node_id for n in nodes]
        topo = NetworkTopology(
            nodes=nodes,
            latency_matrix=_full_latency_matrix(nids, 2.0),
        )
        dag = PipelineDAG()
        for r in range(2):
            for c in range(3):
                sid = f"r{r}c{c}"
                dag.add_stage(Stage(sid, "compute", 0.2, 3.0))
        # Row edges
        for r in range(2):
            for c in range(2):
                dag.add_edge(Edge(f"r{r}c{c}", f"r{r}c{c+1}", latency_bound=10.0))
        # Column edges
        for c in range(3):
            dag.add_edge(Edge(f"r0c{c}", f"r1c{c}", latency_bound=10.0))
        # Cross edges (diagonal)
        for c in range(2):
            dag.add_edge(Edge(f"r0c{c}", f"r1c{c+1}", latency_bound=10.0))
        return dag, topo, GovernancePolicy(), "lattice_2x3_6x5", "lattice"

    if name == "fork_join":
        # Fork-join: source fans out to 3 parallel paths, all join at sink
        #   S → P0 → Q0 ↘
        #   S → P1 → Q1 → J
        #   S → P2 → Q2 ↗
        nodes = [_node(f"n{i}", capacity=2.0) for i in range(4)]
        nids = [n.node_id for n in nodes]
        topo = NetworkTopology(
            nodes=nodes,
            latency_matrix=_full_latency_matrix(nids, 2.0, {
                ("n0", "n1"): 1.0,
                ("n1", "n2"): 1.0,
            }),
        )
        dag = PipelineDAG()
        dag.add_stage(Stage("S", "source", 0.2, 5.0))
        for i in range(3):
            dag.add_stage(Stage(f"P{i}", "process", 0.3, 3.0))
            dag.add_stage(Stage(f"Q{i}", "transform", 0.2, 3.0))
        dag.add_stage(Stage("J", "join", 0.3, 5.0))
        for i in range(3):
            dag.add_edge(Edge("S", f"P{i}", latency_bound=10.0))
            dag.add_edge(Edge(f"P{i}", f"Q{i}", latency_bound=10.0))
            dag.add_edge(Edge(f"Q{i}", "J", latency_bound=10.0))
        return dag, topo, GovernancePolicy(), "fork_join_8x4", "fork_join"

    if name == "shared_resource":
        # Two pipelines sharing a common middle stage
        #   A0 → M → B0
        #   A1 ↗   ↘ B1
        # Congested: high demand relative to capacity (>50% utilisation)
        nodes = [
            ExecutionUnit("n0", "d1", "eMBB", capacity=0.7),
            ExecutionUnit("n1", "d1", "eMBB", capacity=0.7),
            ExecutionUnit("n2", "d1", "eMBB", capacity=0.7),
            ExecutionUnit("n3", "d1", "eMBB", capacity=0.7),
        ]
        nids = [n.node_id for n in nodes]
        topo = NetworkTopology(
            nodes=nodes,
            latency_matrix=_full_latency_matrix(nids, 2.0),
        )
        dag = PipelineDAG()
        dag.add_stage(Stage("A0", "ingest", 0.3, 5.0))
        dag.add_stage(Stage("A1", "ingest", 0.3, 5.0))
        dag.add_stage(Stage("M", "process", 0.5, 5.0))  # shared, heavy
        dag.add_stage(Stage("B0", "output", 0.2, 3.0))
        dag.add_stage(Stage("B1", "output", 0.2, 3.0))
        dag.add_edge(Edge("A0", "M", latency_bound=10.0))
        dag.add_edge(Edge("A1", "M", latency_bound=10.0))
        dag.add_edge(Edge("M", "B0", latency_bound=10.0))
        dag.add_edge(Edge("M", "B1", latency_bound=10.0))
        return dag, topo, GovernancePolicy(), "shared_resource_5x4", "shared"

    if name == "series_parallel":
        # Series-parallel graph: two parallel chains connected in series
        #   S → A0 → A1 ↘
        #                 M → C0 → T
        #   S → B0 → B1 ↗
        nodes = [_node(f"n{i}", capacity=1.5) for i in range(4)]
        nids = [n.node_id for n in nodes]
        topo = NetworkTopology(
            nodes=nodes,
            latency_matrix=_full_latency_matrix(nids, 2.0, {
                ("n0", "n1"): 1.0,
                ("n2", "n3"): 1.0,
            }),
        )
        dag = PipelineDAG()
        dag.add_stage(Stage("S", "source", 0.2, 5.0))
        dag.add_stage(Stage("A0", "process", 0.3, 3.0))
        dag.add_stage(Stage("A1", "process", 0.3, 3.0))
        dag.add_stage(Stage("B0", "process", 0.3, 3.0))
        dag.add_stage(Stage("B1", "process", 0.3, 3.0))
        dag.add_stage(Stage("M", "merge", 0.3, 5.0))
        dag.add_stage(Stage("C0", "compute", 0.2, 3.0))
        dag.add_stage(Stage("T", "sink", 0.2, 3.0))
        dag.add_edge(Edge("S", "A0", latency_bound=10.0))
        dag.add_edge(Edge("S", "B0", latency_bound=10.0))
        dag.add_edge(Edge("A0", "A1", latency_bound=10.0))
        dag.add_edge(Edge("B0", "B1", latency_bound=10.0))
        dag.add_edge(Edge("A1", "M", latency_bound=10.0))
        dag.add_edge(Edge("B1", "M", latency_bound=10.0))
        dag.add_edge(Edge("M", "C0", latency_bound=10.0))
        dag.add_edge(Edge("C0", "T", latency_bound=10.0))
        return dag, topo, GovernancePolicy(), "series_parallel_8x4", "series_parallel"

    raise ValueError(f"Unknown scenario: {name}")


# ---------------------------------------------------------------------------
# Parametrized quality test (all scenarios)
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
@pytest.mark.parametrize("scenario", _SCENARIOS)
def test_placement_quality_parametrized(scenario):
    """Parametrized quality check: tree scenarios within 10% of optimal, non-tree within 200%."""
    dag, topo, gov, label, ptype = _build_scenario(scenario)
    result = _evaluate(label, ptype, dag, topo, gov)

    # Tree DAGs use DP (provably optimal for Eq. 10 cost), but the
    # post-placement redistribution intentionally deviates from Eq. 10
    # optimality for fan-in trees to avoid serialising concurrent stages.
    # Fan-in trees (funnel) have a larger cost gap because redistribution
    # increases inter-node latency while Eq. 10 doesn't reward parallelism.
    # Non-tree DAGs use the greedy heuristic which can have a significant gap,
    # especially on adversarial topologies (e.g. diamond with isolated node).
    FAN_IN_SCENARIOS = ["funnel"]
    if scenario in TREE_SCENARIO_NAMES and scenario not in FAN_IN_SCENARIOS:
        _assert_quality(result, max_gap=0.10)
    elif scenario in FAN_IN_SCENARIOS:
        # Fan-in redistribution trades Eq. 10 cost for execution parallelism.
        # Allow a larger gap since the "optimal" colocated placement actually
        # produces worse real-world performance.
        _assert_quality(result, max_gap=5.0)
    else:
        _assert_quality(result, max_gap=2.0)

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


# ---------------------------------------------------------------------------
# Non-tree DAG structural verification
# ---------------------------------------------------------------------------


_NON_TREE_SCENARIOS = [
    pytest.param(name, id=name) for name in NON_TREE_SCENARIO_NAMES
]

_TREE_SCENARIOS = [
    pytest.param(name, id=name) for name in TREE_SCENARIO_NAMES
]


@pytest.mark.benchmark
@pytest.mark.parametrize("scenario", _NON_TREE_SCENARIOS)
def test_non_tree_dag_is_not_tree(scenario):
    """Non-tree scenarios must produce DAGs where is_tree() is False (greedy path)."""
    dag, _topo, _gov, _label, _ptype = _build_scenario(scenario)
    assert dag.is_tree() is False, (
        f"Scenario {scenario!r} should produce a non-tree DAG but is_tree() returned True"
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("scenario", _NON_TREE_SCENARIOS)
def test_non_tree_brute_force_finds_feasible_placement(scenario):
    """Brute-force must find at least one feasible placement for each non-tree scenario."""
    dag, topo, gov, _label, _ptype = _build_scenario(scenario)
    opt_cost, opt_placement = _brute_force_optimal(dag, topo, gov)
    assert opt_cost < float("inf"), (
        f"Scenario {scenario!r}: no feasible placement found by brute-force"
    )
    assert len(opt_placement) == len(dag), (
        f"Scenario {scenario!r}: optimal placement has {len(opt_placement)} stages, "
        f"expected {len(dag)}"
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("scenario", _NON_TREE_SCENARIOS)
def test_non_tree_gap_ratio_computed(scenario):
    """Gap ratio is computed for non-tree scenarios (may be > 0, that's expected)."""
    dag, topo, gov, label, ptype = _build_scenario(scenario)
    result = _evaluate(label, ptype, dag, topo, gov)
    assert result.constraint_violations == 0, (
        f"Scenario {scenario!r}: {result.constraint_violations} constraint violation(s)"
    )
    assert result.gap_ratio >= 0.0, (
        f"Scenario {scenario!r}: negative gap_ratio {result.gap_ratio:.4f}"
    )
    # Non-tree scenarios: greedy is a heuristic, gap can be significant on
    # adversarial topologies. We bound it loosely here; the actual gap values
    # are the interesting experimental output reported in the CSV.
    _assert_quality(result, max_gap=2.0)


_SERIAL_TREE_SCENARIOS = [
    pytest.param(name, id=name)
    for name in TREE_SCENARIO_NAMES if name != "funnel"
]


@pytest.mark.benchmark
@pytest.mark.parametrize("scenario", _SERIAL_TREE_SCENARIOS)
def test_serial_tree_scenarios_have_zero_gap(scenario):
    """Regression: serial tree scenarios must have gap_ratio = 0 (DP optimal)."""
    dag, topo, gov, label, ptype = _build_scenario(scenario)
    result = _evaluate(label, ptype, dag, topo, gov)
    assert result.gap_ratio < 1e-9, (
        f"Tree scenario {scenario!r}: gap_ratio {result.gap_ratio:.6f} is not zero "
        f"(algo={result.algorithm_cost:.6f}, opt={result.optimal_cost:.6f})"
    )


@pytest.mark.benchmark
def test_fanin_tree_redistributes_siblings():
    """Fan-in tree (funnel): DP redistributes colocated siblings.

    The redistribution intentionally deviates from Eq. 10 optimality to
    avoid serialising concurrent fan-in stages. The Eq. 10 cost increases
    but real-world execution parallelism improves.
    """
    dag, topo, gov, label, ptype = _build_scenario("funnel")
    result = _evaluate(label, ptype, dag, topo, gov)
    # The redistribution increases cost but uses multiple workers
    assert result.gap_ratio > 0, "Funnel should redistribute (non-zero gap)"
    assert result.constraint_violations == 0, "No constraint violations"


@pytest.mark.benchmark
def test_shared_resource_is_congested():
    """Shared-resource scenario has demand > 50% of total capacity (congestion test)."""
    dag, topo, _gov, _label, _ptype = _build_scenario("shared_resource")
    total_demand = sum(dag.get_stage(sid).computational_demand for sid in dag.stages)
    total_capacity = sum(n.capacity for n in topo.nodes)
    utilisation = total_demand / total_capacity
    assert utilisation > 0.5, (
        f"Shared-resource scenario should be congested (>50% utilisation) "
        f"but utilisation is {utilisation:.1%}"
    )
