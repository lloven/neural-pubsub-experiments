"""Tests for heuristic baseline placements (Task 5).

Verifies locality, latency-greedy, and spillover placement against a
known 2-domain topology with controlled capacities.
"""

import pytest

from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
    locality_placement,
    latency_greedy_placement,
    spillover_placement,
)
from src.pipeline.dag import PipelineDAG, Stage, Edge


# ---------------------------------------------------------------------------
# Shared 2-domain topology
# ---------------------------------------------------------------------------


def _make_two_domain_topology(
    d1_capacity: float = 1.0,
    d2_capacity: float = 1.0,
):
    """Two domains, one worker each.

    d1/w1: compute_times cqi_predict=50ms, anomaly_detect=200ms
    d2/w2: compute_times cqi_predict=200ms, anomaly_detect=50ms
    Cross-domain latency = 100ms, intra-domain = 0ms.
    """
    w1 = ExecutionUnit(
        node_id="w1", domain_id="d1", slice_id="flat",
        capacity=d1_capacity, current_load=0.0,
        compute_times={"cqi_predict": 50.0, "anomaly_detect": 200.0},
    )
    w2 = ExecutionUnit(
        node_id="w2", domain_id="d2", slice_id="flat",
        capacity=d2_capacity, current_load=0.0,
        compute_times={"cqi_predict": 200.0, "anomaly_detect": 50.0},
    )
    topo = NetworkTopology(
        nodes=[w1, w2],
        latency_matrix={
            ("w1", "w2"): 100.0,
            ("w1", "w1"): 0.0,
            ("w2", "w2"): 0.0,
        },
    )
    return topo


def _make_two_stage_pipeline():
    """Pipeline: cqi_predict -> anomaly_detect."""
    dag = PipelineDAG()
    dag.add_stage(Stage("s1", "cqi_predict", 0.1, 1.0))
    dag.add_stage(Stage("s2", "anomaly_detect", 0.1, 1.0))
    dag.add_edge(Edge("s1", "s2", latency_bound=1000.0))
    return dag


def _make_three_stage_pipeline():
    """Pipeline: s1 -> s2 -> s3 (all demand 0.4 each)."""
    dag = PipelineDAG()
    dag.add_stage(Stage("s1", "cqi_predict", 0.4, 1.0))
    dag.add_stage(Stage("s2", "anomaly_detect", 0.4, 1.0))
    dag.add_stage(Stage("s3", "cqi_predict", 0.4, 1.0))
    dag.add_edge(Edge("s1", "s2", latency_bound=1000.0))
    dag.add_edge(Edge("s2", "s3", latency_bound=1000.0))
    return dag


# ---------------------------------------------------------------------------
# Locality placement
# ---------------------------------------------------------------------------


class TestLocalityPlacement:
    """locality_placement places all stages on local-domain workers only."""

    def test_all_stages_on_local_domain(self):
        """All stages placed on d1 workers when local_domain='d1'."""
        topo = _make_two_domain_topology()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()

        placement = locality_placement(dag, topo, gov, local_domain="d1")

        assert placement["s1"] == "w1"
        assert placement["s2"] == "w1"

    def test_zero_cross_domain_placements(self):
        """No stage is placed on a remote domain."""
        topo = _make_two_domain_topology()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()

        placement = locality_placement(dag, topo, gov, local_domain="d1")

        for stage_id, node_id in placement.items():
            node = topo.get_node(node_id)
            assert node.domain_id == "d1", (
                f"Stage {stage_id} placed on {node_id} in domain {node.domain_id}"
            )

    def test_raises_when_local_full(self):
        """Raises RuntimeError when local domain has insufficient capacity."""
        topo = _make_two_domain_topology(d1_capacity=0.05)  # Too small
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()  # Each stage demands 0.1

        with pytest.raises(RuntimeError, match="No feasible local worker"):
            locality_placement(dag, topo, gov, local_domain="d1")

    def test_locality_d2_places_on_d2(self):
        """When local_domain='d2', all stages go to w2."""
        topo = _make_two_domain_topology()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()

        placement = locality_placement(dag, topo, gov, local_domain="d2")

        assert placement["s1"] == "w2"
        assert placement["s2"] == "w2"


# ---------------------------------------------------------------------------
# Latency-greedy placement
# ---------------------------------------------------------------------------


class TestLatencyGreedyPlacement:
    """latency_greedy_placement picks the lowest-latency worker per stage."""

    def test_picks_fastest_worker_per_stage_type(self):
        """cqi_predict goes to w1 (50ms), anomaly_detect goes to w2 (50ms)."""
        topo = _make_two_domain_topology()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()

        placement = latency_greedy_placement(dag, topo, gov)

        # s1 (cqi_predict): w1 has 50ms compute, w2 has 200ms -> w1
        assert placement["s1"] == "w1"
        # s2 (anomaly_detect): w2 has 50ms compute, w1 has 200ms
        # But w2 also incurs 100ms WAN latency from s1 on w1
        # w1: 200ms compute + 0ms latency = 200ms
        # w2: 50ms compute + 100ms latency = 150ms
        assert placement["s2"] == "w2"

    def test_includes_cross_domain_placement(self):
        """Latency-greedy can place stages across domains (unlike locality)."""
        topo = _make_two_domain_topology()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()

        placement = latency_greedy_placement(dag, topo, gov)

        domains = {topo.get_node(nid).domain_id for nid in placement.values()}
        # Should use both domains since each has a faster stage type
        assert len(domains) == 2

    def test_single_stage_picks_fastest(self):
        """Single-stage DAG: picks the worker with lowest compute time."""
        topo = _make_two_domain_topology()
        gov = GovernancePolicy()
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "anomaly_detect", 0.1, 1.0))

        placement = latency_greedy_placement(dag, topo, gov)
        # w2 has 50ms for anomaly_detect vs w1's 200ms
        assert placement["s1"] == "w2"


# ---------------------------------------------------------------------------
# Spillover placement
# ---------------------------------------------------------------------------


class TestSpilloverPlacement:
    """spillover_placement uses local first, overflow to remote."""

    def test_all_local_when_capacity_sufficient(self):
        """When local has enough capacity, all stages stay local."""
        topo = _make_two_domain_topology(d1_capacity=5.0)
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()

        placement = spillover_placement(dag, topo, gov, local_domain="d1")

        assert placement["s1"] == "w1"
        assert placement["s2"] == "w1"

    def test_spillover_when_local_full(self):
        """When local capacity is exceeded, excess spills to remote domain."""
        # d1 has capacity 0.5, pipeline has 3 stages each demanding 0.4
        # First stage fits (0.4 <= 0.5), second doesn't (0.4 > 0.1 remaining)
        topo = _make_two_domain_topology(d1_capacity=0.5, d2_capacity=5.0)
        gov = GovernancePolicy()
        dag = _make_three_stage_pipeline()

        placement = spillover_placement(dag, topo, gov, local_domain="d1")

        # s1 fits on w1 (0.4 <= 0.5)
        assert placement["s1"] == "w1"
        # s2 doesn't fit locally (0.4 > 0.1 remaining), spills to w2
        assert placement["s2"] == "w2"
        # s3 doesn't fit locally either (0.4 > 0.1), spills to w2
        assert placement["s3"] == "w2"

    def test_cross_domain_only_after_local_full(self):
        """Cross-domain placements only appear after local utilization is exceeded."""
        topo = _make_two_domain_topology(d1_capacity=0.5, d2_capacity=5.0)
        gov = GovernancePolicy()
        dag = _make_three_stage_pipeline()

        placement = spillover_placement(dag, topo, gov, local_domain="d1")

        # Check ordering: first stage(s) are local, later are remote
        local_stages = [
            sid for sid, nid in placement.items()
            if topo.get_node(nid).domain_id == "d1"
        ]
        remote_stages = [
            sid for sid, nid in placement.items()
            if topo.get_node(nid).domain_id != "d1"
        ]
        assert len(local_stages) >= 1  # At least one local
        assert len(remote_stages) >= 1  # At least one remote (spillover)

    def test_raises_when_both_domains_full(self):
        """Raises when neither local nor remote has capacity."""
        topo = _make_two_domain_topology(d1_capacity=0.05, d2_capacity=0.05)
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline()  # Each stage demands 0.1

        with pytest.raises(RuntimeError, match="No feasible worker"):
            spillover_placement(dag, topo, gov, local_domain="d1")
