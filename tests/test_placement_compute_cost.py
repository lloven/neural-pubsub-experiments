"""Tests for compute-time-aware placement cost (Block 1, Step 3).

The placement cost function (Eq. 10) should include per-stage compute time:
    min α·(L_total + C_total) + β·U_total + γ·D_cross

where C_total = Σ compute_time(worker, stage_type) for all stages in the pipeline.

This ensures the optimizer can trade off WAN latency against compute time:
a remote primary-tier worker (50ms compute + 100ms WAN = 150ms) may be
cheaper than a local secondary-tier worker (200ms compute + 0ms WAN = 200ms).
"""

import pytest

from src.broker.placement import (
    ExecutionUnit,
    NetworkTopology,
    GovernancePolicy,
    compute_placement_cost,
)
from src.pipeline.dag import PipelineDAG, Stage, Edge


def _make_two_domain_topology(
    *,
    d1_compute: dict[str, float] | None = None,
    d2_compute: dict[str, float] | None = None,
):
    """Create a minimal 2-domain topology with optional compute times.

    d1 has worker 'w1' (domain d1, slice URLLC).
    d2 has worker 'w2' (domain d2, slice eMBB).
    WAN latency between w1 and w2 is 100ms.
    """
    w1 = ExecutionUnit(
        node_id="w1", domain_id="d1", slice_id="URLLC",
        capacity=1.0, current_load=0.0,
        compute_times=d1_compute,
    )
    w2 = ExecutionUnit(
        node_id="w2", domain_id="d2", slice_id="eMBB",
        capacity=1.0, current_load=0.0,
        compute_times=d2_compute,
    )
    topo = NetworkTopology(
        nodes=[w1, w2],
        latency_matrix={("w1", "w2"): 100.0, ("w1", "w1"): 0.0, ("w2", "w2"): 0.0},
    )
    return topo


def _make_linear_pipeline():
    """2-stage linear pipeline: collect → predict."""
    dag = PipelineDAG()
    dag.add_stage(Stage("collect", "cqi_collect", 0.1, 1.0))
    dag.add_stage(Stage("predict", "cqi_predict", 0.1, 1.0))
    dag.add_edge(Edge("collect", "predict", latency_bound=1000.0))
    return dag


class TestComputeCostInPlacement:
    """Placement cost includes per-stage compute time."""

    def test_cost_includes_compute_time(self):
        """When workers have compute_times, the cost function adds C_total."""
        topo = _make_two_domain_topology(
            d1_compute={"cqi_collect": 50.0, "cqi_predict": 50.0},
            d2_compute={"cqi_collect": 200.0, "cqi_predict": 200.0},
        )
        dag = _make_linear_pipeline()
        gov = GovernancePolicy()

        # Place both stages on w1 (d1, primary-tier: 50ms each)
        cost_local = compute_placement_cost(
            {"collect": "w1", "predict": "w1"}, dag, topo, gov,
            alpha=1.0, beta=0.0, gamma=0.0,
        )
        # L_total = 0 (same node) + C_total = 50 + 50 = 100
        # Total = 1.0 * (0 + 100) = 100
        assert cost_local == pytest.approx(100.0)

    def test_remote_primary_cheaper_than_local_secondary(self):
        """Remote primary (50ms + 100ms WAN) < local secondary (200ms)."""
        topo = _make_two_domain_topology(
            d1_compute={"cqi_collect": 50.0, "cqi_predict": 200.0},  # d1: primary collect, secondary predict
            d2_compute={"cqi_collect": 200.0, "cqi_predict": 50.0},  # d2: secondary collect, primary predict
        )
        dag = _make_linear_pipeline()
        gov = GovernancePolicy()

        # Option A: both local on w1 → C_total = 50+200 = 250, L_total = 0
        cost_local = compute_placement_cost(
            {"collect": "w1", "predict": "w1"}, dag, topo, gov,
            alpha=1.0, beta=0.0, gamma=0.0,
        )
        # Option B: collect on w1, predict on w2 → C_total = 50+50 = 100, L_total = 100 (WAN)
        cost_split = compute_placement_cost(
            {"collect": "w1", "predict": "w2"}, dag, topo, gov,
            alpha=1.0, beta=0.0, gamma=0.0,
        )
        # cost_local = 250, cost_split = 200
        assert cost_split < cost_local

    def test_no_compute_times_backward_compatible(self):
        """When compute_times is None, cost = α·L + β·U + γ·D (no C_total)."""
        topo = _make_two_domain_topology(d1_compute=None, d2_compute=None)
        dag = _make_linear_pipeline()
        gov = GovernancePolicy()

        cost = compute_placement_cost(
            {"collect": "w1", "predict": "w1"}, dag, topo, gov,
            alpha=1.0, beta=0.0, gamma=0.0,
        )
        # L_total = 0 (same node), C_total = 0 (no compute_times)
        assert cost == pytest.approx(0.0)

    def test_domain_crossing_penalty_independent(self):
        """D_cross penalty is additive with compute cost."""
        topo = _make_two_domain_topology(
            d1_compute={"cqi_collect": 50.0, "cqi_predict": 50.0},
            d2_compute={"cqi_collect": 50.0, "cqi_predict": 50.0},
        )
        dag = _make_linear_pipeline()
        gov = GovernancePolicy()

        cost_local = compute_placement_cost(
            {"collect": "w1", "predict": "w1"}, dag, topo, gov,
            alpha=1.0, beta=0.0, gamma=500.0,
        )
        cost_cross = compute_placement_cost(
            {"collect": "w1", "predict": "w2"}, dag, topo, gov,
            alpha=1.0, beta=0.0, gamma=500.0,
        )
        # cost_local = 1*(0+100) + 500*0 = 100
        # cost_cross = 1*(100+100) + 500*1 = 700
        assert cost_local == pytest.approx(100.0)
        assert cost_cross == pytest.approx(700.0)
