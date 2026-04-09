"""Tests for DP solver colocation fix.

The DP solver must not assign all stages of a fan-in pipeline to the same
worker. When multiple independent stages (e.g., 4 parallel sources in the
anomaly-sp pipeline) can execute concurrently, colocating them serializes
execution and destroys the parallelism benefit.

The fix: the DP solver tracks accumulated demand per worker within a single
pipeline placement, so assigning a second stage to an already-loaded worker
incurs a higher utilization cost.
"""

from __future__ import annotations

import pytest

from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
    find_placement,
    _dp_placement,
)
from src.pipeline.patterns import (
    cqi_prediction_chain_8stage,
    ran_anomaly_detection_8stage,
)


def _make_topology(n_workers: int = 12, domain: str = "d1") -> NetworkTopology:
    """Create a single-domain topology with n identical workers."""
    nodes = [
        ExecutionUnit(f"{domain}-w{i}", domain, "URLLC", 1.0, 0.0)
        for i in range(n_workers)
    ]
    latency = {}
    for i in range(n_workers):
        for j in range(i + 1, n_workers):
            latency[(f"{domain}-w{i}", f"{domain}-w{j}")] = 2.0
    return NetworkTopology(nodes=nodes, latency_matrix=latency)


class TestDPFanInDistribution:
    """DP solver should distribute independent stages across workers."""

    def test_anomaly_sp_sources_on_different_workers(self):
        """4 parallel source stages must NOT all land on the same worker."""
        dag = ran_anomaly_detection_8stage()
        topo = _make_topology(12)
        gov = GovernancePolicy()

        placement = find_placement(dag, topo, gov)

        source_stages = ["du_metrics", "du_rf_meas", "cu_cp_signaling", "cu_up_traffic"]
        source_workers = {placement[s] for s in source_stages}
        assert len(source_workers) >= 2, (
            f"4 independent source stages should use >=2 workers, "
            f"got {len(source_workers)}: {source_workers}"
        )

    def test_anomaly_sp_uses_multiple_workers(self):
        """Anomaly-sp pipeline should use more than 1 worker total."""
        dag = ran_anomaly_detection_8stage()
        topo = _make_topology(12)
        gov = GovernancePolicy()

        placement = find_placement(dag, topo, gov)
        workers_used = set(placement.values())
        assert len(workers_used) >= 2, (
            f"8-stage pipeline should use >=2 workers, got {workers_used}"
        )

    def test_cqi_chain_still_works(self):
        """CQI chain (serial) should still produce valid placement."""
        dag = cqi_prediction_chain_8stage()
        topo = _make_topology(12)
        gov = GovernancePolicy()

        placement = find_placement(dag, topo, gov)
        assert len(placement) == 8

    def test_dp_with_load_tracking(self):
        """DP solver considers accumulated load when choosing workers.

        Two stages with demand 0.5 each should prefer different workers
        (capacity 1.0 each) over colocating on one.
        """
        from src.pipeline.dag import PipelineDAG, Stage, Edge

        dag = PipelineDAG()
        dag.add_stage(Stage("src1", "collect", 0.5, 1.0))
        dag.add_stage(Stage("src2", "collect", 0.5, 1.0))
        dag.add_stage(Stage("sink", "fuse", 0.1, 1.0))
        dag.add_edge(Edge("src1", "sink", latency_bound=50.0))
        dag.add_edge(Edge("src2", "sink", latency_bound=50.0))

        # 2 workers — if DP tracks load, src1 and src2 go to different workers
        topo = _make_topology(2)
        gov = GovernancePolicy()

        placement = find_placement(dag, topo, gov)
        assert placement["src1"] != placement["src2"], (
            "Two independent stages with demand 0.5 each should be on "
            "different workers (capacity 1.0)"
        )

    def test_dp_does_not_exceed_capacity(self):
        """DP solver must not assign demand exceeding a worker's capacity."""
        from src.pipeline.dag import PipelineDAG, Stage, Edge

        dag = PipelineDAG()
        # 3 sources with demand 0.4 each -> sink
        for i in range(3):
            dag.add_stage(Stage(f"src{i}", "collect", 0.4, 1.0))
        dag.add_stage(Stage("sink", "fuse", 0.1, 1.0))
        for i in range(3):
            dag.add_edge(Edge(f"src{i}", "sink", latency_bound=50.0))

        # 2 workers with capacity 1.0 — can't fit 3x0.4=1.2 on one worker
        topo = _make_topology(2)
        gov = GovernancePolicy()

        placement = find_placement(dag, topo, gov)
        # Check no worker gets > 1.0 total demand
        load = {}
        for stage_id, node_id in placement.items():
            d = dag.get_stage(stage_id).computational_demand
            load[node_id] = load.get(node_id, 0.0) + d
        for node_id, total in load.items():
            assert total <= 1.0 + 1e-9, (
                f"Worker {node_id} overloaded: {total:.2f} > 1.0"
            )
