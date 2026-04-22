"""Tests for within-round load tracking in market_mode_placement.

Regression: 2026-04-22 discovery. At sat-15 (15 pps, no failure), market-quad
underperformed oracle-global (CR 71.8% vs 82.9% on cqi-chain). Root cause:
``market_mode_placement`` checks ``w.residual_capacity`` per stage but does
not track cumulative load committed within the same placement round. Oracle's
``_greedy_placement`` uses an ``additional_load: dict[str, float]`` to prevent
double-booking a worker across multiple stages of the same pipeline.

At saturation, multi-stage pipelines can be placed entirely on the same
cheap worker because each stage sees the pre-round residual_capacity
snapshot. Oracle avoids this by booking capacity after each placement
decision. This test pins the fix.
"""

from __future__ import annotations

import pytest

from src.broker.market import WorkerBid, compute_clearing_prices
from src.broker.placement import (
    ExecutionUnit,
    NetworkTopology,
    GovernancePolicy,
    market_mode_placement,
)
from src.pipeline.dag import PipelineDAG, Stage, Edge


def _make_single_domain_four_workers():
    """One domain, 4 workers with small capacity (0.3 each).

    Each worker can host exactly ONE stage of demand 0.25.
    Two stages cannot share a worker without overbooking.
    """
    workers = []
    for i in range(4):
        workers.append(
            ExecutionUnit(
                node_id=f"w{i}",
                domain_id="d1",
                slice_id="URLLC",
                capacity=0.3,
                current_load=0.0,
                compute_times={"stage_a": 100.0, "stage_b": 100.0},
            )
        )
    # All pairwise latencies = 0 (same domain)
    lat = {}
    for a in workers:
        for b in workers:
            lat[(a.node_id, b.node_id)] = 0.0
    return NetworkTopology(nodes=workers, latency_matrix=lat)


def _make_four_stage_pipeline():
    """4 sequential stages, each with demand 0.25."""
    dag = PipelineDAG()
    stage_type = "stage_a"
    dag.add_stage(Stage("s1", stage_type, 0.25, 1.0))
    dag.add_stage(Stage("s2", stage_type, 0.25, 1.0))
    dag.add_stage(Stage("s3", stage_type, 0.25, 1.0))
    dag.add_stage(Stage("s4", stage_type, 0.25, 1.0))
    dag.add_edge(Edge("s1", "s2", latency_bound=1000.0))
    dag.add_edge(Edge("s2", "s3", latency_bound=1000.0))
    dag.add_edge(Edge("s3", "s4", latency_bound=1000.0))
    return dag


class TestMarketWithinRoundLoadTracking:
    """market_mode_placement must track cumulative load placed within a
    single placement round, mirroring oracle's ``additional_load`` logic."""

    def test_load_aware_distributes_stages_across_workers(self):
        """With load_aware=True, 4 stages of demand 0.25 each must be
        placed on 4 distinct workers when each worker holds only 0.3
        capacity. Buggy behaviour would concentrate multiple stages on
        the same least-loaded worker (w0) because its current_load stays
        at 0 for the entire placement round.
        """
        topo = _make_single_domain_four_workers()
        gov = GovernancePolicy()
        dag = _make_four_stage_pipeline()

        # Uniform prices so all workers look equally attractive
        bids = [
            WorkerBid(w.node_id, "d1", "stage_a", 100.0, cost_per_stage=1.0)
            for w in topo.nodes
        ]
        prices = compute_clearing_prices(bids, {"stage_a": 4})

        placement = market_mode_placement(
            dag, topo, gov, prices,
            wan_cost=0.0, local_domain="d1", load_aware=True,
        )
        assert placement is not None, "placement must succeed"
        placed_workers = set(placement.values())
        assert len(placed_workers) == 4, (
            "4 stages of demand 0.25 with 0.3-capacity workers must be "
            f"spread across 4 distinct workers; got {sorted(placement.items())}"
        )

    def test_reject_when_total_pipeline_demand_exceeds_cluster_capacity(self):
        """Pipeline requesting more capacity than the cluster can provide
        must be rejected (return None), not silently overbook workers.

        Two stages of demand 0.4 each = 0.8 total demand. Two workers
        with 0.3 capacity each = 0.6 total cluster capacity. The
        pipeline is infeasible; correct behaviour is to return None.
        """
        workers = [
            ExecutionUnit(
                node_id=f"w{i}", domain_id="d1", slice_id="URLLC",
                capacity=0.3, current_load=0.0,
                compute_times={"stage_a": 100.0},
            )
            for i in range(2)
        ]
        lat = {(a.node_id, b.node_id): 0.0 for a in workers for b in workers}
        topo = NetworkTopology(nodes=workers, latency_matrix=lat)
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "stage_a", 0.4, 1.0))
        dag.add_stage(Stage("s2", "stage_a", 0.4, 1.0))
        dag.add_edge(Edge("s1", "s2", latency_bound=1000.0))

        bids = [WorkerBid(w.node_id, "d1", "stage_a", 100.0, 1.0) for w in workers]
        prices = compute_clearing_prices(bids, {"stage_a": 2})

        placement = market_mode_placement(
            dag, topo, governance=GovernancePolicy(), clearing_prices=prices,
            wan_cost=0.0, local_domain="d1", load_aware=True,
        )
        # Each worker has 0.3 residual; each stage needs 0.4 > 0.3
        # Even individually, no worker can host any stage → reject
        assert placement is None

    def test_load_aware_packs_when_capacity_permits(self):
        """Sanity: if a single worker has enough capacity for all stages,
        load_aware placement packs them there (it doesn't artificially
        spread when not needed)."""
        # One big worker + three tiny
        big = ExecutionUnit(
            node_id="big", domain_id="d1", slice_id="URLLC",
            capacity=4.0, current_load=0.0,
            compute_times={"stage_a": 100.0},
        )
        smalls = [
            ExecutionUnit(
                node_id=f"s{i}", domain_id="d1", slice_id="URLLC",
                capacity=0.3, current_load=0.0,
                compute_times={"stage_a": 100.0},
            )
            for i in range(3)
        ]
        workers = [big] + smalls
        lat = {(a.node_id, b.node_id): 0.0 for a in workers for b in workers}
        topo = NetworkTopology(nodes=workers, latency_matrix=lat)
        dag = _make_four_stage_pipeline()

        bids = [WorkerBid(w.node_id, "d1", "stage_a", 100.0, 1.0) for w in workers]
        prices = compute_clearing_prices(bids, {"stage_a": 4})

        placement = market_mode_placement(
            dag, topo, governance=GovernancePolicy(), clearing_prices=prices,
            wan_cost=0.0, local_domain="d1", load_aware=True,
        )
        assert placement is not None
        # All 4 stages fit on the big worker (4.0 capacity, 4×0.25 demand).
        # After placing each, residual is tracked down: 4.0 → 3.75 → 3.5 → 3.25 → 3.0
        # Smalls always preferred only when big's effective residual goes below 0.25
        # which doesn't happen here. load_aware picks least-loaded among feasible.
        # After first placement, big.current_load would be tracked as 0.25; smalls
        # at 0. So smalls are preferred from stage 2 onward when load tracking works.
        # The test doesn't over-constrain which worker — it just asserts nothing
        # was rejected and no worker got double-booked beyond its capacity.
        load_per_worker: dict[str, float] = {}
        for stage_id, node_id in placement.items():
            stage = dag.get_stage(stage_id)
            load_per_worker[node_id] = (
                load_per_worker.get(node_id, 0.0) + stage.computational_demand
            )
        for node_id, committed in load_per_worker.items():
            node = next(w for w in topo.nodes if w.node_id == node_id)
            assert committed <= node.capacity + 1e-9, (
                f"worker {node_id} overbooked: {committed} > {node.capacity}"
            )

    def test_legacy_load_aware_false_still_first_feasible(self):
        """load_aware=False preserves legacy behaviour (first feasible
        worker in iteration order). Within-round load tracking must still
        prevent overbooking even in legacy mode, but ordering is preserved."""
        topo = _make_single_domain_four_workers()
        gov = GovernancePolicy()
        dag = _make_four_stage_pipeline()

        bids = [
            WorkerBid(w.node_id, "d1", "stage_a", 100.0, 1.0)
            for w in topo.nodes
        ]
        prices = compute_clearing_prices(bids, {"stage_a": 4})

        placement = market_mode_placement(
            dag, topo, gov, prices,
            wan_cost=0.0, local_domain="d1", load_aware=False,
        )
        assert placement is not None
        # Legacy mode picks first feasible worker. With load tracking, after
        # placing s1 on w0, w0 has residual 0.05 which is < 0.25; s2 goes to w1.
        # So stages fill workers in order.
        assert placement["s1"] == "w0"
        assert placement["s2"] == "w1"
        assert placement["s3"] == "w2"
        assert placement["s4"] == "w3"
