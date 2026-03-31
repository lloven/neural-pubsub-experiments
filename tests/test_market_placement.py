"""Tests for market-mode placement (Block 3, Step 13).

In market mode, the broker allocates stages using clearing prices
rather than full worker visibility. Cross-domain placement occurs
only when remote_price + WAN < local_price. Pipelines are rejected
if total placement cost exceeds value_budget.
"""

import pytest

from src.broker.market import WorkerBid, compute_clearing_prices, should_trade_cross_domain
from src.broker.placement import (
    ExecutionUnit,
    NetworkTopology,
    GovernancePolicy,
    compute_placement_cost,
    market_mode_placement,
)
from src.pipeline.dag import PipelineDAG, Stage, Edge


def _make_two_domain_setup():
    """Two domains: d1 has URLLC-primary worker, d2 has eMBB-primary worker.

    d1/w1: cqi_predict=50ms (primary), anomaly_detect=200ms (secondary)
    d2/w2: cqi_predict=200ms (secondary), anomaly_detect=50ms (primary)
    WAN latency = 100ms.
    """
    w1 = ExecutionUnit(
        node_id="w1", domain_id="d1", slice_id="URLLC",
        capacity=1.0, current_load=0.0,
        compute_times={"cqi_predict": 50.0, "anomaly_detect": 200.0},
    )
    w2 = ExecutionUnit(
        node_id="w2", domain_id="d2", slice_id="eMBB",
        capacity=1.0, current_load=0.0,
        compute_times={"cqi_predict": 200.0, "anomaly_detect": 50.0},
    )
    topo = NetworkTopology(
        nodes=[w1, w2],
        latency_matrix={("w1", "w2"): 100.0, ("w1", "w1"): 0.0, ("w2", "w2"): 0.0},
    )
    return topo, w1, w2


def _make_two_stage_pipeline(value_budget=None):
    """Pipeline: cqi_predict → anomaly_detect."""
    dag = PipelineDAG(value_budget=value_budget)
    dag.add_stage(Stage("s1", "cqi_predict", 0.1, 1.0))
    dag.add_stage(Stage("s2", "anomaly_detect", 0.1, 1.0))
    dag.add_edge(Edge("s1", "s2", latency_bound=1000.0))
    return dag


class TestMarketModePlacement:
    """Broker in market mode uses prices for allocation."""

    def test_market_places_on_cheapest_domain(self):
        """Each stage goes to the domain with lowest price for its type."""
        topo, w1, w2 = _make_two_domain_setup()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline(value_budget=500.0)

        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0),
            WorkerBid("w1", "d1", "anomaly_detect", 200.0, cost_per_stage=4.0),
            WorkerBid("w2", "d2", "cqi_predict", 200.0, cost_per_stage=4.0),
            WorkerBid("w2", "d2", "anomaly_detect", 50.0, cost_per_stage=1.0),
        ]
        demand = {"cqi_predict": 1, "anomaly_detect": 1}
        prices = compute_clearing_prices(bids, demand)

        placement = market_mode_placement(
            dag, topo, gov, prices, wan_cost=100.0, local_domain="d1",
        )
        # cqi_predict cheaper on d1 (1.0 vs 4.0+100 WAN)
        # anomaly_detect cheaper on d2 (1.0+100 WAN vs 4.0 local)
        # 1+100=101 < 4 is FALSE → anomaly_detect stays local
        # So both stages on w1 (local domain d1)
        assert placement["s1"] == "w1"
        assert placement["s2"] == "w1"

    def test_cross_domain_when_price_difference_exceeds_wan(self):
        """Cross-domain placement when remote is much cheaper."""
        topo, w1, w2 = _make_two_domain_setup()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline(value_budget=500.0)

        # Make d1's anomaly_detect very expensive (10.0) vs d2's (1.0)
        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0),
            WorkerBid("w1", "d1", "anomaly_detect", 200.0, cost_per_stage=10.0),
            WorkerBid("w2", "d2", "cqi_predict", 200.0, cost_per_stage=10.0),
            WorkerBid("w2", "d2", "anomaly_detect", 50.0, cost_per_stage=1.0),
        ]
        demand = {"cqi_predict": 1, "anomaly_detect": 1}
        prices = compute_clearing_prices(bids, demand)

        # WAN cost = 2.0 (very low)
        placement = market_mode_placement(
            dag, topo, gov, prices, wan_cost=2.0, local_domain="d1",
        )
        # cqi_predict: local=1.0 vs remote=10.0+2.0=12.0 → local (w1)
        # anomaly_detect: local=10.0 vs remote=1.0+2.0=3.0 → remote (w2)
        assert placement["s1"] == "w1"
        assert placement["s2"] == "w2"

    def test_pipeline_rejected_when_cost_exceeds_budget(self):
        """Pipeline with insufficient budget is rejected (returns None)."""
        topo, w1, w2 = _make_two_domain_setup()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline(value_budget=0.5)  # Very low budget

        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=5.0),
            WorkerBid("w1", "d1", "anomaly_detect", 200.0, cost_per_stage=5.0),
        ]
        demand = {"cqi_predict": 1, "anomaly_detect": 1}
        prices = compute_clearing_prices(bids, demand)

        placement = market_mode_placement(
            dag, topo, gov, prices, wan_cost=100.0, local_domain="d1",
        )
        # Total cost = 5.0 + 5.0 = 10.0 > budget 0.5 → rejected
        assert placement is None

    def test_no_budget_always_accepts(self):
        """Pipeline without value_budget (legacy) always accepted."""
        topo, w1, w2 = _make_two_domain_setup()
        gov = GovernancePolicy()
        dag = _make_two_stage_pipeline(value_budget=None)

        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=999.0),
            WorkerBid("w1", "d1", "anomaly_detect", 200.0, cost_per_stage=999.0),
        ]
        demand = {"cqi_predict": 1, "anomaly_detect": 1}
        prices = compute_clearing_prices(bids, demand)

        placement = market_mode_placement(
            dag, topo, gov, prices, wan_cost=100.0, local_domain="d1",
        )
        assert placement is not None
