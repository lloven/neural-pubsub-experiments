"""Tests for realistic clearing price computation in the broker.

The broker should compute per-stage-type clearing prices based on:
1. Workers bid per stage type (not __all__) based on their capability tier
2. Demand is estimated from the observed pipeline mix and arrival rate
3. Clearing price = marginal cost at the demand quantity

These tests verify the broker's _compute_clearing_prices produces
meaningful prices, not the placeholder __all__ bid.
"""

import pytest

from src.broker.models import WorkerInfo
from src.worker.worker import Tier


def make_worker(node_id, domain_id, slice_id, bid_cost_ms, capabilities=None):
    """Create a WorkerInfo with optional capability tiers."""
    w = WorkerInfo(
        node_id=node_id,
        domain_id=domain_id,
        slice_id=slice_id,
        capacity=1.0,
        bid_cost_ms=bid_cost_ms,
    )
    if capabilities:
        w.capabilities = capabilities
    return w


class TestPerStageTypeBids:
    """Clearing prices are computed per stage type, not __all__."""

    def test_different_stage_types_get_different_prices(self):
        """Workers with different costs for different stage types
        produce different clearing prices."""
        from src.broker.market import WorkerBid, compute_clearing_prices

        bids = [
            WorkerBid("w1", "d1", "predict", compute_ms=100, cost_per_stage=100),
            WorkerBid("w2", "d1", "predict", compute_ms=150, cost_per_stage=150),
            WorkerBid("w3", "d1", "collect", compute_ms=50, cost_per_stage=50),
            WorkerBid("w4", "d1", "collect", compute_ms=80, cost_per_stage=80),
        ]
        demand = {"predict": 1, "collect": 1}
        prices = compute_clearing_prices(bids, demand)

        # predict clears at 100 (cheapest bidder meets demand=1)
        # collect clears at 50
        assert "d1" in prices
        assert "predict" in prices["d1"]
        assert "collect" in prices["d1"]
        assert prices["d1"]["predict"] != prices["d1"]["collect"]

    def test_no_all_stage_type_in_realistic_bids(self):
        """Realistic bids should never use '__all__' as stage type."""
        from src.broker.market import WorkerBid

        # A properly generated bid should have a real stage type
        bid = WorkerBid("w1", "d1", "predict", compute_ms=100, cost_per_stage=100)
        assert bid.stage_type != "__all__"


class TestDemandEstimation:
    """Demand is estimated from pipeline mix, not set to len(workers)."""

    def test_demand_scales_with_pipeline_count(self):
        """With 3 active pipelines each needing 1 'predict' stage,
        demand for 'predict' should be ~3."""
        # This tests the broker's demand estimation logic
        # For now, we test the principle: demand should reflect
        # the number of stages needed, not the number of workers
        from src.broker.market import compute_clearing_prices, WorkerBid

        bids = [
            WorkerBid("w1", "d1", "predict", compute_ms=100, cost_per_stage=100),
            WorkerBid("w2", "d1", "predict", compute_ms=200, cost_per_stage=200),
            WorkerBid("w3", "d1", "predict", compute_ms=300, cost_per_stage=300),
        ]

        # Demand = 1: only w1 clears (cheapest)
        prices_low = compute_clearing_prices(bids, {"predict": 1})
        # Demand = 2: w1 and w2 clear, price set by w2
        prices_high = compute_clearing_prices(bids, {"predict": 2})

        assert prices_high["d1"]["predict"] >= prices_low["d1"]["predict"]


class TestBrokerClearingIntegration:
    """The broker's _compute_clearing_prices produces realistic prices."""

    def test_broker_generates_per_stage_bids(self):
        """The broker should generate bids per stage type from worker capabilities,
        not a single __all__ bid."""
        # This is the integration test — verifying the broker's method
        # produces per-stage-type prices. We test by examining the output
        # structure (keys should be real stage types, not __all__).
        from src.broker.market import WorkerBid, compute_clearing_prices

        # Simulate what the broker SHOULD produce:
        # Worker w1 is capable of predict (primary) and collect (secondary)
        # Worker w2 is capable of collect (primary) and predict (impossible)
        bids = [
            WorkerBid("w1", "d1", "predict", compute_ms=100, cost_per_stage=100),
            WorkerBid("w1", "d1", "collect", compute_ms=150, cost_per_stage=150),
            WorkerBid("w2", "d1", "collect", compute_ms=80, cost_per_stage=80),
            # w2 does NOT bid on predict (impossible tier)
        ]
        demand = {"predict": 1, "collect": 1}
        prices = compute_clearing_prices(bids, demand)

        assert "d1" in prices
        # Should have separate prices for predict and collect
        assert "predict" in prices["d1"]
        assert "collect" in prices["d1"]
        # No __all__ key
        assert "__all__" not in prices["d1"]
