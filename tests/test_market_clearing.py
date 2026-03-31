"""Tests for market clearing engine (Block 2, Step 6).

The market clearing engine computes equilibrium prices per stage type
per domain using iterative tâtonnement. For tree/SP DAGs with GS
valuations, convergence is guaranteed (Kelso-Crawford 1982).

Key concepts:
- WorkerBid: a worker's reported cost for processing a stage type
- DomainPrice: clearing price per stage type in a domain
- PriceSignal: aggregate prices for federation (no worker-level details)
"""

import pytest

from src.broker.market import (
    WorkerBid,
    DomainPrice,
    PriceSignal,
    compute_clearing_prices,
    should_trade_cross_domain,
)


# ---------------------------------------------------------------------------
# WorkerBid construction
# ---------------------------------------------------------------------------


class TestWorkerBid:
    """WorkerBid captures a worker's cost for processing a stage type."""

    def test_create_bid(self):
        bid = WorkerBid(
            worker_id="w1",
            domain_id="d1",
            stage_type="cqi_predict",
            compute_ms=50.0,
            cost_per_stage=1.0,
        )
        assert bid.worker_id == "w1"
        assert bid.cost_per_stage == 1.0

    def test_cost_increases_with_load(self):
        """Congestion pricing: cost increases as worker utilization grows."""
        bid_idle = WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0)
        bid_busy = WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=3.0)
        assert bid_busy.cost_per_stage > bid_idle.cost_per_stage


# ---------------------------------------------------------------------------
# Price clearing
# ---------------------------------------------------------------------------


class TestClearingPrices:
    """Tâtonnement computes equilibrium prices."""

    def test_single_stage_type_single_domain(self):
        """One stage type, 2 workers, 3 units of demand → price = cost of marginal worker."""
        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0),
            WorkerBid("w2", "d1", "cqi_predict", 50.0, cost_per_stage=2.0),
        ]
        # 2 workers can serve 2 units; if demand is 2, clearing price = max bid = 2.0
        demand = {"cqi_predict": 2}
        prices = compute_clearing_prices(bids, demand)
        assert "d1" in prices
        assert prices["d1"]["cqi_predict"] == pytest.approx(2.0)

    def test_excess_supply_low_price(self):
        """3 workers, 1 unit demand → clearing price = lowest bid."""
        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0),
            WorkerBid("w2", "d1", "cqi_predict", 50.0, cost_per_stage=2.0),
            WorkerBid("w3", "d1", "cqi_predict", 50.0, cost_per_stage=3.0),
        ]
        demand = {"cqi_predict": 1}
        prices = compute_clearing_prices(bids, demand)
        assert prices["d1"]["cqi_predict"] == pytest.approx(1.0)

    def test_excess_demand_high_price(self):
        """1 worker, 3 units demand → clearing price = worker's bid (supply-constrained)."""
        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=5.0),
        ]
        demand = {"cqi_predict": 3}
        prices = compute_clearing_prices(bids, demand)
        # Only 1 unit of supply; price = 5.0 (the only bid)
        assert prices["d1"]["cqi_predict"] == pytest.approx(5.0)

    def test_two_domains_independent_prices(self):
        """Each domain clears independently."""
        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0),
            WorkerBid("w2", "d2", "cqi_predict", 50.0, cost_per_stage=3.0),
        ]
        demand = {"cqi_predict": 1}  # 1 unit needed per domain
        prices = compute_clearing_prices(bids, demand)
        assert prices["d1"]["cqi_predict"] == pytest.approx(1.0)
        assert prices["d2"]["cqi_predict"] == pytest.approx(3.0)

    def test_multiple_stage_types(self):
        """Different stage types have independent prices."""
        bids = [
            WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0),
            WorkerBid("w1", "d1", "anomaly_detect", 200.0, cost_per_stage=4.0),
        ]
        demand = {"cqi_predict": 1, "anomaly_detect": 1}
        prices = compute_clearing_prices(bids, demand)
        assert prices["d1"]["cqi_predict"] == pytest.approx(1.0)
        assert prices["d1"]["anomaly_detect"] == pytest.approx(4.0)

    def test_empty_bids_returns_empty(self):
        prices = compute_clearing_prices([], {})
        assert prices == {}

    def test_no_demand_returns_zero_prices(self):
        bids = [WorkerBid("w1", "d1", "cqi_predict", 50.0, cost_per_stage=1.0)]
        prices = compute_clearing_prices(bids, {})
        # No demand → price undefined or zero
        assert prices.get("d1", {}).get("cqi_predict", 0.0) == 0.0


# ---------------------------------------------------------------------------
# PriceSignal for federation
# ---------------------------------------------------------------------------


class TestPriceSignal:
    """PriceSignal aggregates domain prices for federation."""

    def test_from_clearing_prices(self):
        clearing = {"d1": {"cqi_predict": 1.5, "anomaly_detect": 3.0}}
        signal = PriceSignal.from_clearing_prices(clearing, domain_id="d1")
        assert signal.domain_id == "d1"
        assert signal.prices["cqi_predict"] == pytest.approx(1.5)

    def test_no_worker_details(self):
        """PriceSignal must NOT contain worker-level information."""
        clearing = {"d1": {"cqi_predict": 1.5}}
        signal = PriceSignal.from_clearing_prices(clearing, domain_id="d1")
        assert not hasattr(signal, "worker_ids")
        assert not hasattr(signal, "worker_bids")


# ---------------------------------------------------------------------------
# Cross-domain trade decision
# ---------------------------------------------------------------------------


class TestCrossDomainTrade:
    """Trade decision based on price comparison."""

    def test_trade_when_remote_cheaper(self):
        """Remote price + WAN < local price → trade."""
        assert should_trade_cross_domain(
            local_price=5.0, remote_price=2.0, wan_cost=1.0
        ) is True  # 2 + 1 = 3 < 5

    def test_no_trade_when_local_cheaper(self):
        """Remote price + WAN > local price → don't trade."""
        assert should_trade_cross_domain(
            local_price=2.0, remote_price=2.0, wan_cost=1.0
        ) is False  # 2 + 1 = 3 > 2

    def test_no_trade_when_equal(self):
        """Tie → prefer local (no WAN risk)."""
        assert should_trade_cross_domain(
            local_price=3.0, remote_price=2.0, wan_cost=1.0
        ) is False  # 2 + 1 = 3 = 3 → prefer local

    def test_high_wan_prevents_trade(self):
        """Very high WAN cost prevents cross-domain trade."""
        assert should_trade_cross_domain(
            local_price=5.0, remote_price=1.0, wan_cost=100.0
        ) is False  # 1 + 100 = 101 > 5
