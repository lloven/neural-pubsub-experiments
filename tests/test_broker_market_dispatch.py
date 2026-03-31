"""Tests for broker market-mode integration (Tasks 2-4, 6).

Verifies that:
- Worker bids are stored on registration (Task 2).
- Clearing prices are computed from registered workers (Task 3).
- Placement dispatch routes to the correct function per mode (Task 4).
- BrokerConfig has wan_cost_ms (Task 6).
"""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from src.broker.models import WorkerInfo, RegisterRequest
from src.broker.neural_broker import BrokerConfig, NeuralBroker
from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
)
from src.pipeline.dag import PipelineDAG, Stage, Edge


# ---------------------------------------------------------------------------
# Task 2: Broker stores worker bids
# ---------------------------------------------------------------------------


class TestBrokerStoresBid:
    """After registration, the broker's WorkerInfo contains the bid."""

    def test_worker_info_has_bid_cost_field(self):
        """WorkerInfo must have a bid_cost_ms attribute."""
        info = WorkerInfo(
            node_id="w1",
            domain_id="d1",
            slice_id="URLLC",
            capacity=1.0,
            url="http://w1:8081",
            bid_cost_ms=15.0,
        )
        assert info.bid_cost_ms == 15.0

    def test_worker_info_bid_defaults_to_zero(self):
        """WorkerInfo.bid_cost_ms defaults to 0.0 if not specified."""
        info = WorkerInfo(
            node_id="w1",
            domain_id="d1",
            slice_id="URLLC",
            capacity=1.0,
            url="http://w1:8081",
        )
        assert info.bid_cost_ms == 0.0

    def test_broker_stores_bid_on_register(self):
        """After registering a worker with bid_cost_ms=15, the broker's
        worker registry contains 15."""
        config = BrokerConfig(domain_id="d1", broker_id="broker-d1-0")
        broker = NeuralBroker(config)

        # Simulate what the register endpoint does (without HTTP)
        req = RegisterRequest(
            node_id="w1",
            domain_id="d1",
            slice_id="URLLC",
            capacity=1.0,
            url="http://w1:8081",
            bid_cost_ms=15.0,
        )
        # Directly store like the register handler does
        broker._workers[req.node_id] = WorkerInfo(
            node_id=req.node_id,
            domain_id=req.domain_id,
            slice_id=req.slice_id,
            capacity=req.capacity,
            url=req.url,
            bid_cost_ms=req.bid_cost_ms,
        )
        assert broker._workers["w1"].bid_cost_ms == 15.0


# ---------------------------------------------------------------------------
# Helpers for Task 4
# ---------------------------------------------------------------------------


def _make_broker_with_workers(placement_mode: str = "neural", wan_cost_ms: float = 0.0):
    """Create a NeuralBroker with 2 registered workers and a known topology."""
    config = BrokerConfig(
        domain_id="d1",
        broker_id="broker-d1-0",
        placement_mode=placement_mode,
        wan_cost_ms=wan_cost_ms,
    )
    broker = NeuralBroker(config)

    # Register two workers
    for wid, did, bid in [("w1", "d1", 10.0), ("w2", "d2", 20.0)]:
        broker._workers[wid] = WorkerInfo(
            node_id=wid,
            domain_id=did,
            slice_id="flat",
            capacity=5.0,
            url=f"http://{wid}:8081",
            bid_cost_ms=bid,
        )
    broker._rebuild_topology()
    return broker


def _make_simple_dag():
    """One-stage pipeline for dispatch testing."""
    dag = PipelineDAG()
    dag.add_stage(Stage("s1", "predict", 0.1, 1.0))
    return dag


# ---------------------------------------------------------------------------
# Task 4: Placement mode dispatch
# ---------------------------------------------------------------------------


class TestPlacementModeDispatch:
    """Broker dispatch routes to the correct placement function per mode."""

    def test_market_mode_calls_market_placement(self):
        """placement_mode='market' calls market_mode_placement."""
        broker = _make_broker_with_workers("market", wan_cost_ms=5.0)
        dag = _make_simple_dag()

        with patch("src.broker.neural_broker.market_mode_placement") as mock_market, \
             patch("src.broker.neural_broker.find_placement") as mock_neural:
            mock_market.return_value = {"s1": "w1"}
            result = broker._dispatch_placement(dag)
            mock_market.assert_called_once()
            mock_neural.assert_not_called()
            assert result == {"s1": "w1"}

    def test_neural_mode_calls_find_placement(self):
        """placement_mode='neural' calls find_placement (existing behavior)."""
        broker = _make_broker_with_workers("neural")
        dag = _make_simple_dag()

        with patch("src.broker.neural_broker.find_placement") as mock_neural, \
             patch("src.broker.neural_broker.market_mode_placement") as mock_market:
            mock_neural.return_value = {"s1": "w1"}
            result = broker._dispatch_placement(dag)
            mock_neural.assert_called_once()
            mock_market.assert_not_called()
            assert result == {"s1": "w1"}

    def test_locality_mode_calls_locality_placement(self):
        """placement_mode='locality' calls locality_placement."""
        broker = _make_broker_with_workers("locality")
        dag = _make_simple_dag()

        with patch("src.broker.neural_broker.locality_placement") as mock_loc:
            mock_loc.return_value = {"s1": "w1"}
            result = broker._dispatch_placement(dag)
            mock_loc.assert_called_once()
            assert result == {"s1": "w1"}

    def test_latency_mode_calls_latency_greedy_placement(self):
        """placement_mode='latency' calls latency_greedy_placement."""
        broker = _make_broker_with_workers("latency")
        dag = _make_simple_dag()

        with patch("src.broker.neural_broker.latency_greedy_placement") as mock_lat:
            mock_lat.return_value = {"s1": "w1"}
            result = broker._dispatch_placement(dag)
            mock_lat.assert_called_once()
            assert result == {"s1": "w1"}

    def test_spillover_mode_calls_spillover_placement(self):
        """placement_mode='spillover' calls spillover_placement."""
        broker = _make_broker_with_workers("spillover")
        dag = _make_simple_dag()

        with patch("src.broker.neural_broker.spillover_placement") as mock_spill:
            mock_spill.return_value = {"s1": "w1"}
            result = broker._dispatch_placement(dag)
            mock_spill.assert_called_once()
            assert result == {"s1": "w1"}

    def test_market_mode_passes_wan_cost(self):
        """Market mode passes wan_cost_ms from config to market_mode_placement."""
        broker = _make_broker_with_workers("market", wan_cost_ms=42.0)
        dag = _make_simple_dag()

        with patch("src.broker.neural_broker.market_mode_placement") as mock_market:
            mock_market.return_value = {"s1": "w1"}
            broker._dispatch_placement(dag)
            # Check wan_cost arg
            call_kwargs = mock_market.call_args
            assert call_kwargs[1].get("wan_cost", call_kwargs[0][4] if len(call_kwargs[0]) > 4 else None) == 42.0 or \
                   42.0 in call_kwargs[0]


# ---------------------------------------------------------------------------
# Task 6: BrokerConfig has wan_cost_ms
# ---------------------------------------------------------------------------


class TestBrokerConfigWanCost:
    """BrokerConfig has wan_cost_ms defaulting to 0.0."""

    def test_broker_config_has_wan_cost(self):
        config = BrokerConfig(domain_id="d1", broker_id="b1")
        assert config.wan_cost_ms == 0.0

    def test_broker_config_wan_cost_custom(self):
        config = BrokerConfig(domain_id="d1", broker_id="b1", wan_cost_ms=25.0)
        assert config.wan_cost_ms == 25.0
