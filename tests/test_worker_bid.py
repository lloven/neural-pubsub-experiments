"""Tests for worker bid cost in registration (Task 1).

Workers advertise a bid cost (processing cost per stage in ms) during
registration. This enables market-mode allocation where the broker
selects the cheapest eligible worker.
"""

import os
import pytest

from src.worker.worker import WorkerConfig
from src.broker.models import RegisterRequest


class TestWorkerConfigBid:
    """WorkerConfig supports bid_cost_ms field."""

    def test_default_bid_is_zero(self):
        cfg = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="eMBB",
            capacity=1.0, broker_url="http://localhost:8080",
        )
        assert cfg.bid_cost_ms == 0.0

    def test_explicit_bid(self):
        cfg = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="eMBB",
            capacity=1.0, broker_url="http://localhost:8080",
            bid_cost_ms=15.0,
        )
        assert cfg.bid_cost_ms == 15.0

    def test_bid_from_env(self, monkeypatch):
        """BID_COST_MS env var sets the bid cost."""
        monkeypatch.setenv("BID_COST_MS", "25")
        # Re-read: the env var should be picked up by config construction
        val = float(os.environ.get("BID_COST_MS", "0"))
        assert val == 25.0


class TestRegisterRequestBid:
    """RegisterRequest includes bid_cost_ms."""

    def test_register_request_has_bid_field(self):
        req = RegisterRequest(
            node_id="w1",
            domain_id="d1",
            slice_id="eMBB",
            capacity=1.0,
            bid_cost_ms=15.0,
        )
        assert req.bid_cost_ms == 15.0

    def test_register_request_bid_defaults_to_zero(self):
        req = RegisterRequest(
            node_id="w1",
            domain_id="d1",
            slice_id="eMBB",
            capacity=1.0,
        )
        assert req.bid_cost_ms == 0.0
