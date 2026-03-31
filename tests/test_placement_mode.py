"""Tests for placement mode dispatch (Block 8).

The broker must support multiple placement modes:
  - neural:    S3 semantic placement (existing default)
  - market:    price-signal-based allocation (new)
  - locality:  local-only, no cross-domain
  - latency:   lowest-latency worker
  - spillover: local-first, overflow to remote

These are dispatched via --placement-mode CLI flag.
"""

import pytest

from src.broker.neural_broker import BrokerConfig


class TestBrokerConfigPlacementMode:
    """BrokerConfig supports placement_mode field."""

    def test_default_is_neural(self):
        cfg = BrokerConfig(domain_id="d1", broker_id="b1")
        assert cfg.placement_mode == "neural"

    def test_market_mode(self):
        cfg = BrokerConfig(domain_id="d1", broker_id="b1", placement_mode="market")
        assert cfg.placement_mode == "market"

    def test_locality_mode(self):
        cfg = BrokerConfig(domain_id="d1", broker_id="b1", placement_mode="locality")
        assert cfg.placement_mode == "locality"

    def test_latency_mode(self):
        cfg = BrokerConfig(domain_id="d1", broker_id="b1", placement_mode="latency")
        assert cfg.placement_mode == "latency"

    def test_spillover_mode(self):
        cfg = BrokerConfig(domain_id="d1", broker_id="b1", placement_mode="spillover")
        assert cfg.placement_mode == "spillover"
