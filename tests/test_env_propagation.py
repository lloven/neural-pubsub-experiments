"""Tests for environment variable propagation in runner _COMPOSE_MAPs.

Each runner (baseline, slicing) defines a _COMPOSE_MAP that maps config
names to compose overlays and env overrides. These tests verify that the
correct broker module and placement strategy are set for each config.

Previously, this tested the now-removed resolve_config() / _CONFIG_TABLE.
The invariants are the same; the source of truth moved to per-runner maps.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Baseline runner env propagation
# ---------------------------------------------------------------------------


class TestBaselineEnvPropagation:
    """Baseline _COMPOSE_MAP must set correct broker/placement for each config."""

    def test_rr_sets_static_broker(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert _COMPOSE_MAP["rr"]["env"]["BROKER_MODULE"] == "src.broker.static_broker"

    def test_rr_sets_round_robin_placement(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert _COMPOSE_MAP["rr"]["env"]["PLACEMENT"] == "round_robin"

    def test_random_sets_static_broker(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert _COMPOSE_MAP["random"]["env"]["BROKER_MODULE"] == "src.broker.static_broker"

    def test_random_sets_random_placement(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert _COMPOSE_MAP["random"]["env"]["PLACEMENT"] == "random"

    def test_neural_has_no_broker_override(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert "BROKER_MODULE" not in _COMPOSE_MAP["neural"]["env"]

    def test_neural_has_no_placement_override(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert "PLACEMENT" not in _COMPOSE_MAP["neural"]["env"]

    def test_all_configs_present(self):
        from scripts.run_baseline import _COMPOSE_MAP
        assert set(_COMPOSE_MAP.keys()) == {"rr", "random", "neural"}


# ---------------------------------------------------------------------------
# Slicing runner env propagation
# ---------------------------------------------------------------------------


class TestSlicingEnvPropagation:
    """Slicing _COMPOSE_MAP must set correct broker/placement for each config."""

    def test_rr_sets_static_broker(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert _COMPOSE_MAP["rr"]["env"]["BROKER_MODULE"] == "src.broker.static_broker"

    def test_rr_sets_round_robin_placement(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert _COMPOSE_MAP["rr"]["env"]["PLACEMENT"] == "round_robin"

    def test_neural_has_no_broker_override(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert "BROKER_MODULE" not in _COMPOSE_MAP["neural"]["env"]

    def test_flat_has_no_broker_override(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert "BROKER_MODULE" not in _COMPOSE_MAP["flat"]["env"]

    def test_gov_has_no_broker_override(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert "BROKER_MODULE" not in _COMPOSE_MAP["gov"]["env"]

    def test_gov_fail_has_no_broker_override(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert "BROKER_MODULE" not in _COMPOSE_MAP["gov-fail"]["env"]

    def test_all_configs_present(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert set(_COMPOSE_MAP.keys()) == {"flat", "neural", "rr", "gov", "gov-fail"}

    def test_flat_uses_flat_eq_overlay(self):
        from scripts.run_slicing import _COMPOSE_MAP
        from scripts._common import COMPOSE_FLAT_EQ
        assert COMPOSE_FLAT_EQ in _COMPOSE_MAP["flat"]["overlays"]

    def test_gov_uses_governance_overlay(self):
        from scripts.run_slicing import _COMPOSE_MAP
        from scripts._common import COMPOSE_GOVERNANCE
        assert COMPOSE_GOVERNANCE in _COMPOSE_MAP["gov"]["overlays"]

    def test_neural_has_no_overlays(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert _COMPOSE_MAP["neural"]["overlays"] == []

    def test_rr_has_no_overlays(self):
        from scripts.run_slicing import _COMPOSE_MAP
        assert _COMPOSE_MAP["rr"]["overlays"] == []
