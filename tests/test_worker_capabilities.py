"""Tests for tiered worker capabilities (Block 1, Step 1).

Workers have per-stage-type capability tiers:
- Primary: fast processing (e.g., 50ms)
- Secondary: slow but possible (e.g., 200ms)
- Tertiary (impossible): worker rejects the stage

The WORKER_CAPABILITIES env var configures this as JSON:
    {"cqi_predict": {"tier": "primary", "compute_ms": 50},
     "anomaly_detect": {"tier": "secondary", "compute_ms": 200},
     "sensor_fuse": {"tier": "impossible"}}

When no WORKER_CAPABILITIES is set, the worker falls back to the
existing processing_speed * computational_demand behavior (backward
compatibility).
"""

import json
import pytest

from src.worker.worker import WorkerConfig, parse_capabilities, Capability, Tier


# ---------------------------------------------------------------------------
# Capability parsing
# ---------------------------------------------------------------------------


class TestParseCapabilities:
    """Parse WORKER_CAPABILITIES JSON into structured data."""

    def test_parse_primary_tier(self):
        raw = json.dumps({"cqi_predict": {"tier": "primary", "compute_ms": 50}})
        caps = parse_capabilities(raw)
        assert caps["cqi_predict"].tier == Tier.PRIMARY
        assert caps["cqi_predict"].compute_ms == 50

    def test_parse_secondary_tier(self):
        raw = json.dumps({"anomaly_detect": {"tier": "secondary", "compute_ms": 200}})
        caps = parse_capabilities(raw)
        assert caps["anomaly_detect"].tier == Tier.SECONDARY
        assert caps["anomaly_detect"].compute_ms == 200

    def test_parse_impossible_tier(self):
        raw = json.dumps({"sensor_fuse": {"tier": "impossible"}})
        caps = parse_capabilities(raw)
        assert caps["sensor_fuse"].tier == Tier.IMPOSSIBLE
        assert caps["sensor_fuse"].compute_ms is None

    def test_parse_multiple_types(self):
        raw = json.dumps({
            "cqi_predict": {"tier": "primary", "compute_ms": 50},
            "anomaly_detect": {"tier": "secondary", "compute_ms": 200},
            "sensor_fuse": {"tier": "impossible"},
        })
        caps = parse_capabilities(raw)
        assert len(caps) == 3
        assert caps["cqi_predict"].tier == Tier.PRIMARY
        assert caps["sensor_fuse"].tier == Tier.IMPOSSIBLE

    def test_parse_empty_returns_empty(self):
        caps = parse_capabilities("")
        assert caps == {}

    def test_parse_none_returns_empty(self):
        caps = parse_capabilities(None)
        assert caps == {}

    def test_invalid_tier_raises(self):
        raw = json.dumps({"foo": {"tier": "unknown", "compute_ms": 100}})
        with pytest.raises(ValueError, match="unknown"):
            parse_capabilities(raw)


# ---------------------------------------------------------------------------
# Compute time resolution
# ---------------------------------------------------------------------------


class TestComputeTimeResolution:
    """Worker resolves compute time per stage type using capabilities."""

    @pytest.fixture
    def capabilities(self):
        return parse_capabilities(json.dumps({
            "cqi_predict": {"tier": "primary", "compute_ms": 50},
            "anomaly_detect": {"tier": "secondary", "compute_ms": 200},
            "sensor_fuse": {"tier": "impossible"},
        }))

    def test_primary_tier_returns_fast_time(self, capabilities):
        ms = Capability.resolve_compute_ms(capabilities, "cqi_predict")
        assert ms == 50

    def test_secondary_tier_returns_slow_time(self, capabilities):
        ms = Capability.resolve_compute_ms(capabilities, "anomaly_detect")
        assert ms == 200

    def test_impossible_tier_returns_none(self, capabilities):
        ms = Capability.resolve_compute_ms(capabilities, "sensor_fuse")
        assert ms is None  # Worker should reject this stage

    def test_unknown_stage_type_uses_default(self, capabilities):
        """Stage type not in capabilities → use fallback (processing_speed * demand)."""
        ms = Capability.resolve_compute_ms(capabilities, "unknown_stage")
        assert ms is None  # Falls back to legacy behavior

    def test_empty_capabilities_always_returns_none(self):
        """No capabilities configured → all stages use legacy behavior."""
        caps = parse_capabilities(None)
        ms = Capability.resolve_compute_ms(caps, "cqi_predict")
        assert ms is None


# ---------------------------------------------------------------------------
# Stage rejection for impossible tiers
# ---------------------------------------------------------------------------


class TestStageRejection:
    """Worker rejects stages for which it has tier=impossible."""

    @pytest.fixture
    def capabilities(self):
        return parse_capabilities(json.dumps({
            "cqi_predict": {"tier": "primary", "compute_ms": 50},
            "sensor_fuse": {"tier": "impossible"},
        }))

    def test_can_execute_primary(self, capabilities):
        assert Capability.can_execute(capabilities, "cqi_predict") is True

    def test_cannot_execute_impossible(self, capabilities):
        assert Capability.can_execute(capabilities, "sensor_fuse") is False

    def test_unknown_stage_can_execute(self, capabilities):
        """Unknown stages are executable (backward compatibility)."""
        assert Capability.can_execute(capabilities, "new_stage") is True

    def test_empty_capabilities_can_execute_anything(self):
        caps = parse_capabilities(None)
        assert Capability.can_execute(caps, "anything") is True
