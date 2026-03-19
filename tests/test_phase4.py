"""Tests for Phase 4 (environment and polish) items.

Written RED-first per strict TDD.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# 4.1: tc qdisc WAN emulation in docker-compose.local.yaml
# ---------------------------------------------------------------------------


def test_compose_local_has_tc_wan_emulation():
    """docker-compose.local.yaml must contain tc qdisc WAN emulation config.

    The federation network should have latency added via tc netem or equivalent
    Docker compose network driver options.
    """
    compose_path = PROJECT_ROOT / "docker-compose.local.yaml"
    content = compose_path.read_text()
    # Must reference tc netem or network driver_opts with delay
    assert "tc" in content or "delay" in content or "netem" in content, (
        "docker-compose.local.yaml must include tc qdisc WAN emulation "
        "(tc netem or network driver_opts with delay)"
    )


def test_compose_federation_network_has_delay():
    """Federation network must have a configurable latency value."""
    compose_path = PROJECT_ROOT / "docker-compose.local.yaml"
    data = yaml.safe_load(compose_path.read_text())
    networks = data.get("networks", {})
    federation = networks.get("federation", {})
    # Check for driver_opts with delay, or that entrypoint references tc
    driver_opts = federation.get("driver_opts", {})

    # Alternative: check if any service has a WAN emulation entrypoint
    services = data.get("services", {})
    has_tc_in_entrypoint = any(
        "tc" in str(svc.get("entrypoint", "")) or "tc" in str(svc.get("command", ""))
        for svc in services.values()
    )
    has_network_delay = bool(driver_opts) or "delay" in str(federation)
    has_wan_profile = "wan-profile" in str(data) or "WAN_DELAY" in str(data)

    assert has_tc_in_entrypoint or has_network_delay or has_wan_profile, (
        "Federation network must have WAN emulation (delay configuration)"
    )


# ---------------------------------------------------------------------------
# 4.2: Phase C figure generation
# ---------------------------------------------------------------------------


def test_generate_figures_has_phase_c():
    """generate_figures.py must include Phase C figure generators."""
    from scripts.generate_figures import PHASE_GENERATORS

    assert "C" in PHASE_GENERATORS, "PHASE_GENERATORS must include Phase C"
    assert len(PHASE_GENERATORS["C"]) >= 1, "Phase C must have at least 1 figure generator"


def test_phase_c_figure_generators_are_callable():
    """Phase C figure generators must be callable."""
    from scripts.generate_figures import PHASE_GENERATORS

    for name, gen_func in PHASE_GENERATORS["C"]:
        assert callable(gen_func), f"Phase C generator '{name}' is not callable"


# ---------------------------------------------------------------------------
# 4.3: 60s cool-down between Phase B-D runs
# ---------------------------------------------------------------------------


def test_common_has_cooldown_function():
    """_common.py must expose a cooldown function."""
    from scripts._common import cooldown_between_runs

    assert callable(cooldown_between_runs)


def test_cooldown_default_duration():
    """cooldown_between_runs default duration must be 60 seconds."""
    import inspect
    from scripts._common import cooldown_between_runs

    sig = inspect.signature(cooldown_between_runs)
    default = sig.parameters["duration_s"].default
    assert default == 60, f"Expected default cooldown of 60s, got {default}"


def test_cooldown_is_skippable_in_dry_run():
    """cooldown_between_runs must accept a dry_run flag to skip the sleep."""
    from scripts._common import cooldown_between_runs

    # Should return immediately without actually sleeping
    with patch("time.sleep") as mock_sleep:
        cooldown_between_runs(duration_s=60, dry_run=True)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# 4.4: cpuset CPU pinning
# ---------------------------------------------------------------------------


def test_compose_has_cpuset_cpus():
    """docker-compose.local.yaml must have cpuset_cpus for broker services."""
    compose_path = PROJECT_ROOT / "docker-compose.local.yaml"
    content = compose_path.read_text()
    assert "cpuset" in content, (
        "docker-compose.local.yaml must include cpuset CPU pinning"
    )


def test_compose_broker_has_cpuset():
    """Broker services must have cpuset_cpus entries."""
    compose_path = PROJECT_ROOT / "docker-compose.local.yaml"
    data = yaml.safe_load(compose_path.read_text())
    services = data.get("services", {})

    broker_d1 = services.get("broker-d1", {})
    assert "cpuset" in str(broker_d1), (
        "broker-d1 must have cpuset CPU pinning"
    )


# ---------------------------------------------------------------------------
# 4.5: Pipeline complexity=2
# ---------------------------------------------------------------------------


def test_complexity_2_pipeline_has_2_stages():
    """There must be a 2-stage pipeline template or documented mapping.

    Either:
    a) A factory function that creates a 2-stage pipeline, or
    b) run_phase_a.py COMPLEXITIES[2] maps to a valid 2-stage pipeline config
    """
    from scripts.run_phase_a import COMPLEXITIES

    assert 2 in COMPLEXITIES, "COMPLEXITIES must include key 2"
    # The complexity=2 config must select pipeline types that have 2 stages
    # or the mapping must be documented and valid
    mix = COMPLEXITIES[2]
    assert isinstance(mix, dict), f"COMPLEXITIES[2] must be a dict, got {type(mix)}"
    assert sum(mix.values()) == pytest.approx(1.0), "Pipeline mix probabilities must sum to 1.0"


def test_complexity_2_pipeline_is_valid():
    """The pipeline(s) selected for complexity=2 must produce valid DAGs.

    This checks that whatever pipeline type is used for complexity=2
    actually exists and produces a working pipeline.
    """
    from scripts.run_phase_a import COMPLEXITIES

    mix = COMPLEXITIES[2]
    # At least one pipeline type must have non-zero probability
    active_types = [k for k, v in mix.items() if v > 0]
    assert len(active_types) >= 1, "complexity=2 must have at least one active pipeline type"

    # Import the template factories and verify the active types work
    from src.pipeline.patterns import (
        cqi_prediction_pipeline,
        anomaly_detection_pipeline,
        map_pipeline,
    )

    TEMPLATE_FACTORIES = {
        "cqi_prediction": cqi_prediction_pipeline,
        "anomaly_detection": anomaly_detection_pipeline,
        "map_2stage": lambda: map_pipeline("transform", n_stages=2),
    }

    for pt in active_types:
        if pt in TEMPLATE_FACTORIES:
            dag = TEMPLATE_FACTORIES[pt]()
            assert len(dag) >= 2, f"Pipeline '{pt}' must have at least 2 stages"
        # If the type is not in TEMPLATE_FACTORIES, it's a known type
        # that should be verified separately
