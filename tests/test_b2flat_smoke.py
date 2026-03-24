"""Tests for B2-flat (sliced topology, flat placement) configuration.

B2flat decomposes the network isolation effect from placement intelligence
in Phase B. It uses the full multi-broker, multi-domain sliced topology
(same as B2) but with round-robin placement (ignoring slice affinity).

The comparison becomes:
  B1eq   (flat network, flat placement, 5 workers)   = baseline
  B2flat (sliced network, flat placement, 5 workers)  = infrastructure effect only
  B2     (sliced network, neural placement, 5 workers) = infrastructure + algorithm
  B2 - B2flat = pure algorithm contribution
  B2flat - B1eq = pure infrastructure contribution

Covers:
  - Cycle 1: resolve_config("B2flat") returns correct compose overlays and env
  - Cycle 2: B2flat uses static broker with round-robin placement
  - Cycle 3: B2flat has same worker count and topology as B2
  - Cycle 4: Phase B run matrix includes B2flat with correct parameters
  - Cycle 5: B2flat run passes round-robin placement env to Docker
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from scripts._common import (
    COMPOSE_FILE,
    COMPOSE_FLAT,
    COMPOSE_FLAT_EQ,
    PROJECT_ROOT,
    _CONFIG_TABLE,
)
from scripts.experiment_matrix import expected_run_count


COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"


# ============================================================================
# Cycle 1: resolve_config("B2flat") returns correct compose overlays and env
# ============================================================================


class TestB2flatResolveConfig:
    """B2flat config must resolve to local compose with no flat overlay."""

    def _resolve(self, config_name: str, **kwargs):
        from scripts._common import resolve_config
        return resolve_config(config_name, **kwargs)

    def test_b2flat_is_recognised(self):
        """resolve_config('B2flat') must not raise ValueError."""
        cfg = self._resolve("B2flat")
        assert cfg is not None

    def test_b2flat_uses_no_flat_overlay(self):
        """B2flat must NOT use flat or flat-equalized overlays (sliced topology)."""
        cfg = self._resolve("B2flat")
        overlay_names = [f.name for f in cfg.compose_files]
        assert "docker-compose.flat.yaml" not in overlay_names, (
            f"B2flat should NOT use flat overlay, got: {overlay_names}"
        )
        assert "docker-compose.flat-equalized.yaml" not in overlay_names, (
            f"B2flat should NOT use flat-equalized overlay, got: {overlay_names}"
        )

    def test_b2flat_compose_files_are_local_only(self):
        """B2flat compose_files = [local] (same base topology as B2, no overlays)."""
        cfg = self._resolve("B2flat", transport="http")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    def test_b2flat_env_has_standard_fields(self):
        """B2flat env must have ARRIVAL_RATE, SEED, PIPELINE_STAGES."""
        cfg = self._resolve("B2flat", rate="medium", seed=42, stages=3)
        assert cfg.env["ARRIVAL_RATE"] == "5.0"
        assert cfg.env["SEED"] == "42"
        assert cfg.env["PIPELINE_STAGES"] == "3"


# ============================================================================
# Cycle 2: B2flat uses static broker with round-robin placement
# ============================================================================


class TestB2flatPlacement:
    """B2flat must use the static broker with round-robin placement."""

    def test_b2flat_uses_static_broker(self):
        """B2flat must set BROKER_MODULE to static_broker."""
        entry = _CONFIG_TABLE["B2flat"]
        assert entry["env"]["BROKER_MODULE"] == "src.broker.static_broker"

    def test_b2flat_uses_round_robin_placement(self):
        """B2flat must set PLACEMENT to round_robin."""
        entry = _CONFIG_TABLE["B2flat"]
        assert entry["env"]["PLACEMENT"] == "round_robin"

    def test_b2flat_broker_is_static(self):
        """B2flat broker_module should be src.broker.static_broker."""
        entry = _CONFIG_TABLE["B2flat"]
        assert entry["broker"] == "src.broker.static_broker"

    def test_b2_uses_neural_broker(self):
        """B2 (the comparison) must NOT use static broker (it uses neural)."""
        entry = _CONFIG_TABLE["B2"]
        assert "BROKER_MODULE" not in entry["env"], (
            "B2 should use default neural broker, not static"
        )
        assert entry["broker"] is None


# ============================================================================
# Cycle 3: B2flat has same topology as B2 (same overlays = no overlays)
# ============================================================================


class TestB2flatTopologyMatchesB2:
    """B2flat must use the same Docker topology as B2 (sliced, 2 brokers, 5 workers)."""

    def _resolve(self, config_name: str, **kwargs):
        from scripts._common import resolve_config
        return resolve_config(config_name, **kwargs)

    def test_b2flat_same_overlays_as_b2(self):
        """B2flat and B2 must have the same compose overlays (none)."""
        entry_b2 = _CONFIG_TABLE["B2"]
        entry_b2flat = _CONFIG_TABLE["B2flat"]
        assert entry_b2["overlays"] == entry_b2flat["overlays"], (
            f"B2 overlays={entry_b2['overlays']}, "
            f"B2flat overlays={entry_b2flat['overlays']}"
        )

    def test_b2flat_and_b2_same_compose_files_for_http(self):
        """B2flat and B2 must resolve to the same compose files (HTTP transport)."""
        cfg_b2 = self._resolve("B2", transport="http")
        cfg_b2flat = self._resolve("B2flat", transport="http")
        assert cfg_b2.compose_files == cfg_b2flat.compose_files

    def test_b2flat_different_from_b1eq_overlays(self):
        """B2flat must differ from B1eq (B1eq uses flat-equalized overlay)."""
        entry_b1eq = _CONFIG_TABLE["B1eq"]
        entry_b2flat = _CONFIG_TABLE["B2flat"]
        assert entry_b1eq["overlays"] != entry_b2flat["overlays"], (
            "B2flat should use sliced topology (no overlays), "
            "not flat-equalized overlay like B1eq"
        )


# ============================================================================
# Cycle 4: Phase B run matrix includes B2flat
# ============================================================================


class TestB2flatPhaseB:
    """B2flat must be included in the Phase B run matrix."""

    def test_b2flat_in_phase_b_configs(self):
        """B2flat must be a valid Phase B config."""
        from scripts.run_phase_b import CONFIGS
        assert "B2flat" in CONFIGS, (
            f"B2flat missing from Phase B CONFIGS: {sorted(CONFIGS.keys())}"
        )

    def test_b2flat_config_properties(self):
        """B2flat: 3 slices (same as B2), no governance, no failure injection."""
        from scripts.run_phase_b import CONFIGS
        cfg = CONFIGS["B2flat"]
        assert cfg["num_slices"] == 3
        assert cfg["governance"] is False
        assert cfg["failure_injection"] is False

    def test_b2flat_same_slices_as_b2(self):
        """B2flat must have same num_slices as B2."""
        from scripts.run_phase_b import CONFIGS
        assert CONFIGS["B2flat"]["num_slices"] == CONFIGS["B2"]["num_slices"]

    def test_b2flat_in_run_matrix(self):
        """B2flat appears in the run matrix when selected."""
        from scripts.run_phase_b import build_run_matrix
        runs = build_run_matrix(["B2flat"], [42], transports=["http"])
        assert len(runs) == 1
        assert runs[0].config_name == "B2flat"

    def test_phase_b_matrix_with_b2flat_is_correct_size(self):
        """All Phase B configs x 1 transport (HTTP) x seeds = expected runs."""
        from scripts.run_phase_b import build_run_matrix, CONFIGS
        runs = build_run_matrix(
            sorted(CONFIGS.keys()),
            [42, 123, 456, 789, 0],
            transports=["http"],
        )
        expected = expected_run_count("B", transports=["http"])
        assert len(runs) == expected, (
            f"Expected {expected} runs, got {len(runs)}"
        )


# ============================================================================
# Cycle 5: B2flat run passes round-robin placement env to Docker
# ============================================================================


class TestB2flatRunExecution:
    """B2flat _run must pass static broker + round-robin placement to Docker."""

    def test_b2flat_run_sets_placement_strategy(self):
        """B2flat run must set PLACEMENT_STRATEGY to round_robin (not neural)."""
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS["B2flat"]
        run = RunConfig(
            config_name="B2flat",
            seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_phase_b.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        env = captured["env"]
        # B2flat should override the default "neural" PLACEMENT_STRATEGY
        # The static broker reads PLACEMENT env var for its strategy
        assert env.get("BROKER_MODULE") == "src.broker.static_broker", (
            f"B2flat must use static broker, got BROKER_MODULE={env.get('BROKER_MODULE')}"
        )
        assert env.get("PLACEMENT") == "round_robin", (
            f"B2flat must use round_robin placement, got PLACEMENT={env.get('PLACEMENT')}"
        )

    def test_b2flat_run_uses_local_compose_only(self):
        """B2flat run must use only local compose (sliced topology, no flat overlay)."""
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS["B2flat"]
        run = RunConfig(
            config_name="B2flat",
            seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_phase_b.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        names = [f.name for f in captured["compose_files"]]
        assert "docker-compose.local.yaml" in names
        assert "docker-compose.flat.yaml" not in names
        assert "docker-compose.flat-equalized.yaml" not in names

    def test_b2_run_still_uses_neural_placement(self):
        """B2 run must still use neural placement (regression check)."""
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS["B2"]
        run = RunConfig(
            config_name="B2",
            seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_phase_b.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        env = captured["env"]
        assert env.get("PLACEMENT_STRATEGY") == "neural", (
            f"B2 must still use neural placement, got: {env.get('PLACEMENT_STRATEGY')}"
        )
        assert "BROKER_MODULE" not in env or env["BROKER_MODULE"] != "src.broker.static_broker", (
            "B2 must NOT use static broker"
        )
