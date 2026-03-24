"""Tests for slicing configurations: flat and rr.

Merged from test_b1eq_smoke.py and test_b2flat_smoke.py. Tests both the
flat (equalized) baseline and rr (round-robin on sliced topology) configs.

Covers:
  - resolve_config returns correct compose overlays and env for flat/rr
  - Docker Compose config validation (5 workers, correct networks)
  - Slicing run matrix includes flat and rr
  - rr uses static broker with round-robin placement
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from scripts._common import (
    COMPOSE_FILE,
    COMPOSE_FLAT,
    COMPOSE_FLAT_EQ,
    PROJECT_ROOT,
    _CONFIG_TABLE,
)


COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"


# ============================================================================
# flat (was B1eq): resolve_config and compose validation
# ============================================================================


class TestFlatResolveConfig:
    """flat config must resolve to the flat-equalized overlay with no extra env."""

    def _resolve(self, config_name: str, **kwargs):
        from scripts._common import resolve_config
        return resolve_config(config_name, **kwargs)

    def test_b1eq_is_recognised(self):
        """resolve_config('B1eq') must not raise ValueError."""
        cfg = self._resolve("B1eq")
        assert cfg is not None

    def test_b1eq_uses_flat_equalized_overlay(self):
        """B1eq must use docker-compose.flat-equalized.yaml, NOT docker-compose.flat.yaml."""
        cfg = self._resolve("B1eq")
        overlay_names = [f.name for f in cfg.compose_files]
        assert "docker-compose.flat-equalized.yaml" in overlay_names
        assert "docker-compose.flat.yaml" not in overlay_names

    def test_b1eq_compose_files_are_local_plus_flat_eq(self):
        """B1eq compose_files = [local, flat-equalized]."""
        cfg = self._resolve("B1eq")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_FLAT_EQ]

    def test_b1eq_has_no_broker_module_override(self):
        cfg = self._resolve("B1eq")
        assert "BROKER_MODULE" not in cfg.env

    def test_b1eq_broker_is_none(self):
        cfg = self._resolve("B1eq")
        assert cfg.broker_module is None

    def test_b1eq_kafka_transport_adds_kafka_overlay(self):
        from scripts._common import COMPOSE_KAFKA
        cfg = self._resolve("B1eq", transport="kafka")
        assert COMPOSE_KAFKA in cfg.compose_files

    def test_b1eq_env_has_standard_fields(self):
        cfg = self._resolve("B1eq", rate="medium", seed=42, stages=3)
        assert cfg.env["ARRIVAL_RATE"] == "5.0"
        assert cfg.env["SEED"] == "42"
        assert cfg.env["PIPELINE_STAGES"] == "3"


class TestFlatComposeConfig:
    """Validate that the merged compose config has 5 workers and correct topology."""

    @pytest.fixture(scope="class")
    def compose_config(self):
        result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_LOCAL), "-f", str(COMPOSE_FLAT_EQ), "config"],
            capture_output=True, text=True, timeout=30, cwd=str(PROJECT_ROOT),
        )
        if result.returncode != 0:
            pytest.skip(f"docker compose config failed: {result.stderr[:200]}")
        return yaml.safe_load(result.stdout)

    def _active_services(self, config: dict) -> dict:
        active = {}
        for name, svc in config.get("services", {}).items():
            deploy = svc.get("deploy", {})
            replicas = deploy.get("replicas")
            if replicas == 0:
                continue
            active[name] = svc
        return active

    def _worker_services(self, config: dict) -> dict:
        return {name: svc for name, svc in self._active_services(config).items() if name.startswith("worker-")}

    def test_five_workers_defined(self, compose_config):
        workers = self._worker_services(compose_config)
        assert len(workers) == 5

    def test_expected_worker_names(self, compose_config):
        workers = self._worker_services(compose_config)
        expected = {"worker-d1-urllc-1", "worker-d1-urllc-2", "worker-d1-embb-1", "worker-d2-embb-1", "worker-d2-embb-2"}
        assert set(workers.keys()) == expected

    def test_broker_d2_has_replicas_zero(self, compose_config):
        broker_d2 = compose_config["services"].get("broker-d2", {})
        replicas = broker_d2.get("deploy", {}).get("replicas")
        assert replicas == 0


class TestFlatInSlicing:
    """flat must be included in the slicing run matrix."""

    def test_flat_in_slicing_configs(self):
        from scripts.run_slicing import CONFIGS
        assert "flat" in CONFIGS

    def test_flat_config_properties(self):
        from scripts.run_slicing import CONFIGS
        cfg = CONFIGS["flat"]
        assert cfg["num_slices"] == 1
        assert cfg["governance"] is False
        assert cfg["failure_injection"] is False

    def test_flat_in_run_matrix(self):
        from scripts.run_slicing import build_run_matrix
        runs = build_run_matrix(["flat"], [42])
        assert len(runs) >= 1
        assert all(r.config_name == "flat" for r in runs)

    def test_slicing_matrix_with_all_configs(self):
        """5 configs x 2 transports x 5 seeds = 50 runs."""
        from scripts.run_slicing import build_run_matrix, CONFIGS
        runs = build_run_matrix(sorted(CONFIGS.keys()), [42, 123, 456, 789, 0])
        assert len(runs) == 50, (
            f"Expected 50 runs (5 configs x 2 transports x 5 seeds), got {len(runs)}"
        )

    def test_flat_run_uses_flat_equalized_overlay(self):
        from scripts.run_slicing import RunConfig, CONFIGS, _run

        cfg = CONFIGS["flat"]
        run = RunConfig(
            config_name="flat", seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_slicing.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        assert captured.get("compose_files") is not None
        names = [f.name for f in captured["compose_files"]]
        assert "docker-compose.flat-equalized.yaml" in names
        assert "docker-compose.flat.yaml" not in names


# ============================================================================
# rr (was B2flat): resolve_config, placement, topology
# ============================================================================


class TestRrResolveConfig:
    """rr config must resolve to local compose with no flat overlay."""

    def _resolve(self, config_name: str, **kwargs):
        from scripts._common import resolve_config
        return resolve_config(config_name, **kwargs)

    def test_b2flat_is_recognised(self):
        cfg = self._resolve("B2flat")
        assert cfg is not None

    def test_b2flat_uses_no_flat_overlay(self):
        cfg = self._resolve("B2flat")
        overlay_names = [f.name for f in cfg.compose_files]
        assert "docker-compose.flat.yaml" not in overlay_names
        assert "docker-compose.flat-equalized.yaml" not in overlay_names

    def test_b2flat_compose_files_are_local_only(self):
        cfg = self._resolve("B2flat", transport="http")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    def test_b2flat_env_has_standard_fields(self):
        cfg = self._resolve("B2flat", rate="medium", seed=42, stages=3)
        assert cfg.env["ARRIVAL_RATE"] == "5.0"
        assert cfg.env["SEED"] == "42"
        assert cfg.env["PIPELINE_STAGES"] == "3"


class TestRrPlacement:
    """rr must use the static broker with round-robin placement."""

    def test_b2flat_uses_static_broker(self):
        entry = _CONFIG_TABLE["B2flat"]
        assert entry["env"]["BROKER_MODULE"] == "src.broker.static_broker"

    def test_b2flat_uses_round_robin_placement(self):
        entry = _CONFIG_TABLE["B2flat"]
        assert entry["env"]["PLACEMENT"] == "round_robin"

    def test_b2flat_broker_is_static(self):
        entry = _CONFIG_TABLE["B2flat"]
        assert entry["broker"] == "src.broker.static_broker"

    def test_b2_uses_neural_broker(self):
        entry = _CONFIG_TABLE["B2"]
        assert "BROKER_MODULE" not in entry["env"]
        assert entry["broker"] is None


class TestRrTopologyMatchesNeural:
    """rr must use the same Docker topology as neural (sliced, 2 brokers, 5 workers)."""

    def test_b2flat_same_overlays_as_b2(self):
        entry_b2 = _CONFIG_TABLE["B2"]
        entry_b2flat = _CONFIG_TABLE["B2flat"]
        assert entry_b2["overlays"] == entry_b2flat["overlays"]

    def test_b2flat_different_from_b1eq_overlays(self):
        entry_b1eq = _CONFIG_TABLE["B1eq"]
        entry_b2flat = _CONFIG_TABLE["B2flat"]
        assert entry_b1eq["overlays"] != entry_b2flat["overlays"]


class TestRrInSlicing:
    """rr must be included in the slicing run matrix."""

    def test_rr_in_slicing_configs(self):
        from scripts.run_slicing import CONFIGS
        assert "rr" in CONFIGS

    def test_rr_config_properties(self):
        from scripts.run_slicing import CONFIGS
        cfg = CONFIGS["rr"]
        assert cfg["num_slices"] == 3
        assert cfg["governance"] is False
        assert cfg["failure_injection"] is False

    def test_rr_same_slices_as_neural(self):
        from scripts.run_slicing import CONFIGS
        assert CONFIGS["rr"]["num_slices"] == CONFIGS["neural"]["num_slices"]

    def test_rr_in_run_matrix(self):
        from scripts.run_slicing import build_run_matrix
        runs = build_run_matrix(["rr"], [42], transports=["http"])
        assert len(runs) == 1
        assert runs[0].config_name == "rr"


class TestRrRunExecution:
    """rr _run must pass static broker + round-robin placement to Docker."""

    def test_rr_run_sets_placement_strategy(self):
        from scripts.run_slicing import RunConfig, CONFIGS, _run

        cfg = CONFIGS["rr"]
        run = RunConfig(
            config_name="rr", seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_slicing.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        env = captured["env"]
        assert env.get("BROKER_MODULE") == "src.broker.static_broker"
        assert env.get("PLACEMENT") == "round_robin"

    def test_rr_run_uses_local_compose_only(self):
        from scripts.run_slicing import RunConfig, CONFIGS, _run

        cfg = CONFIGS["rr"]
        run = RunConfig(
            config_name="rr", seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_slicing.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        names = [f.name for f in captured["compose_files"]]
        assert "docker-compose.local.yaml" in names
        assert "docker-compose.flat.yaml" not in names
        assert "docker-compose.flat-equalized.yaml" not in names

    def test_neural_run_still_uses_neural_placement(self):
        from scripts.run_slicing import RunConfig, CONFIGS, _run

        cfg = CONFIGS["neural"]
        run = RunConfig(
            config_name="neural", seed=42,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_slicing.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        env = captured["env"]
        assert env.get("PLACEMENT_STRATEGY") == "neural"
        assert "BROKER_MODULE" not in env or env["BROKER_MODULE"] != "src.broker.static_broker"
