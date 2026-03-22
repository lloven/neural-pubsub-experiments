"""Tests for B1-eq (equalized flat baseline) configuration.

B1eq uses 5 workers on a flat network with 1 broker, providing a fair
comparison with B2's 5 workers while isolating the slice-awareness effect.

Covers:
  - Cycle 1: resolve_config("B1eq") returns correct compose overlays and env
  - Cycle 2: Docker Compose config validation (5 workers, correct networks)
  - Cycle 3: Phase B run matrix includes B1eq
  - Cycle 4: Extended smoke test (full stack, slow)
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from scripts._common import PROJECT_ROOT


# Compose file paths
COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"
COMPOSE_FLAT_EQ = PROJECT_ROOT / "docker-compose.flat-equalized.yaml"
COMPOSE_FLAT = PROJECT_ROOT / "docker-compose.flat.yaml"


# ============================================================================
# Cycle 1: resolve_config("B1eq") returns correct overlays and env
# ============================================================================


class TestB1eqResolveConfig:
    """B1eq config must resolve to the flat-equalized overlay with no extra env."""

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
        assert "docker-compose.flat-equalized.yaml" in overlay_names, (
            f"B1eq should use flat-equalized overlay, got: {overlay_names}"
        )
        assert "docker-compose.flat.yaml" not in overlay_names, (
            f"B1eq should NOT use the original flat overlay, got: {overlay_names}"
        )

    def test_b1eq_compose_files_are_local_plus_flat_eq(self):
        """B1eq compose_files = [local, flat-equalized]."""
        cfg = self._resolve("B1eq")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_FLAT_EQ]

    def test_b1eq_has_no_broker_module_override(self):
        """B1eq uses default neural broker (no BROKER_MODULE env override)."""
        cfg = self._resolve("B1eq")
        assert "BROKER_MODULE" not in cfg.env

    def test_b1eq_broker_is_none(self):
        """B1eq broker_module should be None (default neural broker)."""
        cfg = self._resolve("B1eq")
        assert cfg.broker_module is None

    def test_b1eq_kafka_transport_adds_kafka_overlay(self):
        """B1eq with transport='kafka' adds kafka compose overlay."""
        from scripts._common import COMPOSE_KAFKA
        cfg = self._resolve("B1eq", transport="kafka")
        assert COMPOSE_KAFKA in cfg.compose_files

    def test_b1eq_env_has_standard_fields(self):
        """B1eq env must have ARRIVAL_RATE, SEED, PIPELINE_STAGES."""
        cfg = self._resolve("B1eq", rate="medium", seed=42, stages=3)
        assert cfg.env["ARRIVAL_RATE"] == "5.0"
        assert cfg.env["SEED"] == "42"
        assert cfg.env["PIPELINE_STAGES"] == "3"


# ============================================================================
# Cycle 2: Docker Compose config validation
# ============================================================================


class TestB1eqComposeConfig:
    """Validate that the merged compose config has 5 workers and correct topology."""

    @pytest.fixture(scope="class")
    def compose_config(self):
        """Run `docker compose config` and parse the merged YAML."""
        result = subprocess.run(
            [
                "docker", "compose",
                "-f", str(COMPOSE_LOCAL),
                "-f", str(COMPOSE_FLAT_EQ),
                "config",
            ],
            capture_output=True, text=True, timeout=30,
            cwd=str(PROJECT_ROOT),
        )
        if result.returncode != 0:
            pytest.skip(f"docker compose config failed: {result.stderr[:200]}")
        return yaml.safe_load(result.stdout)

    def _active_services(self, config: dict) -> dict:
        """Return services that are not scaled to 0."""
        active = {}
        for name, svc in config.get("services", {}).items():
            deploy = svc.get("deploy", {})
            replicas = deploy.get("replicas")
            if replicas == 0:
                continue
            active[name] = svc
        return active

    def _worker_services(self, config: dict) -> dict:
        """Return only worker services (not brokers, workload, etc.)."""
        return {
            name: svc for name, svc in self._active_services(config).items()
            if name.startswith("worker-")
        }

    def test_five_workers_defined(self, compose_config):
        """B1eq must have exactly 5 active worker services."""
        workers = self._worker_services(compose_config)
        assert len(workers) == 5, (
            f"Expected 5 workers, got {len(workers)}: {sorted(workers.keys())}"
        )

    def test_expected_worker_names(self, compose_config):
        """B1eq workers must include both D1 and D2 workers."""
        workers = self._worker_services(compose_config)
        expected = {
            "worker-d1-urllc-1",
            "worker-d1-urllc-2",
            "worker-d1-embb-1",
            "worker-d2-embb-1",
            "worker-d2-embb-2",
        }
        assert set(workers.keys()) == expected

    def test_broker_d2_has_replicas_zero(self, compose_config):
        """broker-d2 must be scaled to 0 (single broker topology)."""
        broker_d2 = compose_config["services"].get("broker-d2", {})
        replicas = broker_d2.get("deploy", {}).get("replicas")
        assert replicas == 0, (
            f"broker-d2 should have replicas: 0, got: {replicas}"
        )

    def test_all_workers_use_flat_network(self, compose_config):
        """All workers must connect to the slice-flat network."""
        workers = self._worker_services(compose_config)
        for name, svc in workers.items():
            networks = svc.get("networks", {})
            # docker compose config outputs networks as dict
            if isinstance(networks, dict):
                net_names = list(networks.keys())
            else:
                net_names = list(networks)
            assert any("flat" in n.lower() for n in net_names), (
                f"Worker {name} should be on slice-flat network, "
                f"got: {net_names}"
            )

    def test_all_workers_point_to_broker_d1(self, compose_config):
        """All workers must connect to broker-d1 (single broker)."""
        workers = self._worker_services(compose_config)
        for name, svc in workers.items():
            command = svc.get("command", "")
            # docker compose config returns command as a list
            if isinstance(command, list):
                command = " ".join(str(c) for c in command)
            assert "broker-d1" in command, (
                f"Worker {name} should point to broker-d1, "
                f"got command: {command}"
            )

    def test_broker_d1_active(self, compose_config):
        """broker-d1 must be active (not scaled to 0)."""
        active = self._active_services(compose_config)
        assert "broker-d1" in active, "broker-d1 must be active"


# ============================================================================
# Cycle 3: Phase B run matrix includes B1eq
# ============================================================================


class TestB1eqPhaseB:
    """B1eq must be included in the Phase B run matrix."""

    def test_b1eq_in_phase_b_configs(self):
        """B1eq must be a valid Phase B config."""
        from scripts.run_phase_b import CONFIGS
        assert "B1eq" in CONFIGS, (
            f"B1eq missing from Phase B CONFIGS: {sorted(CONFIGS.keys())}"
        )

    def test_b1eq_config_properties(self):
        """B1eq config: 1 slice, no governance, no failure injection."""
        from scripts.run_phase_b import CONFIGS
        cfg = CONFIGS["B1eq"]
        assert cfg["num_slices"] == 1
        assert cfg["governance"] is False
        assert cfg["failure_injection"] is False

    def test_b1eq_in_run_matrix(self):
        """B1eq appears in the run matrix when selected."""
        from scripts.run_phase_b import build_run_matrix, CONFIGS
        runs = build_run_matrix(["B1eq"], [42])
        assert len(runs) >= 1
        assert all(r.config_name == "B1eq" for r in runs)

    def test_phase_b_matrix_with_b1eq_is_50_runs(self):
        """5 configs (B1,B1eq,B2,B3,B4) x 2 transports x 5 seeds = 50 runs."""
        from scripts.run_phase_b import build_run_matrix, CONFIGS
        runs = build_run_matrix(
            sorted(CONFIGS.keys()),
            [42, 123, 456, 789, 0],
        )
        assert len(runs) == 50, (
            f"Expected 50 runs (5 configs x 2 transports x 5 seeds), "
            f"got {len(runs)}"
        )

    def test_b1eq_run_uses_flat_equalized_overlay(self):
        """Phase B runner must pass flat-equalized overlay for B1eq."""
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS["B1eq"]
        run = RunConfig(
            config_name="B1eq",
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

        assert captured.get("compose_files") is not None
        names = [f.name for f in captured["compose_files"]]
        assert "docker-compose.flat-equalized.yaml" in names, (
            f"B1eq run should use flat-equalized overlay, got: {names}"
        )
        assert "docker-compose.flat.yaml" not in names, (
            f"B1eq run should NOT use original flat overlay, got: {names}"
        )

    def test_existing_b1_unchanged(self):
        """Original B1 config must still use flat.yaml (not flat-equalized)."""
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS["B1"]
        run = RunConfig(
            config_name="B1",
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
        assert "docker-compose.flat.yaml" in names
        assert "docker-compose.flat-equalized.yaml" not in names


# ============================================================================
# Cycle 4: Extended smoke test (full stack)
# ============================================================================


@pytest.mark.slow
class TestB1eqFullStack:
    """Full-stack smoke test: start B1eq, verify pipelines complete.

    Requires Docker and builds the image. Skipped in fast test runs.
    """

    @pytest.fixture(scope="class")
    def stack_result(self):
        """Start B1eq stack with short duration and collect results."""
        import tempfile
        import os

        results_dir = Path(tempfile.mkdtemp(prefix="b1eq_smoke_"))
        result_file = results_dir / "b1eq_smoke.csv"
        container_result = f"/app/results/b1eq_smoke.csv"

        env = {
            **os.environ,
            "ARRIVAL_RATE": "2.0",
            "DURATION_S": "30",
            "SEED": "42",
            "WARMUP_S": "0",
            "RESULT_FILE": container_result,
        }

        project_name = "npubsub-b1eq-smoke"
        compose_files = [str(COMPOSE_LOCAL), str(COMPOSE_FLAT_EQ)]
        file_args = []
        for f in compose_files:
            file_args.extend(["-f", f])

        try:
            # Start stack
            subprocess.run(
                [
                    "docker", "compose", *file_args,
                    "-p", project_name,
                    "up", "--build", "--abort-on-container-exit",
                    "--timeout", "30",
                ],
                env=env,
                timeout=120,
                cwd=str(PROJECT_ROOT),
                check=False,
            )
        finally:
            # Tear down
            subprocess.run(
                [
                    "docker", "compose", *file_args,
                    "-p", project_name,
                    "down", "--volumes", "--remove-orphans",
                ],
                env=env,
                timeout=60,
                cwd=str(PROJECT_ROOT),
                check=False,
            )

        # Copy results from the project results dir
        actual_result = PROJECT_ROOT / "results" / "b1eq_smoke.csv"
        return actual_result

    def test_result_csv_exists(self, stack_result):
        """The result CSV must be written."""
        assert stack_result.exists(), f"Result CSV not found: {stack_result}"

    def test_result_csv_has_rows(self, stack_result):
        """The result CSV must contain at least 10 pipeline rows."""
        import csv
        if not stack_result.exists():
            pytest.skip("No result CSV")
        with open(stack_result) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) >= 10, (
            f"Expected >= 10 pipeline rows, got {len(rows)}"
        )

    def test_result_csv_has_expected_columns(self, stack_result):
        """The result CSV must have pipeline_id and e2e_latency_ms columns."""
        import csv
        if not stack_result.exists():
            pytest.skip("No result CSV")
        with open(stack_result) as f:
            reader = csv.DictReader(f)
            fields = set(reader.fieldnames or [])
        for col in ["pipeline_id", "e2e_latency_ms", "success"]:
            assert col in fields, f"Missing column: {col}"
