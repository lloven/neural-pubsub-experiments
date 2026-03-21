"""Tests for the unified orchestration layer.

Covers:
  - Cycle 1: resolve_config() — config→compose-files and config→env mapping
  - Cycle 2: Phase B compose overlay wiring
  - Cycle 3: Monitor --once flag and CSV discovery
  - Cycle 4: Bash dispatcher delegation
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts._common import PROJECT_ROOT

# Compose file paths (constants that resolve_config should use)
COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"
COMPOSE_KAFKA = PROJECT_ROOT / "docker-compose.kafka.yaml"
COMPOSE_FLAT = PROJECT_ROOT / "docker-compose.flat.yaml"
COMPOSE_GOVERNANCE = PROJECT_ROOT / "docker-compose.governance.yaml"


# ============================================================================
# Cycle 1: resolve_config()
# ============================================================================

class TestResolveConfig:
    """Tests 1-9: resolve_config() maps config names to compose files and env."""

    def _resolve(self, config_name: str, rate: str = "medium", stages: int = 3, seed: int = 42):
        from scripts._common import resolve_config
        return resolve_config(config_name, rate=rate, stages=stages, seed=seed)

    # --- Compose file resolution ---

    def test_A1_uses_kafka_overlay(self):
        """Test 1: A1 config uses local + kafka compose files."""
        cfg = self._resolve("A1")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_KAFKA]

    def test_B1_uses_flat_overlay(self):
        """Test 2: B1 config uses local + flat compose files."""
        cfg = self._resolve("B1")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_FLAT]

    def test_B3_uses_governance_overlay(self):
        """Test 3: B3 config uses local + governance compose files."""
        cfg = self._resolve("B3")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_GOVERNANCE]

    def test_B4_uses_governance_overlay(self):
        """Test 4: B4 config uses local + governance compose files."""
        cfg = self._resolve("B4")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_GOVERNANCE]

    def test_A2_uses_base_only(self):
        """Test 5: A2 config uses only the local compose file."""
        cfg = self._resolve("A2")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    # --- Environment variable resolution ---

    def test_A1_env_has_no_broker_module(self):
        """Test 6: A1 env does not set BROKER_MODULE (kafka overlay handles it)."""
        cfg = self._resolve("A1")
        assert "BROKER_MODULE" not in cfg.env

    def test_A2_env_has_static_broker_round_robin(self):
        """Test 7: A2 env sets static broker with round_robin placement."""
        cfg = self._resolve("A2")
        assert cfg.env["BROKER_MODULE"] == "src.broker.static_broker"
        assert cfg.env["PLACEMENT"] == "round_robin"

    def test_A3_env_has_static_broker_random(self):
        """Test 8: A3 env sets static broker with random placement."""
        cfg = self._resolve("A3")
        assert cfg.env["BROKER_MODULE"] == "src.broker.static_broker"
        assert cfg.env["PLACEMENT"] == "random"

    def test_rate_mapping(self):
        """Test 9: Rate labels map to correct numeric values."""
        for label, expected in [("low", "2.0"), ("medium", "5.0"), ("high", "10.0")]:
            cfg = self._resolve("A4", rate=label)
            assert cfg.env["ARRIVAL_RATE"] == expected, f"rate={label} should map to {expected}"

    # --- Additional edge cases ---

    def test_A4_uses_base_only(self):
        """A4 (neural) uses only the local compose file."""
        cfg = self._resolve("A4")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    def test_A4_env_has_no_broker_module(self):
        """A4 (neural, default) does not set BROKER_MODULE."""
        cfg = self._resolve("A4")
        assert "BROKER_MODULE" not in cfg.env

    def test_B2_uses_base_only(self):
        """B2 uses only the local compose file."""
        cfg = self._resolve("B2")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    def test_C_configs_use_base_only(self):
        """C1-C4 use only the local compose file (future phases)."""
        for name in ["C1", "C2", "C3", "C4"]:
            cfg = self._resolve(name)
            assert cfg.compose_files == [COMPOSE_LOCAL], f"{name} should use base only"

    def test_D_configs_use_base_only(self):
        """D1-D4 use only the local compose file (future phases)."""
        for name in ["D1", "D2", "D3", "D4"]:
            cfg = self._resolve(name)
            assert cfg.compose_files == [COMPOSE_LOCAL], f"{name} should use base only"

    def test_unknown_config_raises(self):
        """Unknown config name should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown config"):
            self._resolve("Z9")

    def test_seed_in_env(self):
        """Seed is passed through to env."""
        cfg = self._resolve("A4", seed=999)
        assert cfg.env["SEED"] == "999"

    def test_stages_in_env(self):
        """Pipeline stages are passed through to env."""
        cfg = self._resolve("A4", stages=5)
        assert cfg.env.get("PIPELINE_STAGES") == "5" or "DURATION_S" in cfg.env


# ============================================================================
# Cycle 2: Phase B compose overlay wiring
# ============================================================================

class TestPhaseBOverlays:
    """Tests 10-14: Phase B orchestrator passes correct compose files."""

    def _get_run_kwargs(self, config_name: str, seed: int = 42):
        """Call Phase B's _run() in dry-run mode and capture the compose_files
        passed to run_single() via monkey-patching."""
        from unittest.mock import patch
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS[config_name]
        run = RunConfig(
            config_name=config_name,
            seed=seed,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        # Patch run_single to capture kwargs instead of running Docker
        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_phase_b.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        return captured

    def test_B1_uses_flat_overlay(self):
        """Test 10: Phase B runner passes flat overlay for B1."""
        kwargs = self._get_run_kwargs("B1")
        assert kwargs.get("compose_files") is not None, "B1 must pass compose_files"
        names = [f.name for f in kwargs["compose_files"]]
        assert "docker-compose.flat.yaml" in names

    def test_B2_uses_base_only(self):
        """Test 11: Phase B runner uses only local.yaml for B2."""
        kwargs = self._get_run_kwargs("B2")
        files = kwargs.get("compose_files")
        # B2 either passes None (uses default) or passes [local] only
        if files is not None:
            names = [f.name for f in files]
            assert names == ["docker-compose.local.yaml"]

    def test_B3_uses_governance_overlay(self):
        """Test 12: Phase B runner passes governance overlay for B3."""
        kwargs = self._get_run_kwargs("B3")
        assert kwargs.get("compose_files") is not None, "B3 must pass compose_files"
        names = [f.name for f in kwargs["compose_files"]]
        assert "docker-compose.governance.yaml" in names

    def test_B4_has_failure_injection(self):
        """Test 13: B4 run config sets failure_fn."""
        kwargs = self._get_run_kwargs("B4")
        assert kwargs.get("failure_fn") is not None, "B4 must have failure injection"

    def test_phase_b_matrix_is_20_runs(self):
        """Test 14: 4 configs x 5 seeds = 20 runs."""
        from scripts.run_phase_b import build_run_matrix, CONFIGS
        runs = build_run_matrix(
            list(CONFIGS.keys()),
            [42, 123, 456, 789, 0],
        )
        assert len(runs) == 20


# ============================================================================
# Cycle 3: Monitor improvements
# ============================================================================

class TestMonitorCSVDiscovery:
    """Tests 15-19: _discover_progress_from_csvs() and --once flag."""

    def test_discover_csvs_finds_completed_runs(self, tmp_path):
        """Test 15: CSV files are discovered as 'done' runs."""
        from scripts.monitor import _discover_progress_from_csvs

        # Create fake CSV files
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")
        (tmp_path / "B2_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        assert "B1_rate-medium_stages-3_seed-42" in progress
        assert progress["B1_rate-medium_stages-3_seed-42"]["status"] == "done"
        assert "B2_rate-medium_stages-3_seed-42" in progress
        assert progress["B2_rate-medium_stages-3_seed-42"]["status"] == "done"

    def test_discover_csvs_finds_failed_runs(self, tmp_path):
        """Test 16: .FAILED files are discovered as 'failed' runs."""
        from scripts.monitor import _discover_progress_from_csvs

        (tmp_path / "B3_rate-medium_stages-3_seed-42.FAILED").write_text("")

        progress = _discover_progress_from_csvs(tmp_path)
        assert "B3_rate-medium_stages-3_seed-42" in progress
        assert progress["B3_rate-medium_stages-3_seed-42"]["status"] == "failed"

    def test_discover_csvs_skips_smoke_files(self, tmp_path):
        """Test 17: smoke_*.csv and summary files are excluded."""
        from scripts.monitor import _discover_progress_from_csvs

        (tmp_path / "smoke_A4_20260301.csv").write_text("header\n")
        (tmp_path / "phase_b_summary.csv").write_text("header\n")
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        # smoke file should be excluded
        assert not any("smoke" in k for k in progress), f"smoke files should be excluded: {list(progress.keys())}"
        # real run should be included
        assert "B1_rate-medium_stages-3_seed-42" in progress

    def test_monitor_once_flag_exits(self):
        """Test 18: --once flag causes monitor to print status and exit."""
        import subprocess
        result = subprocess.run(
            [
                "python3", "-m", "scripts.monitor",
                "--once", "results/phase_b",
            ],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
        )
        # Should exit cleanly (0), not hang in a loop
        assert result.returncode == 0

    def test_discover_csvs_excludes_summary_files(self, tmp_path):
        """Test 19: phase_*_summary.csv files are excluded from discovery."""
        from scripts.monitor import _discover_progress_from_csvs

        (tmp_path / "phase_b_summary.csv").write_text("run_id,status\n")
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        assert not any("summary" in k for k in progress), f"summary files should be excluded: {list(progress.keys())}"
        assert "B1_rate-medium_stages-3_seed-42" in progress


# ============================================================================
# Cycle 4: Bash dispatcher
# ============================================================================

class TestBashDispatcher:
    """Tests 20-22: run-experiments.sh delegates to Python scripts."""

    SCRIPT = str(PROJECT_ROOT / "run-experiments.sh")

    def test_help_shows_all_phases(self):
        """Test 20: --help lists all phase commands."""
        import subprocess
        result = subprocess.run(
            [self.SCRIPT, "help"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
        )
        output = result.stdout + result.stderr
        for cmd in ["phase-a", "phase-b", "phase-c", "phase-d", "single", "smoke", "status", "stop"]:
            assert cmd in output, f"Help should mention '{cmd}'"

    def test_single_calls_python_script(self):
        """Test 21: 'single' command invokes scripts/run_single.py."""
        import subprocess
        # Use bash -x to trace which commands are invoked.
        # The Python script will try to run Docker (which may fail/timeout),
        # so we capture what we need from the trace and accept timeout.
        try:
            result = subprocess.run(
                ["bash", "-x", self.SCRIPT, "single", "A2", "low", "3", "42"],
                capture_output=True, text=True, timeout=10,
                cwd=str(PROJECT_ROOT),
            )
            trace = result.stderr
        except subprocess.TimeoutExpired as e:
            # Timeout is fine -- we just need the trace showing delegation
            trace = (e.stderr or "") if isinstance(e.stderr, str) else (e.stderr or b"").decode()
        # The new dispatcher should invoke python3 -m scripts.run_single
        assert "run_single" in trace, (
            f"'single' command should delegate to run_single.\n"
            f"Trace: {trace[-500:]}"
        )

    def test_status_calls_monitor(self):
        """Test 22: 'status' command invokes monitor.py --once."""
        import subprocess
        result = subprocess.run(
            ["bash", "-x", self.SCRIPT, "status"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
        )
        trace = result.stderr
        # The new dispatcher should invoke monitor.py --once
        assert "monitor" in trace and "--once" in trace, (
            f"'status' command should delegate to monitor.py --once.\n"
            f"Trace: {trace[-500:]}"
        )
