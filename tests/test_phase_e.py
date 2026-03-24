"""Tests for Phase E: Combined H3+H6 contention + failure experiment.

Phase E combines high-load contention (Phase A.6 rates) with failure
injection (Phase D) and strategy comparison (S1 vs S3) to test
whether neural placement outperforms round-robin under overload + failure.

Validates config definitions, timing constraints, run matrix, strategy
env mapping, and CLI interface.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from scripts._common import DEFAULT_SEEDS, PROJECT_ROOT


# ---------------------------------------------------------------------------
# Config validation: 8 configs with correct rates/strategies/failure targets
# ---------------------------------------------------------------------------

class TestPhaseEConfigs:
    """Phase E has 8 configs: E1-E8, varying rate (10/20), strategy (S1/S3),
    and failure (none / worker kill)."""

    def test_has_exactly_eight_configs(self):
        from scripts.run_phase_e import CONFIGS
        assert len(CONFIGS) == 8, (
            f"Expected 8 configs (E1-E8), got {len(CONFIGS)}: "
            f"{sorted(CONFIGS.keys())}"
        )

    def test_all_config_names_present(self):
        from scripts.run_phase_e import CONFIGS
        expected = {f"E{i}" for i in range(1, 9)}
        assert set(CONFIGS.keys()) == expected, (
            f"Expected configs {expected}, got {set(CONFIGS.keys())}"
        )

    def test_e1_rate_10_s1_no_failure(self):
        """E1: 10 pps, S1 (round-robin), no failure."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E1"]
        assert cfg["arrival_rate"] == 10.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] is None

    def test_e2_rate_10_s3_no_failure(self):
        """E2: 10 pps, S3 (neural), no failure."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E2"]
        assert cfg["arrival_rate"] == 10.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] is None

    def test_e3_rate_10_s1_failure(self):
        """E3: 10 pps, S1, eMBB worker kill."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E3"]
        assert cfg["arrival_rate"] == 10.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] == "worker-d1-embb-1"

    def test_e4_rate_10_s3_failure(self):
        """E4: 10 pps, S3, eMBB worker kill."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E4"]
        assert cfg["arrival_rate"] == 10.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] == "worker-d1-embb-1"

    def test_e5_rate_20_s1_no_failure(self):
        """E5: 20 pps, S1, no failure."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E5"]
        assert cfg["arrival_rate"] == 20.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] is None

    def test_e6_rate_20_s3_no_failure(self):
        """E6: 20 pps, S3, no failure."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E6"]
        assert cfg["arrival_rate"] == 20.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] is None

    def test_e7_rate_20_s1_failure(self):
        """E7: 20 pps, S1, eMBB worker kill (H3+H6 key cell)."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E7"]
        assert cfg["arrival_rate"] == 20.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] == "worker-d1-embb-1"

    def test_e8_rate_20_s3_failure(self):
        """E8: 20 pps, S3, eMBB worker kill (H3+H6 key cell)."""
        from scripts.run_phase_e import CONFIGS
        cfg = CONFIGS["E8"]
        assert cfg["arrival_rate"] == 20.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] == "worker-d1-embb-1"


# ---------------------------------------------------------------------------
# Timing validation
# ---------------------------------------------------------------------------

class TestPhaseETiming:
    """Timing constraints for Phase E runs."""

    def test_default_warmup_is_120(self):
        """Default warmup_s must be 120s."""
        from scripts.run_phase_e import PhaseERunConfig
        rc = PhaseERunConfig(
            config_name="E1", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target=None,
        )
        assert rc.warmup_s == 120

    def test_default_measurement_is_600(self):
        """Default measurement_s must be 600s."""
        from scripts.run_phase_e import PhaseERunConfig
        rc = PhaseERunConfig(
            config_name="E1", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target=None,
        )
        assert rc.measurement_s == 600

    def test_failure_delay_less_than_total(self):
        """failure_delay_s must be < warmup_s + measurement_s."""
        from scripts.run_phase_e import PhaseERunConfig
        rc = PhaseERunConfig(
            config_name="E3", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        assert rc.failure_delay_s < total, (
            f"Failure at {rc.failure_delay_s}s but run ends at {total}s"
        )

    def test_post_failure_at_least_5_minutes(self):
        """After failure injection, at least 300s must remain."""
        from scripts.run_phase_e import PhaseERunConfig
        rc = PhaseERunConfig(
            config_name="E7", seed=42, arrival_rate=20.0,
            strategy="S1", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        post_failure = total - rc.failure_delay_s
        assert post_failure >= 300, (
            f"Only {post_failure}s after failure at {rc.failure_delay_s}s"
        )


# ---------------------------------------------------------------------------
# Run matrix validation
# ---------------------------------------------------------------------------

class TestPhaseEMatrix:
    """Phase E run matrix: 8 configs x 5 seeds = 40 runs."""

    def test_full_matrix_is_40_runs(self):
        """8 configs x 5 seeds = 40 runs."""
        from scripts.run_phase_e import CONFIGS, build_run_matrix
        matrix = build_run_matrix(list(CONFIGS.keys()), DEFAULT_SEEDS)
        assert len(matrix) == 40, f"Expected 40 runs, got {len(matrix)}"

    def test_all_run_ids_unique(self):
        """All 40 runs must have unique run_ids for resume tracking."""
        from scripts.run_phase_e import CONFIGS, build_run_matrix
        matrix = build_run_matrix(list(CONFIGS.keys()), DEFAULT_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(set(run_ids)) == 40, f"Duplicate run_ids found"

    def test_run_id_includes_rate(self):
        """run_id must include the arrival rate for disambiguation."""
        from scripts.run_phase_e import PhaseERunConfig
        rc = PhaseERunConfig(
            config_name="E5", seed=42, arrival_rate=20.0,
            strategy="S1", failure_target=None,
        )
        assert "20" in rc.run_id, f"Rate not in run_id: {rc.run_id}"

    def test_run_id_includes_strategy(self):
        """run_id must include the strategy label."""
        from scripts.run_phase_e import PhaseERunConfig
        rc = PhaseERunConfig(
            config_name="E1", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target=None,
        )
        assert "S1" in rc.run_id, f"Strategy not in run_id: {rc.run_id}"

    def test_run_id_includes_failure_indicator(self):
        """run_id must distinguish failure from no-failure configs."""
        from scripts.run_phase_e import PhaseERunConfig
        rc_fail = PhaseERunConfig(
            config_name="E7", seed=42, arrival_rate=20.0,
            strategy="S1", failure_target="worker-d1-embb-1",
        )
        rc_nofail = PhaseERunConfig(
            config_name="E5", seed=42, arrival_rate=20.0,
            strategy="S1", failure_target=None,
        )
        assert rc_fail.run_id != rc_nofail.run_id

    def test_single_config_single_seed(self):
        """1 config x 1 seed = 1 run."""
        from scripts.run_phase_e import build_run_matrix
        matrix = build_run_matrix(["E7"], [42])
        assert len(matrix) == 1


# ---------------------------------------------------------------------------
# Strategy env mapping (reuses _strategy_env from Phase D)
# ---------------------------------------------------------------------------

class TestPhaseEStrategyEnv:
    """Phase E reuses _strategy_env and STRATEGIES from Phase D."""

    def test_s1_sets_static_broker(self):
        """S1 must set BROKER_MODULE to static_broker."""
        from scripts.run_phase_d import _strategy_env
        env = _strategy_env("S1")
        assert env["BROKER_MODULE"] == "src.broker.static_broker"
        assert env["PLACEMENT"] == "round_robin"

    def test_s3_sets_neural(self):
        """S3 must set PLACEMENT_STRATEGY to neural, no BROKER_MODULE."""
        from scripts.run_phase_d import _strategy_env
        env = _strategy_env("S3")
        assert "BROKER_MODULE" not in env
        assert env["PLACEMENT_STRATEGY"] == "neural"

    def test_unknown_strategy_raises(self):
        """Unknown strategy must raise ValueError."""
        from scripts.run_phase_d import _strategy_env
        with pytest.raises(ValueError, match="Unknown strategy"):
            _strategy_env("S99")


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------

class TestPhaseECLI:
    """Phase E CLI: help, dry-run, timing overrides."""

    def test_help_text(self):
        """run_phase_e.py --help should succeed."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_e", "--help"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        assert result.returncode == 0
        assert "Phase E" in result.stdout or "phase_e" in result.stdout.lower()

    def test_dry_run_reports_40_runs(self):
        """--dry-run with all configs should report 40 runs planned."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_e", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert "40 runs planned" in output, (
            f"Expected 40 runs planned, got:\n{output[-1000:]}"
        )

    def test_warmup_and_measurement_override(self):
        """--warmup and --measurement should override defaults."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_e",
             "--configs", "E7", "--seeds", "42",
             "--warmup", "30", "--measurement", "120", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        # warmup=30 + measurement=120 = 150s
        assert "duration=150s" in output, (
            f"Expected duration=150s, got:\n{output[-1000:]}"
        )

    def test_failure_delay_override(self):
        """--failure-delay should override default failure_delay_s."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_e",
             "--configs", "E7", "--seeds", "42",
             "--warmup", "30", "--measurement", "120",
             "--failure-delay", "60", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert result.returncode == 0, (
            f"--failure-delay flag not accepted:\n{output[-1000:]}"
        )
        assert "inject_at=60s" in output, (
            f"Expected inject_at=60s, got:\n{output[-1000:]}"
        )


# ---------------------------------------------------------------------------
# Failure delay validation
# ---------------------------------------------------------------------------

class TestPhaseEFailureDelay:
    """Failure delay must be overridable and passed to build_run_matrix."""

    def test_build_matrix_with_failure_delay_override(self):
        """build_run_matrix should accept failure_delay_s kwarg."""
        from scripts.run_phase_e import build_run_matrix
        matrix = build_run_matrix(["E7"], [42], failure_delay_s=60)
        assert len(matrix) == 1
        assert matrix[0].failure_delay_s == 60

    def test_build_matrix_default_failure_delay(self):
        """Without override, failure_delay_s should be 300."""
        from scripts.run_phase_e import build_run_matrix
        matrix = build_run_matrix(["E7"], [42])
        assert matrix[0].failure_delay_s == 300
