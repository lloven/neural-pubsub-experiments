"""Tests for Stress: Combined H3+H6 contention + failure experiment.

Stress phase combines high-load contention with failure injection and
strategy comparison (S1 vs S3) to test whether neural placement outperforms
round-robin under overload + failure.

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
# Config validation: 12 configs with correct rates/strategies/failure targets
# ---------------------------------------------------------------------------

class TestStressConfigs:
    """Stress has 12 configs varying rate (10/20/50), strategy (S1/S3),
    and failure (none / worker kill)."""

    def test_has_exactly_twelve_configs(self):
        from scripts.run_stress import CONFIGS
        assert len(CONFIGS) == 12, (
            f"Expected 12 configs, got {len(CONFIGS)}: "
            f"{sorted(CONFIGS.keys())}"
        )

    def test_all_config_names_present(self):
        from scripts.run_stress import CONFIGS
        expected = {
            "10pps-rr-nofail", "10pps-neural-nofail",
            "10pps-rr-fail", "10pps-neural-fail",
            "20pps-rr-nofail", "20pps-neural-nofail",
            "20pps-rr-fail", "20pps-neural-fail",
            "50pps-rr-nofail", "50pps-neural-nofail",
            "50pps-rr-fail", "50pps-neural-fail",
        }
        assert set(CONFIGS.keys()) == expected, (
            f"Expected configs {expected}, got {set(CONFIGS.keys())}"
        )

    def test_10pps_rr_nofail(self):
        """10pps-rr-nofail: 10 pps, S1 (round-robin), no failure."""
        from scripts.run_stress import CONFIGS
        cfg = CONFIGS["10pps-rr-nofail"]
        assert cfg["arrival_rate"] == 10.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] is None

    def test_10pps_neural_nofail(self):
        from scripts.run_stress import CONFIGS
        cfg = CONFIGS["10pps-neural-nofail"]
        assert cfg["arrival_rate"] == 10.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] is None

    def test_20pps_rr_fail(self):
        """20pps-rr-fail: 20 pps, S1, eMBB worker kill (H3+H6 key cell)."""
        from scripts.run_stress import CONFIGS
        cfg = CONFIGS["20pps-rr-fail"]
        assert cfg["arrival_rate"] == 20.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] == "worker-d1-embb-1"

    def test_20pps_neural_fail(self):
        """20pps-neural-fail: 20 pps, S3, eMBB worker kill (H3+H6 key cell)."""
        from scripts.run_stress import CONFIGS
        cfg = CONFIGS["20pps-neural-fail"]
        assert cfg["arrival_rate"] == 20.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] == "worker-d1-embb-1"

    def test_50pps_rr_nofail(self):
        """50pps-rr-nofail: 50 pps, S1, no failure (extreme overload baseline)."""
        from scripts.run_stress import CONFIGS
        cfg = CONFIGS["50pps-rr-nofail"]
        assert cfg["arrival_rate"] == 50.0
        assert cfg["strategy"] == "S1"
        assert cfg["failure_target"] is None

    def test_50pps_neural_fail(self):
        """50pps-neural-fail: 50 pps, S3, eMBB worker kill."""
        from scripts.run_stress import CONFIGS
        cfg = CONFIGS["50pps-neural-fail"]
        assert cfg["arrival_rate"] == 50.0
        assert cfg["strategy"] == "S3"
        assert cfg["failure_target"] == "worker-d1-embb-1"


# ---------------------------------------------------------------------------
# Timing validation
# ---------------------------------------------------------------------------

class TestStressTiming:
    """Timing constraints for stress runs."""

    def test_default_warmup_is_120(self):
        from scripts.run_stress import StressRunConfig
        rc = StressRunConfig(
            config_name="10pps-rr-nofail", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target=None,
        )
        assert rc.warmup_s == 120

    def test_default_measurement_is_600(self):
        from scripts.run_stress import StressRunConfig
        rc = StressRunConfig(
            config_name="10pps-rr-nofail", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target=None,
        )
        assert rc.measurement_s == 600

    def test_failure_delay_less_than_total(self):
        from scripts.run_stress import StressRunConfig
        rc = StressRunConfig(
            config_name="10pps-rr-fail", seed=42, arrival_rate=10.0,
            strategy="S1", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        assert rc.failure_delay_s < total

    def test_post_failure_at_least_5_minutes(self):
        from scripts.run_stress import StressRunConfig
        rc = StressRunConfig(
            config_name="20pps-rr-fail", seed=42, arrival_rate=20.0,
            strategy="S1", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        post_failure = total - rc.failure_delay_s
        assert post_failure >= 300


# ---------------------------------------------------------------------------
# Run matrix validation
# ---------------------------------------------------------------------------

class TestStressMatrix:
    """Stress run matrix: 12 configs x 5 seeds = 60 runs."""

    def test_full_matrix_is_60_runs(self):
        """12 configs x 5 seeds = 60 runs."""
        from scripts.run_stress import CONFIGS, build_run_matrix
        matrix = build_run_matrix(list(CONFIGS.keys()), DEFAULT_SEEDS)
        assert len(matrix) == 60, f"Expected 60 runs, got {len(matrix)}"

    def test_all_run_ids_unique(self):
        """All 60 runs must have unique run_ids for resume tracking."""
        from scripts.run_stress import CONFIGS, build_run_matrix
        matrix = build_run_matrix(list(CONFIGS.keys()), DEFAULT_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(set(run_ids)) == 60, f"Duplicate run_ids found"

    def test_run_id_includes_config_name(self):
        from scripts.run_stress import StressRunConfig
        rc = StressRunConfig(
            config_name="20pps-rr-fail", seed=42, arrival_rate=20.0,
            strategy="S1", failure_target="worker-d1-embb-1",
        )
        assert "20pps-rr-fail" in rc.run_id

    def test_single_config_single_seed(self):
        from scripts.run_stress import build_run_matrix
        matrix = build_run_matrix(["20pps-rr-fail"], [42])
        assert len(matrix) == 1


# ---------------------------------------------------------------------------
# Strategy env mapping (reuses _strategy_env from resilience)
# ---------------------------------------------------------------------------

class TestStressStrategyEnv:
    """Stress reuses _strategy_env and STRATEGIES from resilience."""

    def test_s1_sets_static_broker(self):
        from scripts.run_resilience import _strategy_env
        env = _strategy_env("S1")
        assert env["BROKER_MODULE"] == "src.broker.static_broker"
        assert env["PLACEMENT"] == "round_robin"

    def test_s3_sets_neural(self):
        from scripts.run_resilience import _strategy_env
        env = _strategy_env("S3")
        assert "BROKER_MODULE" not in env
        assert env["PLACEMENT_STRATEGY"] == "neural"

    def test_unknown_strategy_raises(self):
        from scripts.run_resilience import _strategy_env
        with pytest.raises(ValueError, match="Unknown strategy"):
            _strategy_env("S99")


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------

class TestStressCLI:
    """Stress CLI: help, dry-run, timing overrides."""

    def test_help_text(self):
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_stress", "--help"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        assert result.returncode == 0
        assert "Stress" in result.stdout or "stress" in result.stdout.lower()

    def test_dry_run_reports_60_runs(self):
        """--dry-run with all configs should report 60 runs planned."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_stress", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert "60 runs planned" in output, (
            f"Expected 60 runs planned, got:\n{output[-1000:]}"
        )

    def test_warmup_and_measurement_override(self):
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_stress",
             "--configs", "20pps-rr-fail", "--seeds", "42",
             "--warmup", "30", "--measurement", "120", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert "duration=150s" in output

    def test_failure_delay_override(self):
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_stress",
             "--configs", "20pps-rr-fail", "--seeds", "42",
             "--warmup", "30", "--measurement", "120",
             "--failure-delay", "60", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert result.returncode == 0
        assert "inject_at=60s" in output


# ---------------------------------------------------------------------------
# Failure delay validation
# ---------------------------------------------------------------------------

class TestStressFailureDelay:
    """Failure delay must be overridable and passed to build_run_matrix."""

    def test_build_matrix_with_failure_delay_override(self):
        from scripts.run_stress import build_run_matrix
        matrix = build_run_matrix(["20pps-rr-fail"], [42], failure_delay_s=60)
        assert len(matrix) == 1
        assert matrix[0].failure_delay_s == 60

    def test_build_matrix_default_failure_delay(self):
        from scripts.run_stress import build_run_matrix
        matrix = build_run_matrix(["20pps-rr-fail"], [42])
        assert matrix[0].failure_delay_s == 300
