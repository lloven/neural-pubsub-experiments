"""Tests for Phase D: Failure and adaptation.

Validates timing, configuration, and run matrix for Phase D experiments.
"""

from __future__ import annotations

import pytest

from scripts._common import EXTENDED_SEEDS
from scripts.run_phase_d import CONFIGS, RunConfig, build_run_matrix


class TestPhaseDTiming:
    """Failure must be injected BEFORE the run ends."""

    def test_failure_delay_less_than_total_duration(self):
        """failure_delay_s must be < warmup_s + measurement_s,
        otherwise the failure never triggers."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        assert rc.failure_delay_s < total, (
            f"Failure at {rc.failure_delay_s}s but run ends at {total}s "
            f"(warmup={rc.warmup_s} + measurement={rc.measurement_s}). "
            f"Failure would never trigger."
        )

    def test_post_failure_observation_at_least_5_minutes(self):
        """After failure injection, at least 300s of observation must remain
        to measure recovery time and steady-state return."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        post_failure = total - rc.failure_delay_s
        assert post_failure >= 300, (
            f"Only {post_failure}s after failure at {rc.failure_delay_s}s "
            f"(total={total}s). Need ≥300s for recovery observation."
        )


class TestPhaseDMatrix:
    """Phase D run matrix validation."""

    def test_uses_extended_seeds(self):
        """Phase D uses 10 seeds for statistical power."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        seeds_per_config = len(matrix) // len(CONFIGS)
        assert seeds_per_config == 10, (
            f"Phase D should use 10 seeds, got {seeds_per_config}"
        )

    def test_full_matrix_size(self):
        """4 configs × 10 seeds = 40 runs."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        assert len(matrix) == 40, f"Expected 40 runs, got {len(matrix)}"

    def test_single_config_matrix(self):
        """Single config × 5 default seeds = 5 runs."""
        from scripts._common import DEFAULT_SEEDS
        matrix = build_run_matrix(["D1"], DEFAULT_SEEDS)
        assert len(matrix) == 5

    def test_all_configs_have_failure_type(self):
        """Every config must define a failure_type and target."""
        for name, cfg in CONFIGS.items():
            assert "failure_type" in cfg, f"{name} missing failure_type"
            assert "failure_target" in cfg, f"{name} missing failure_target"


class TestPhaseDRunId:
    """Each RunConfig must have a unique run_id for resume to work correctly."""

    def test_run_id_property_exists(self):
        """RunConfig must expose a run_id property (not just config_name)."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        assert hasattr(rc, 'run_id'), "RunConfig must have a run_id property"

    def test_run_id_includes_seed(self):
        """run_id must distinguish different seeds of the same config."""
        rc1 = RunConfig(config_name="D1", seed=42, failure_type="worker", failure_target="worker-d1-embb-1")
        rc2 = RunConfig(config_name="D1", seed=123, failure_type="worker", failure_target="worker-d1-embb-1")
        assert rc1.run_id != rc2.run_id, "Different seeds must have different run_ids"

    def test_run_id_includes_failure_type(self):
        """run_id must include the failure type for clarity."""
        rc = RunConfig(config_name="D1", seed=42, failure_type="worker", failure_target="worker-d1-embb-1")
        assert "worker" in rc.run_id

    def test_run_id_matches_run_function_format(self):
        """run_id property must match the format used in _run()."""
        rc = RunConfig(config_name="D1", seed=42, failure_type="worker", failure_target="worker-d1-embb-1")
        expected = f"{rc.config_name}_failure-{rc.failure_type}_seed-{rc.seed}"
        assert rc.run_id == expected, f"Expected '{expected}', got '{rc.run_id}'"

    def test_all_d1_seeds_have_unique_run_ids(self):
        """All 10 D1 seeds must produce unique run_ids for resume tracking."""
        matrix = build_run_matrix(["D1"], EXTENDED_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(run_ids) == len(set(run_ids)), f"Duplicate run_ids: {run_ids}"

    def test_full_matrix_unique_run_ids(self):
        """All 40 runs in the full matrix must have unique run_ids."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(run_ids) == 40
        assert len(set(run_ids)) == 40, f"Duplicate run_ids found"


class TestPhaseDDefaultSeeds:
    """Phase D should default to 10 extended seeds for recovery-time analysis."""

    def test_phase_d_defaults_to_extended_seeds(self):
        """run_phase_d.py with no --seeds arg should plan 10 seeds per config."""
        import subprocess, sys, os
        from scripts._common import PROJECT_ROOT
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_d", "--configs", "D1", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert "10 runs planned" in output, (
            f"Phase D should default to 10 seeds (EXTENDED_SEEDS), got:\n{output[-1000:]}"
        )
