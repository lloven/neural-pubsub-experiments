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
            failure_type="worker", failure_target="worker",
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
            failure_type="worker", failure_target="worker",
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
