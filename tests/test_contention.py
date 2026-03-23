"""Tests for Phase A.6: Resource contention.

Validates configuration, timing, run matrix, CLI overrides, and failure
injection for Phase A.6 experiments.
"""

from __future__ import annotations

import pytest

from scripts._common import DEFAULT_SEEDS
from scripts.run_phase_a5_a6 import (
    CONTENTION_CONFIGS,
    FAILURE_KILL_WORKERS,
    ContentionRunConfig,
    build_contention_matrix,
)


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------


class TestContentionConfigs:
    """Phase A.6 must have exactly 3 configs with correct parameters."""

    def test_has_exactly_three_configs(self):
        assert set(CONTENTION_CONFIGS.keys()) == {"A6.1", "A6.2", "A6.3"}

    def test_a61_arrival_rate(self):
        assert CONTENTION_CONFIGS["A6.1"]["arrival_rate"] == 20.0

    def test_a62_arrival_rate(self):
        assert CONTENTION_CONFIGS["A6.2"]["arrival_rate"] == 50.0

    def test_a63_arrival_rate(self):
        assert CONTENTION_CONFIGS["A6.3"]["arrival_rate"] == 10.0

    def test_a61_no_failure(self):
        assert CONTENTION_CONFIGS["A6.1"]["failure"] is None

    def test_a62_no_failure(self):
        assert CONTENTION_CONFIGS["A6.2"]["failure"] is None

    def test_a63_has_failure_injection(self):
        assert CONTENTION_CONFIGS["A6.3"]["failure"] == FAILURE_KILL_WORKERS


# ---------------------------------------------------------------------------
# Timing tests
# ---------------------------------------------------------------------------


class TestContentionTiming:
    """Default timing must be 120s warmup + 600s measurement = 720s total."""

    def test_default_warmup(self):
        rc = ContentionRunConfig(
            config_name="A6.1", arrival_rate=20.0,
            n_workers=5, seed=42,
        )
        assert rc.warmup_s == 120

    def test_default_measurement(self):
        rc = ContentionRunConfig(
            config_name="A6.1", arrival_rate=20.0,
            n_workers=5, seed=42,
        )
        assert rc.measurement_s == 600

    def test_default_total_duration(self):
        rc = ContentionRunConfig(
            config_name="A6.1", arrival_rate=20.0,
            n_workers=5, seed=42,
        )
        assert rc.warmup_s + rc.measurement_s == 720

    def test_a63_failure_delay_less_than_total(self):
        """A6.3 failure_delay_s must be strictly less than total duration."""
        rc = ContentionRunConfig(
            config_name="A6.3", arrival_rate=10.0,
            n_workers=5, seed=42, failure=FAILURE_KILL_WORKERS,
        )
        total = rc.warmup_s + rc.measurement_s
        assert rc.failure_delay_s < total, (
            f"Failure at {rc.failure_delay_s}s but run ends at {total}s"
        )

    def test_a63_failure_delay_default(self):
        """A6.3 failure delay should default to 300s (5 min)."""
        rc = ContentionRunConfig(
            config_name="A6.3", arrival_rate=10.0,
            n_workers=5, seed=42, failure=FAILURE_KILL_WORKERS,
        )
        assert rc.failure_delay_s == 300

    def test_a63_post_failure_observation_sufficient(self):
        """After failure, at least 300s of observation must remain."""
        rc = ContentionRunConfig(
            config_name="A6.3", arrival_rate=10.0,
            n_workers=5, seed=42, failure=FAILURE_KILL_WORKERS,
        )
        total = rc.warmup_s + rc.measurement_s
        post_failure = total - rc.failure_delay_s
        assert post_failure >= 300, (
            f"Only {post_failure}s after failure at {rc.failure_delay_s}s"
        )


# ---------------------------------------------------------------------------
# Run matrix tests
# ---------------------------------------------------------------------------


class TestContentionMatrix:
    """Phase A.6 run matrix: 3 configs x 5 seeds = 15 runs."""

    def test_full_matrix_size(self):
        matrix = build_contention_matrix(
            list(CONTENTION_CONFIGS.keys()), DEFAULT_SEEDS,
        )
        assert len(matrix) == 15

    def test_single_config_matrix(self):
        matrix = build_contention_matrix(["A6.1"], [42])
        assert len(matrix) == 1

    def test_two_configs_three_seeds(self):
        matrix = build_contention_matrix(["A6.1", "A6.2"], [42, 123, 456])
        assert len(matrix) == 6

    def test_matrix_preserves_config_params(self):
        matrix = build_contention_matrix(["A6.2"], [42])
        assert matrix[0].arrival_rate == 50.0
        assert matrix[0].n_workers == 5
        assert matrix[0].failure is None

    def test_matrix_preserves_a63_failure(self):
        matrix = build_contention_matrix(["A6.3"], [42])
        assert matrix[0].failure == FAILURE_KILL_WORKERS


# ---------------------------------------------------------------------------
# CLI override tests
# ---------------------------------------------------------------------------


class TestContentionCLIOverrides:
    """--warmup and --measurement overrides must propagate to run configs."""

    def test_warmup_override(self):
        matrix = build_contention_matrix(
            ["A6.1"], [42], warmup_s=10,
        )
        assert matrix[0].warmup_s == 10

    def test_measurement_override(self):
        matrix = build_contention_matrix(
            ["A6.1"], [42], measurement_s=30,
        )
        assert matrix[0].measurement_s == 30

    def test_both_overrides(self):
        matrix = build_contention_matrix(
            ["A6.1"], [42], warmup_s=5, measurement_s=15,
        )
        rc = matrix[0]
        assert rc.warmup_s == 5
        assert rc.measurement_s == 15
        assert rc.warmup_s + rc.measurement_s == 20

    def test_override_applies_to_all_configs(self):
        matrix = build_contention_matrix(
            list(CONTENTION_CONFIGS.keys()), [42],
            warmup_s=10, measurement_s=30,
        )
        for rc in matrix:
            assert rc.warmup_s == 10
            assert rc.measurement_s == 30

    def test_no_override_uses_defaults(self):
        matrix = build_contention_matrix(["A6.1"], [42])
        rc = matrix[0]
        assert rc.warmup_s == 120
        assert rc.measurement_s == 600


# ---------------------------------------------------------------------------
# Output path tests
# ---------------------------------------------------------------------------


class TestContentionOutput:
    """Result CSV path must be under results/phase_a5_a6/."""

    def test_results_dir_path(self):
        from scripts.run_phase_a5_a6 import RESULTS_DIR
        assert RESULTS_DIR.name == "phase_a5_a6"
        assert RESULTS_DIR.parent.name == "results"
