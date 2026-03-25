"""Tests for Contention: Resource contention under overload.

Validates configuration, timing, run matrix, CLI overrides, and failure
injection for contention experiments.
"""

from __future__ import annotations

import pytest

from scripts._common import DEFAULT_SEEDS
from scripts.run_contention import (
    CONTENTION_CONFIGS,
    FAILURE_KILL_WORKERS,
    ContentionRunConfig,
    build_contention_matrix,
)


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------


class TestContentionNewConfigs:
    """Contention must have exactly 3 configs with correct parameters."""

    def test_has_exactly_three_configs(self):
        assert set(CONTENTION_CONFIGS.keys()) == {"20pps", "50pps", "10pps-kill"}

    def test_20pps_arrival_rate(self):
        assert CONTENTION_CONFIGS["20pps"]["arrival_rate"] == 20.0

    def test_50pps_arrival_rate(self):
        assert CONTENTION_CONFIGS["50pps"]["arrival_rate"] == 50.0

    def test_10pps_kill_arrival_rate(self):
        assert CONTENTION_CONFIGS["10pps-kill"]["arrival_rate"] == 10.0

    def test_20pps_no_failure(self):
        assert CONTENTION_CONFIGS["20pps"]["failure"] is None

    def test_50pps_no_failure(self):
        assert CONTENTION_CONFIGS["50pps"]["failure"] is None

    def test_10pps_kill_has_failure_injection(self):
        assert CONTENTION_CONFIGS["10pps-kill"]["failure"] == FAILURE_KILL_WORKERS


# ---------------------------------------------------------------------------
# Timing tests
# ---------------------------------------------------------------------------


class TestContentionNewTiming:
    """Default timing must be 120s warmup + 600s measurement = 720s total."""

    def test_default_warmup(self):
        rc = ContentionRunConfig(
            config_name="20pps", arrival_rate=20.0, n_workers=5, seed=42,
        )
        assert rc.warmup_s == 120

    def test_default_measurement(self):
        rc = ContentionRunConfig(
            config_name="20pps", arrival_rate=20.0, n_workers=5, seed=42,
        )
        assert rc.measurement_s == 600

    def test_default_total_duration(self):
        rc = ContentionRunConfig(
            config_name="20pps", arrival_rate=20.0, n_workers=5, seed=42,
        )
        assert rc.warmup_s + rc.measurement_s == 720

    def test_10pps_kill_failure_delay_less_than_total(self):
        rc = ContentionRunConfig(
            config_name="10pps-kill", arrival_rate=10.0,
            n_workers=5, seed=42, failure=FAILURE_KILL_WORKERS,
        )
        total = rc.warmup_s + rc.measurement_s
        assert rc.failure_delay_s < total

    def test_10pps_kill_failure_delay_default(self):
        rc = ContentionRunConfig(
            config_name="10pps-kill", arrival_rate=10.0,
            n_workers=5, seed=42, failure=FAILURE_KILL_WORKERS,
        )
        assert rc.failure_delay_s == 300

    def test_10pps_kill_post_failure_observation_sufficient(self):
        rc = ContentionRunConfig(
            config_name="10pps-kill", arrival_rate=10.0,
            n_workers=5, seed=42, failure=FAILURE_KILL_WORKERS,
        )
        total = rc.warmup_s + rc.measurement_s
        post_failure = total - rc.failure_delay_s
        assert post_failure >= 300


# ---------------------------------------------------------------------------
# Run matrix tests
# ---------------------------------------------------------------------------


class TestContentionNewMatrix:
    """Contention run matrix: 3 configs x 5 seeds = 15 runs."""

    def test_full_matrix_size(self):
        matrix = build_contention_matrix(
            list(CONTENTION_CONFIGS.keys()), DEFAULT_SEEDS,
        )
        assert len(matrix) == 15

    def test_single_config_matrix(self):
        matrix = build_contention_matrix(["20pps"], [42])
        assert len(matrix) == 1

    def test_two_configs_three_seeds(self):
        matrix = build_contention_matrix(["20pps", "50pps"], [42, 123, 456])
        assert len(matrix) == 6

    def test_matrix_preserves_config_params(self):
        matrix = build_contention_matrix(["50pps"], [42])
        assert matrix[0].arrival_rate == 50.0
        assert matrix[0].n_workers == 5
        assert matrix[0].failure is None

    def test_matrix_preserves_10pps_kill_failure(self):
        matrix = build_contention_matrix(["10pps-kill"], [42])
        assert matrix[0].failure == FAILURE_KILL_WORKERS


# ---------------------------------------------------------------------------
# CLI override tests
# ---------------------------------------------------------------------------


class TestContentionNewCLIOverrides:
    """--warmup and --measurement overrides must propagate to run configs."""

    def test_warmup_override(self):
        matrix = build_contention_matrix(["20pps"], [42], warmup_s=10)
        assert matrix[0].warmup_s == 10

    def test_measurement_override(self):
        matrix = build_contention_matrix(["20pps"], [42], measurement_s=30)
        assert matrix[0].measurement_s == 30

    def test_both_overrides(self):
        matrix = build_contention_matrix(["20pps"], [42], warmup_s=5, measurement_s=15)
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
        matrix = build_contention_matrix(["20pps"], [42])
        rc = matrix[0]
        assert rc.warmup_s == 120
        assert rc.measurement_s == 600


# ---------------------------------------------------------------------------
# Output path tests
# ---------------------------------------------------------------------------


class TestContentionNewOutput:
    """Result CSV path must be under results/contention/."""

    def test_results_dir_path(self):
        from scripts.run_contention import RESULTS_DIR
        assert RESULTS_DIR.name == "contention"
        assert RESULTS_DIR.parent.name == "results"
