#!/usr/bin/env python3
"""Tests for Phase D recovery time analysis.

RED phase: all tests written before any implementation code.
Tests use synthetic data with known injection point and known recovery.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Module under test — will fail on import until implemented
from scripts.analyze_recovery import (
    assign_elapsed_time,
    compute_windowed_throughput,
    compute_detection_time,
    compute_recovery_time,
    compute_degradation_depth,
    analyze_single_csv,
    analyze_phase_d_recovery,
)


# ---------------------------------------------------------------------------
# Fixtures: synthetic Phase D data
# ---------------------------------------------------------------------------


def _make_synthetic_csv(
    n_rows: int = 600,
    effective_rate: float = 4.0,
    injection_row: int = 300,
    failure_rows: list[int] | None = None,
    recovery_row: int = 350,
    throughput_pps: float = 4.0,
    degraded_latency_factor: float = 1.5,
) -> pd.DataFrame:
    """Create a synthetic Phase D CSV with known properties.

    - Rows 0..injection_row-1: normal operation (all success=True)
    - Rows injection_row..recovery_row-1: some failures, degraded throughput
    - Rows recovery_row..n_rows-1: recovered (all success=True, normal throughput)

    The 'failure_rows' list specifies exactly which rows have success=False.
    If None, defaults to a block of 10 rows starting at injection_row.
    """
    if failure_rows is None:
        failure_rows = list(range(injection_row, injection_row + 10))

    rng = np.random.default_rng(42)
    pipeline_types = ["cqi_prediction", "anomaly_detection", "sensor_fusion"]

    rows = []
    for i in range(n_rows):
        ptype = pipeline_types[i % 3]
        success = i not in failure_rows

        if success:
            if injection_row <= i < recovery_row:
                # Degraded but successful
                latency = rng.normal(1300 * degraded_latency_factor, 50)
            else:
                latency = rng.normal(1300, 50)
        else:
            latency = np.nan

        rows.append({
            "pipeline_id": f"pipe-{i:04d}",
            "pipeline_type": ptype,
            "success": success,
            "error": "" if success else "worker_killed",
            "e2e_latency_ms": latency,
            "throughput_pps": throughput_pps,
            "completion_rate": 1.0 if success else 0.0,
            "governance_violations": 0,
            "federation_bytes_sent": 1000,
            "routing_accuracy_f1": 1.0,
            "warmup": False,
        })

    return pd.DataFrame(rows)


@pytest.fixture
def synthetic_df():
    """Standard synthetic DataFrame with known failure at row 300, recovery at 350."""
    return _make_synthetic_csv()


@pytest.fixture
def synthetic_csv_dir(tmp_path):
    """Directory with synthetic D1 and D2 CSVs for integration testing."""
    for config in ["D1", "D2"]:
        for seed in [42, 0]:
            df = _make_synthetic_csv(
                n_rows=600,
                injection_row=300,
                failure_rows=list(range(300, 310)),
                recovery_row=350,
            )
            fname = f"{config}_failure-worker_seed-{seed}.csv"
            df.to_csv(tmp_path / fname, index=False)
    return tmp_path


# ---------------------------------------------------------------------------
# Test: assign_elapsed_time
# ---------------------------------------------------------------------------


class TestAssignElapsedTime:
    def test_elapsed_time_monotonic(self, synthetic_df):
        """Elapsed time increases monotonically with row index."""
        result = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        assert result["elapsed_s"].is_monotonic_increasing

    def test_elapsed_time_range(self, synthetic_df):
        """Elapsed time spans from 0 to approximately total_duration_s."""
        total_s = 150.0
        result = assign_elapsed_time(synthetic_df, total_duration_s=total_s)
        assert result["elapsed_s"].iloc[0] == pytest.approx(0.0, abs=1.0)
        # Last row should be close to total_duration
        assert result["elapsed_s"].iloc[-1] == pytest.approx(
            total_s, abs=total_s / len(synthetic_df) + 0.1
        )

    def test_elapsed_time_from_throughput(self, synthetic_df):
        """When total_duration_s is None, infer from total_rows / throughput_pps."""
        result = assign_elapsed_time(synthetic_df, total_duration_s=None)
        expected_total = len(synthetic_df) / synthetic_df["throughput_pps"].iloc[0]
        assert result["elapsed_s"].iloc[-1] == pytest.approx(
            expected_total, abs=expected_total / len(synthetic_df) + 0.1
        )


# ---------------------------------------------------------------------------
# Test: windowed throughput computation
# ---------------------------------------------------------------------------


class TestWindowedThroughput:
    def test_window_count(self, synthetic_df):
        """Number of windows matches expected count for given duration and window size."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        windows = compute_windowed_throughput(df, window_s=5.0)
        # 150s / 5s = 30 windows
        assert len(windows) == 30

    def test_pre_failure_throughput_stable(self, synthetic_df):
        """Pre-failure windows all have similar throughput (all rows successful)."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        windows = compute_windowed_throughput(df, window_s=5.0)
        # Injection at row 300 = 75s into 150s. Windows before 75s (first 15 windows)
        pre_failure = windows[windows["window_start_s"] < 70.0]
        assert len(pre_failure) > 5
        # All pre-failure windows should have >0 throughput
        assert (pre_failure["throughput_pps"] > 0).all()
        # CV should be low
        cv = pre_failure["throughput_pps"].std() / pre_failure["throughput_pps"].mean()
        assert cv < 0.3  # reasonable stability

    def test_output_columns(self, synthetic_df):
        """Windowed throughput output has expected columns."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        windows = compute_windowed_throughput(df, window_s=5.0)
        assert "window_start_s" in windows.columns
        assert "window_end_s" in windows.columns
        assert "throughput_pps" in windows.columns
        assert "n_success" in windows.columns
        assert "n_total" in windows.columns

    def test_failure_window_reduced_throughput(self):
        """Windows containing failures show reduced throughput."""
        df = _make_synthetic_csv(
            n_rows=200,
            injection_row=100,
            failure_rows=list(range(100, 120)),  # 20 failures
            recovery_row=130,
        )
        df = assign_elapsed_time(df, total_duration_s=50.0)
        windows = compute_windowed_throughput(df, window_s=5.0)

        # Pre-failure mean throughput
        pre = windows[windows["window_end_s"] <= 24.0]["throughput_pps"].mean()
        # Window containing failures (around 25-30s)
        fail_window = windows[
            (windows["window_start_s"] >= 24.0) & (windows["window_end_s"] <= 35.0)
        ]["throughput_pps"].min()
        assert fail_window < pre


# ---------------------------------------------------------------------------
# Test: detection time
# ---------------------------------------------------------------------------


class TestDetectionTime:
    def test_detection_time_non_negative(self, synthetic_df):
        """Detection time is non-negative (failure at or after injection)."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        injection_s = 75.0  # row 300 / 600 rows * 150s
        dt = compute_detection_time(df, injection_s=injection_s)
        assert dt >= 0

    def test_detection_time_bounded(self, synthetic_df):
        """Detection time should be within a reasonable bound after injection."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        injection_s = 75.0
        dt = compute_detection_time(df, injection_s=injection_s)
        # First failure is at row 300, which is at exactly 75s
        # Detection should be very close to 0 (or at most a few time steps)
        assert dt < 5.0  # within 5 seconds

    def test_detection_time_no_failure(self):
        """Detection time is NaN when no failures exist after injection."""
        df = _make_synthetic_csv(n_rows=200, injection_row=100, failure_rows=[])
        df = assign_elapsed_time(df, total_duration_s=50.0)
        dt = compute_detection_time(df, injection_s=25.0)
        assert np.isnan(dt)

    def test_detection_time_only_counts_post_injection(self):
        """Failures before injection point are ignored."""
        # Place a failure before injection
        df = _make_synthetic_csv(
            n_rows=200, injection_row=100,
            failure_rows=[50, 100, 101],
        )
        df = assign_elapsed_time(df, total_duration_s=50.0)
        injection_s = 25.0  # row 100 out of 200 at 50s total
        dt = compute_detection_time(df, injection_s=injection_s)
        # Should find failure at row 100 (exactly at injection time), not row 50
        assert dt >= 0.0


# ---------------------------------------------------------------------------
# Test: recovery time
# ---------------------------------------------------------------------------


class TestRecoveryTime:
    def test_recovery_time_positive(self, synthetic_df):
        """Recovery time is positive."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        injection_s = 75.0
        rt = compute_recovery_time(
            df, injection_s=injection_s, window_s=5.0, threshold=0.9,
        )
        assert rt > 0

    def test_recovery_time_reasonable(self, synthetic_df):
        """Recovery time should be finite and within measurement window."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        injection_s = 75.0
        rt = compute_recovery_time(
            df, injection_s=injection_s, window_s=5.0, threshold=0.9,
        )
        assert rt < 150.0 - injection_s  # must recover before end

    def test_recovery_time_nan_no_recovery(self):
        """Recovery time is NaN if throughput never recovers."""
        # All rows after injection are failures
        n = 200
        df = _make_synthetic_csv(
            n_rows=n, injection_row=100,
            failure_rows=list(range(100, n)),
            recovery_row=n,  # never recovers
        )
        df = assign_elapsed_time(df, total_duration_s=50.0)
        rt = compute_recovery_time(
            df, injection_s=25.0, window_s=5.0, threshold=0.9,
        )
        assert np.isnan(rt)

    def test_recovery_uses_90pct_threshold(self):
        """Recovery is reached when windowed throughput >= 90% of pre-failure."""
        # Create data where throughput partially recovers
        df = _make_synthetic_csv(
            n_rows=400,
            injection_row=200,
            failure_rows=list(range(200, 220)),
            recovery_row=250,  # full recovery at row 250
        )
        df = assign_elapsed_time(df, total_duration_s=100.0)
        rt = compute_recovery_time(
            df, injection_s=50.0, window_s=5.0, threshold=0.9,
        )
        # Recovery should be detected sometime after the failure window
        assert rt > 0
        # And before end of measurement
        assert rt < 50.0


# ---------------------------------------------------------------------------
# Test: degradation depth
# ---------------------------------------------------------------------------


class TestDegradationDepth:
    def test_degradation_depth_range(self, synthetic_df):
        """Degradation depth is between 0 and 1."""
        df = assign_elapsed_time(synthetic_df, total_duration_s=150.0)
        depth = compute_degradation_depth(
            df, injection_s=75.0, window_s=5.0,
        )
        assert 0.0 <= depth <= 1.0

    def test_degradation_depth_with_failures(self):
        """Degradation depth < 1.0 when there are failures."""
        df = _make_synthetic_csv(
            n_rows=200, injection_row=100,
            failure_rows=list(range(100, 120)),
            recovery_row=130,
        )
        df = assign_elapsed_time(df, total_duration_s=50.0)
        depth = compute_degradation_depth(
            df, injection_s=25.0, window_s=5.0,
        )
        assert depth < 1.0

    def test_degradation_depth_no_failure(self):
        """Degradation depth is ~1.0 when no failures occur."""
        df = _make_synthetic_csv(
            n_rows=200, injection_row=100, failure_rows=[],
            recovery_row=100,
        )
        df = assign_elapsed_time(df, total_duration_s=50.0)
        depth = compute_degradation_depth(
            df, injection_s=25.0, window_s=5.0,
        )
        assert depth == pytest.approx(1.0, abs=0.15)


# ---------------------------------------------------------------------------
# Test: full single-CSV analysis
# ---------------------------------------------------------------------------


class TestAnalyzeSingleCSV:
    def test_returns_all_metrics(self, synthetic_df):
        """analyze_single_csv returns dict with all required metric keys."""
        result = analyze_single_csv(
            synthetic_df, injection_s=75.0, total_duration_s=150.0,
        )
        expected_keys = {
            "detection_time_s",
            "recovery_time_s",
            "degradation_depth",
            "failed_pipelines",
            "pre_throughput",
            "post_throughput",
            "pre_p50_latency",
            "post_p50_latency",
        }
        assert expected_keys.issubset(result.keys())

    def test_failed_pipelines_count(self, synthetic_df):
        """Failed pipeline count matches actual success=False rows after injection."""
        result = analyze_single_csv(
            synthetic_df, injection_s=75.0, total_duration_s=150.0,
        )
        # Our synthetic data has 10 failure rows (300-309)
        assert result["failed_pipelines"] == 10

    def test_pre_post_throughput(self, synthetic_df):
        """Pre-failure throughput is greater than zero; post-recovery also > 0."""
        result = analyze_single_csv(
            synthetic_df, injection_s=75.0, total_duration_s=150.0,
        )
        assert result["pre_throughput"] > 0
        assert result["post_throughput"] > 0

    def test_pre_post_latency(self, synthetic_df):
        """Pre and post p50 latency are finite positive numbers."""
        result = analyze_single_csv(
            synthetic_df, injection_s=75.0, total_duration_s=150.0,
        )
        assert result["pre_p50_latency"] > 0
        assert np.isfinite(result["pre_p50_latency"])
        assert result["post_p50_latency"] > 0
        assert np.isfinite(result["post_p50_latency"])


# ---------------------------------------------------------------------------
# Test: full Phase D analysis (directory of CSVs)
# ---------------------------------------------------------------------------


class TestAnalyzePhaseDRecovery:
    def test_output_csv_schema(self, synthetic_csv_dir):
        """Output CSV has all required columns."""
        out_csv = synthetic_csv_dir / "recovery_summary.csv"
        analyze_phase_d_recovery(
            results_dir=str(synthetic_csv_dir),
            output_csv=str(out_csv),
            injection_s=75.0,
            total_duration_s=150.0,
        )
        df = pd.read_csv(out_csv)
        required_cols = {
            "config", "seed", "detection_time_s", "recovery_time_s",
            "degradation_depth", "failed_pipelines",
            "pre_throughput", "post_throughput",
            "pre_p50_latency", "post_p50_latency",
        }
        assert required_cols.issubset(set(df.columns))

    def test_output_row_count(self, synthetic_csv_dir):
        """Output has one row per CSV file."""
        out_csv = synthetic_csv_dir / "recovery_summary.csv"
        analyze_phase_d_recovery(
            results_dir=str(synthetic_csv_dir),
            output_csv=str(out_csv),
            injection_s=75.0,
            total_duration_s=150.0,
        )
        df = pd.read_csv(out_csv)
        # 2 configs x 2 seeds = 4 CSVs
        assert len(df) == 4

    def test_config_and_seed_extracted(self, synthetic_csv_dir):
        """Config and seed are correctly parsed from filenames."""
        out_csv = synthetic_csv_dir / "recovery_summary.csv"
        analyze_phase_d_recovery(
            results_dir=str(synthetic_csv_dir),
            output_csv=str(out_csv),
            injection_s=75.0,
            total_duration_s=150.0,
        )
        df = pd.read_csv(out_csv)
        assert set(df["config"].unique()) == {"D1", "D2"}
        assert set(df["seed"].unique()) == {0, 42}

    def test_timeseries_output(self, synthetic_csv_dir):
        """Time-series throughput data is generated for TikZ plots."""
        out_csv = synthetic_csv_dir / "recovery_summary.csv"
        ts_csv = synthetic_csv_dir / "recovery_timeseries.csv"
        analyze_phase_d_recovery(
            results_dir=str(synthetic_csv_dir),
            output_csv=str(out_csv),
            timeseries_csv=str(ts_csv),
            injection_s=75.0,
            total_duration_s=150.0,
            window_s=5.0,
        )
        assert ts_csv.exists()
        ts_df = pd.read_csv(ts_csv)
        assert "config" in ts_df.columns
        assert "window_start_s" in ts_df.columns
        assert "throughput_pps_mean" in ts_df.columns
        assert "throughput_pps_std" in ts_df.columns
