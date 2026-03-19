"""Tests for scripts/analyze_results.py and warm-up CV detection.

Written RED-first per strict TDD. Each test targets a specific function or
output format required by the manuscript's statistical methodology (Section 5).

Test plan:
    1. A12 effect size: known values for identical and dominated distributions
    2. Holm-Bonferroni correction: known p-values
    3. Bootstrap CI: synthetic data with known median
    4. Full analysis pipeline: synthetic CSV with known properties
    5. LaTeX table output format
    6. JSON summary structure
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# 1. Vargha-Delaney A12 effect size
# ---------------------------------------------------------------------------


def test_a12_identical_distributions():
    """A12 of two identical distributions must be 0.5 (no effect)."""
    from scripts.analyze_results import vargha_delaney_a12

    x = [1, 2, 3, 4, 5]
    y = [1, 2, 3, 4, 5]
    assert abs(vargha_delaney_a12(x, y) - 0.5) < 1e-6


def test_a12_one_always_greater():
    """If every element of x > every element of y, A12 must be 1.0."""
    from scripts.analyze_results import vargha_delaney_a12

    x = [10, 11, 12, 13, 14]
    y = [1, 2, 3, 4, 5]
    assert abs(vargha_delaney_a12(x, y) - 1.0) < 1e-6


def test_a12_one_always_less():
    """If every element of x < every element of y, A12 must be 0.0."""
    from scripts.analyze_results import vargha_delaney_a12

    x = [1, 2, 3, 4, 5]
    y = [10, 11, 12, 13, 14]
    assert abs(vargha_delaney_a12(x, y) - 0.0) < 1e-6


def test_a12_effect_size_label():
    """A12 helper must classify effect sizes: negligible, small, medium, large."""
    from scripts.analyze_results import a12_effect_label

    assert a12_effect_label(0.50) == "negligible"
    assert a12_effect_label(0.56) == "negligible"
    assert a12_effect_label(0.60) == "small"
    assert a12_effect_label(0.70) == "medium"
    assert a12_effect_label(0.80) == "large"
    # Symmetric: values below 0.5
    assert a12_effect_label(0.40) == "small"
    assert a12_effect_label(0.30) == "medium"
    assert a12_effect_label(0.20) == "large"


# ---------------------------------------------------------------------------
# 2. Holm-Bonferroni correction
# ---------------------------------------------------------------------------


def test_holm_bonferroni_all_significant():
    """Three small p-values should all remain significant after correction."""
    from scripts.analyze_results import holm_bonferroni

    p_values = [0.001, 0.002, 0.003]
    corrected = holm_bonferroni(p_values, alpha=0.05)
    assert len(corrected) == 3
    # All should be significant (adjusted p < 0.05)
    for adj_p, significant in corrected:
        assert significant is True


def test_holm_bonferroni_none_significant():
    """Three large p-values should all be non-significant."""
    from scripts.analyze_results import holm_bonferroni

    p_values = [0.5, 0.6, 0.7]
    corrected = holm_bonferroni(p_values, alpha=0.05)
    for adj_p, significant in corrected:
        assert significant is False


def test_holm_bonferroni_mixed():
    """One small and two large p-values: only the small one significant."""
    from scripts.analyze_results import holm_bonferroni

    p_values = [0.01, 0.5, 0.8]
    corrected = holm_bonferroni(p_values, alpha=0.05)
    # After sorting: 0.01 * 3 = 0.03 < 0.05 (significant)
    # 0.5 * 2 = 1.0 > 0.05 (not significant)
    # 0.8 * 1 = 0.8 > 0.05 (not significant)
    significant_flags = [sig for _, sig in corrected]
    assert significant_flags[0] is True  # p=0.01
    assert significant_flags[1] is False  # p=0.5
    assert significant_flags[2] is False  # p=0.8


def test_holm_bonferroni_preserves_order():
    """Output must be in original input order, not sorted order."""
    from scripts.analyze_results import holm_bonferroni

    p_values = [0.5, 0.001, 0.3]
    corrected = holm_bonferroni(p_values, alpha=0.05)
    # Original order preserved: [0.5, 0.001, 0.3]
    # p=0.5 -> not significant, p=0.001 -> significant, p=0.3 -> not significant
    assert corrected[1][1] is True   # p=0.001 was significant
    assert corrected[0][1] is False  # p=0.5 was not significant


# ---------------------------------------------------------------------------
# 3. Bootstrap CI
# ---------------------------------------------------------------------------


def test_bootstrap_ci_median_contains_true_value():
    """Bootstrap CI for median of N(100, 1) data must contain 100."""
    from scripts.analyze_results import bootstrap_ci

    rng = np.random.default_rng(42)
    data = rng.normal(100, 1, size=1000)
    lo, hi = bootstrap_ci(data, statistic="median", n_resamples=5000, seed=42)
    assert lo < 100 < hi, f"CI [{lo:.2f}, {hi:.2f}] does not contain 100"


def test_bootstrap_ci_p95_contains_true_value():
    """Bootstrap CI for p95 of N(0, 1) data must contain ~1.645."""
    from scripts.analyze_results import bootstrap_ci

    rng = np.random.default_rng(123)
    data = rng.normal(0, 1, size=2000)
    lo, hi = bootstrap_ci(data, statistic="p95", n_resamples=5000, seed=123)
    # Theoretical p95 of standard normal is ~1.645
    assert lo < 1.645 < hi, f"CI [{lo:.2f}, {hi:.2f}] does not contain 1.645"


def test_bootstrap_ci_returns_tuple_of_two_floats():
    """bootstrap_ci must return a (lo, hi) tuple of floats."""
    from scripts.analyze_results import bootstrap_ci

    data = np.arange(100, dtype=float)
    result = bootstrap_ci(data, statistic="median")
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], float)
    assert isinstance(result[1], float)
    assert result[0] < result[1]


# ---------------------------------------------------------------------------
# 4. Full analysis pipeline
# ---------------------------------------------------------------------------


def _make_synthetic_csv(path: str, n_per_config: int = 200) -> None:
    """Create a synthetic Phase A CSV with known properties.

    Configs S1-S4 with latencies drawn from distributions where S4 < S1.
    """
    rng = np.random.default_rng(42)
    configs = {
        "S1": 100.0,  # mean latency 100ms
        "S2": 80.0,   # mean latency 80ms
        "S3": 90.0,   # mean latency 90ms
        "S4": 50.0,   # mean latency 50ms (Neural Pub/Sub, best)
    }
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "pipeline_id", "pipeline_type", "config_name", "success",
            "e2e_latency_ms", "throughput_pps",
        ])
        for config, mean in configs.items():
            for i in range(n_per_config):
                latency = max(1.0, rng.normal(mean, 10.0))
                writer.writerow([
                    f"{config}_{i:04d}", "cqi_prediction", config, "True",
                    f"{latency:.4f}", "5.0",
                ])


def test_analyze_phase_a_runs_without_error():
    """analyze_phase_a must run without error on synthetic data."""
    from scripts.analyze_results import analyze_phase_a

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        result = analyze_phase_a(path)
        assert isinstance(result, dict)
    finally:
        os.unlink(path)


def test_analyze_phase_a_has_contrasts():
    """Result must contain KS test results for 3 planned contrasts."""
    from scripts.analyze_results import analyze_phase_a

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        result = analyze_phase_a(path)
        contrasts = result["contrasts"]
        assert len(contrasts) == 3
        # Contrasts must be S4 vs S1, S4 vs S2, S4 vs S3
        contrast_names = {c["comparison"] for c in contrasts}
        assert contrast_names == {"S4 vs S1", "S4 vs S2", "S4 vs S3"}
    finally:
        os.unlink(path)


def test_analyze_phase_a_contrast_keys():
    """Each contrast must include ks_statistic, p_value, a12, wasserstein, and holm_significant."""
    from scripts.analyze_results import analyze_phase_a

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        result = analyze_phase_a(path)
        for contrast in result["contrasts"]:
            for key in ["comparison", "ks_statistic", "p_value", "adjusted_p",
                        "holm_significant", "a12", "a12_label", "wasserstein_ms"]:
                assert key in contrast, f"Missing key: {key}"
    finally:
        os.unlink(path)


def test_analyze_phase_a_s4_better_than_s1():
    """With synthetic data, S4 (mean 50) should dominate S1 (mean 100)."""
    from scripts.analyze_results import analyze_phase_a

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        result = analyze_phase_a(path)
        s4_vs_s1 = next(c for c in result["contrasts"] if c["comparison"] == "S4 vs S1")
        # KS test should be significant
        assert s4_vs_s1["holm_significant"] is True
        # Wasserstein should reflect ~50ms difference
        assert s4_vs_s1["wasserstein_ms"] > 30
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# 5. LaTeX output format
# ---------------------------------------------------------------------------


def test_latex_table_output():
    """to_latex_table must produce valid LaTeX tabular fragment."""
    from scripts.analyze_results import to_latex_table

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        from scripts.analyze_results import analyze_phase_a
        result = analyze_phase_a(path)
        latex = to_latex_table(result)
        assert isinstance(latex, str)
        assert "\\begin{tabular}" in latex
        assert "\\end{tabular}" in latex
        assert "S4 vs S1" in latex
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# 6. JSON summary structure
# ---------------------------------------------------------------------------


def test_json_summary_structure():
    """to_json_summary must produce valid JSON with required top-level keys."""
    from scripts.analyze_results import analyze_phase_a, to_json_summary

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        result = analyze_phase_a(path)
        json_str = to_json_summary(result)
        parsed = json.loads(json_str)
        assert "contrasts" in parsed
        assert "bootstrap_cis" in parsed
        assert "descriptive_stats" in parsed
    finally:
        os.unlink(path)


def test_json_summary_bootstrap_cis():
    """JSON summary must include bootstrap CIs for median and p95 per config."""
    from scripts.analyze_results import analyze_phase_a, to_json_summary

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        _make_synthetic_csv(path)
        result = analyze_phase_a(path)
        json_str = to_json_summary(result)
        parsed = json.loads(json_str)
        cis = parsed["bootstrap_cis"]
        # Must have entries for all 4 configs
        assert len(cis) >= 4
        for entry in cis:
            assert "config" in entry
            assert "median_ci" in entry
            assert "p95_ci" in entry
            # Each CI must be [lo, hi]
            assert len(entry["median_ci"]) == 2
            assert len(entry["p95_ci"]) == 2
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# 7. Warm-up CV detection (Phase 3.2)
# ---------------------------------------------------------------------------


def test_cv_detector_identifies_steady_state():
    """CV detector must return True (steady state) for constant throughput."""
    from src.measurement.warmup import WarmupCVDetector

    detector = WarmupCVDetector(window_size=30, cv_threshold=0.1)
    # Feed constant throughput: CV should be 0
    for _ in range(60):
        detector.record(100.0)  # 100 pipelines/sec every second
    assert detector.is_steady_state() is True


def test_cv_detector_rejects_high_variance():
    """CV detector must return False for highly variable throughput."""
    from src.measurement.warmup import WarmupCVDetector

    rng = np.random.default_rng(42)
    detector = WarmupCVDetector(window_size=30, cv_threshold=0.1)
    # Feed wildly varying throughput: CV should be >> 0.1
    for _ in range(60):
        detector.record(rng.uniform(1.0, 200.0))
    assert detector.is_steady_state() is False


def test_cv_detector_requires_full_window():
    """CV detector must return False before window_size samples are collected."""
    from src.measurement.warmup import WarmupCVDetector

    detector = WarmupCVDetector(window_size=30, cv_threshold=0.1)
    # Feed fewer than window_size samples
    for _ in range(10):
        detector.record(100.0)
    assert detector.is_steady_state() is False


def test_cv_detector_current_cv():
    """CV detector must expose the current CV value."""
    from src.measurement.warmup import WarmupCVDetector

    detector = WarmupCVDetector(window_size=10, cv_threshold=0.1)
    for _ in range(10):
        detector.record(100.0)
    cv = detector.current_cv()
    assert cv is not None
    assert abs(cv) < 1e-6  # constant values -> CV = 0
