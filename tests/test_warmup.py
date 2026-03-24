"""Unit tests for WarmupCVDetector (src/measurement/warmup.py).

Tests cover:
1. CV calculation with known inputs
2. Steady state detection (CV below threshold)
3. Minimum warmup (window must fill before steady state)
4. Sliding window mechanics (oldest values dropped)
5. Edge cases: zero throughput, constant throughput, single sample
6. total_recorded property
"""

import math

import numpy as np
import pytest

from src.measurement.warmup import WarmupCVDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cv(values: list[float]) -> float:
    """Reference CV calculation using numpy (std / mean)."""
    arr = np.array(values)
    return float(np.std(arr) / np.mean(arr))


# ---------------------------------------------------------------------------
# 1. CV calculation: known input, expected coefficient of variation
# ---------------------------------------------------------------------------


class TestCurrentCV:
    """Tests for WarmupCVDetector.current_cv()."""

    def test_cv_with_known_values(self):
        """CV of [1, 2, 3] with window_size=3 should match numpy reference."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.5)
        for v in [1.0, 2.0, 3.0]:
            detector.record(v)

        cv = detector.current_cv()
        expected = _cv([1.0, 2.0, 3.0])
        assert cv is not None
        assert abs(cv - expected) < 1e-10

    def test_cv_with_identical_values(self):
        """Identical values should produce CV = 0."""
        detector = WarmupCVDetector(window_size=4, cv_threshold=0.1)
        for _ in range(4):
            detector.record(5.0)

        cv = detector.current_cv()
        assert cv is not None
        assert cv == 0.0

    def test_cv_with_large_spread(self):
        """Large spread values should produce high CV."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.1)
        values = [1.0, 100.0, 1.0, 100.0, 1.0]
        for v in values:
            detector.record(v)

        cv = detector.current_cv()
        expected = _cv(values)
        assert cv is not None
        assert abs(cv - expected) < 1e-10
        assert cv > 0.5  # sanity: high variance

    def test_cv_returns_none_before_window_full(self):
        """current_cv() returns None when fewer than window_size samples recorded."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.1)
        detector.record(1.0)
        detector.record(2.0)
        detector.record(3.0)
        # Only 3 of 5 samples
        assert detector.current_cv() is None

    def test_cv_returns_none_with_zero_samples(self):
        """current_cv() returns None with no samples at all."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        assert detector.current_cv() is None

    def test_cv_returns_none_when_mean_is_zero(self):
        """current_cv() returns None when all values are zero (mean=0)."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for _ in range(3):
            detector.record(0.0)
        assert detector.current_cv() is None

    def test_cv_uses_population_std(self):
        """Verify numpy default (population std, ddof=0) is used, not sample std."""
        detector = WarmupCVDetector(window_size=4, cv_threshold=0.5)
        values = [2.0, 4.0, 6.0, 8.0]
        for v in values:
            detector.record(v)

        cv = detector.current_cv()
        # Population std (ddof=0)
        pop_cv = float(np.std(values) / np.mean(values))
        # Sample std (ddof=1) - should NOT match
        sample_cv = float(np.std(values, ddof=1) / np.mean(values))

        assert cv is not None
        assert abs(cv - pop_cv) < 1e-10
        # Confirm it's NOT using sample std
        assert abs(cv - sample_cv) > 1e-6


# ---------------------------------------------------------------------------
# 2. Steady state detection: CV drops below threshold
# ---------------------------------------------------------------------------


class TestSteadyState:
    """Tests for WarmupCVDetector.is_steady_state()."""

    def test_steady_state_when_cv_below_threshold(self):
        """is_steady_state() returns True when CV < threshold."""
        # Use constant values => CV = 0 < any threshold
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for _ in range(3):
            detector.record(10.0)
        assert detector.is_steady_state() is True

    def test_not_steady_state_when_cv_above_threshold(self):
        """is_steady_state() returns False when CV >= threshold."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.01)
        # Values with high CV
        for v in [1.0, 10.0, 1.0]:
            detector.record(v)
        assert detector.is_steady_state() is False

    def test_not_steady_state_before_window_full(self):
        """is_steady_state() returns False before window_size samples."""
        detector = WarmupCVDetector(window_size=10, cv_threshold=0.5)
        # Even constant values (CV would be 0) won't trigger before window fills
        for _ in range(9):
            detector.record(5.0)
        assert detector.is_steady_state() is False

    def test_steady_state_exactly_at_window_size(self):
        """is_steady_state() can return True exactly when window fills."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.1)
        for _ in range(5):
            detector.record(100.0)
        assert detector.is_steady_state() is True

    def test_transition_from_unstable_to_steady(self):
        """Detector transitions from not-steady to steady as variance drops."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.05)

        # Phase 1: highly variable (should not be steady)
        for v in [1.0, 100.0, 1.0, 100.0, 1.0]:
            detector.record(v)
        assert detector.is_steady_state() is False

        # Phase 2: push in stable values to displace the variable ones
        for _ in range(5):
            detector.record(50.0)
        assert detector.is_steady_state() is True

    def test_cv_exactly_at_threshold_is_not_steady(self):
        """When CV == threshold exactly, is_steady_state() returns False (strict <)."""
        # This tests the boundary: cv < threshold (not <=)
        # We need to craft values where CV equals threshold exactly.
        # CV = std/mean. For [a, b] with window=2: std = |a-b|/2, mean = (a+b)/2
        # CV = |a-b|/(a+b). Set CV = 0.1: |a-b|/(a+b) = 0.1
        # a=9, b=11: |9-11|/(9+11) = 2/20 = 0.1
        detector = WarmupCVDetector(window_size=2, cv_threshold=0.1)
        detector.record(9.0)
        detector.record(11.0)

        cv = detector.current_cv()
        assert cv is not None
        assert abs(cv - 0.1) < 1e-10
        # Strict less-than: CV == threshold should NOT be steady
        assert detector.is_steady_state() is False


# ---------------------------------------------------------------------------
# 3. Minimum warmup: window must fill before steady state declared
# ---------------------------------------------------------------------------


class TestMinimumWarmup:
    """The window_size acts as a minimum warmup period (in samples)."""

    def test_window_size_one_allows_immediate_steady_state(self):
        """With window_size=1, a single constant sample triggers steady state."""
        detector = WarmupCVDetector(window_size=1, cv_threshold=0.1)
        detector.record(10.0)
        # window_size=1: std of single value = 0, mean = 10, CV = 0
        # But wait: np.std([10.0]) = 0.0 and np.mean([10.0]) = 10.0, so CV = 0
        assert detector.current_cv() == 0.0
        assert detector.is_steady_state() is True

    def test_large_window_delays_steady_state(self):
        """With large window, many constant samples needed before steady state."""
        detector = WarmupCVDetector(window_size=100, cv_threshold=0.1)
        for i in range(99):
            detector.record(50.0)
            assert detector.is_steady_state() is False, f"Falsely steady at sample {i+1}"
        # Sample 100 fills the window
        detector.record(50.0)
        assert detector.is_steady_state() is True


# ---------------------------------------------------------------------------
# 4. Sliding window: correct size, oldest values dropped
# ---------------------------------------------------------------------------


class TestSlidingWindow:
    """Tests for the sliding window behavior (deque with maxlen)."""

    def test_window_drops_oldest(self):
        """After window fills, new values push out the oldest."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.5)
        # Fill window with [1, 2, 3]
        for v in [1.0, 2.0, 3.0]:
            detector.record(v)
        cv_before = detector.current_cv()

        # Add 3.0 -> window becomes [2, 3, 3]
        detector.record(3.0)
        cv_after = detector.current_cv()

        # CV should decrease (less variance)
        assert cv_before is not None
        assert cv_after is not None
        assert cv_after < cv_before

    def test_window_never_exceeds_size(self):
        """Internal sample count never exceeds window_size."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.1)
        for i in range(100):
            detector.record(float(i))
        # Access internal deque to verify size
        assert len(detector._samples) == 5

    def test_window_contains_most_recent_values(self):
        """After many records, window contains only the last window_size values."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.5)
        for i in range(10):
            detector.record(float(i))
        # Window should contain [7, 8, 9]
        expected_cv = _cv([7.0, 8.0, 9.0])
        cv = detector.current_cv()
        assert cv is not None
        assert abs(cv - expected_cv) < 1e-10

    def test_high_variance_evicted_restores_steady_state(self):
        """Noisy samples, once evicted from window, allow steady state."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.05)
        # Noisy start
        detector.record(1.0)
        detector.record(100.0)
        detector.record(1.0)
        assert detector.is_steady_state() is False

        # Push stable values through
        detector.record(50.0)
        detector.record(50.0)
        detector.record(50.0)
        # Window now [50, 50, 50], CV = 0
        assert detector.is_steady_state() is True


# ---------------------------------------------------------------------------
# 5. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases: zero throughput, constant throughput, single sample, etc."""

    def test_all_zeros_returns_none_cv(self):
        """All-zero throughput: mean=0, CV undefined, returns None."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for _ in range(3):
            detector.record(0.0)
        assert detector.current_cv() is None
        assert detector.is_steady_state() is False

    def test_mix_of_zeros_and_nonzero(self):
        """Mixed zero and non-zero: mean != 0, CV should be valid."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=2.0)
        detector.record(0.0)
        detector.record(0.0)
        detector.record(3.0)
        cv = detector.current_cv()
        expected = _cv([0.0, 0.0, 3.0])
        assert cv is not None
        assert abs(cv - expected) < 1e-10

    def test_negative_throughput_values(self):
        """Negative values are accepted (no validation on sign)."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.5)
        detector.record(-1.0)
        detector.record(-2.0)
        detector.record(-3.0)
        # mean = -2, std = 0.816..., CV = std/mean = -0.408...
        # Note: with negative mean, CV is negative. This is mathematically
        # odd for throughput (which shouldn't be negative) but the detector
        # doesn't validate input sign.
        cv = detector.current_cv()
        assert cv is not None

    def test_very_small_throughput_values(self):
        """Very small but non-zero values should produce valid CV."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for v in [1e-10, 1e-10, 1e-10]:
            detector.record(v)
        cv = detector.current_cv()
        assert cv is not None
        assert cv == 0.0  # identical values

    def test_very_large_throughput_values(self):
        """Very large values should produce valid CV."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for v in [1e15, 1e15, 1e15]:
            detector.record(v)
        cv = detector.current_cv()
        assert cv is not None
        assert cv == 0.0

    def test_single_sample_with_window_one(self):
        """Single sample with window_size=1: CV = 0 (std of one element)."""
        detector = WarmupCVDetector(window_size=1, cv_threshold=0.1)
        detector.record(42.0)
        cv = detector.current_cv()
        assert cv is not None
        assert cv == 0.0

    def test_constant_throughput_is_immediately_steady(self):
        """Constant throughput => CV=0, detected as steady once window fills."""
        detector = WarmupCVDetector(window_size=10, cv_threshold=0.01)
        for _ in range(10):
            detector.record(100.0)
        assert detector.current_cv() == 0.0
        assert detector.is_steady_state() is True


# ---------------------------------------------------------------------------
# 6. total_recorded property
# ---------------------------------------------------------------------------


class TestTotalRecorded:
    """Tests for the total_recorded counter."""

    def test_total_recorded_starts_at_zero(self):
        """New detector has total_recorded = 0."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.1)
        assert detector.total_recorded == 0

    def test_total_recorded_increments_on_each_record(self):
        """Each call to record() increments total_recorded by 1."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for i in range(7):
            detector.record(float(i))
        assert detector.total_recorded == 7

    def test_total_recorded_exceeds_window_size(self):
        """total_recorded counts ALL samples, not just those in the window."""
        detector = WarmupCVDetector(window_size=3, cv_threshold=0.1)
        for i in range(50):
            detector.record(float(i))
        assert detector.total_recorded == 50
        assert len(detector._samples) == 3  # window is only 3


# ---------------------------------------------------------------------------
# 7. Constructor defaults and parameter validation
# ---------------------------------------------------------------------------


class TestConstructor:
    """Tests for constructor defaults and parameter handling."""

    def test_default_parameters(self):
        """Default window_size=30, cv_threshold=0.1."""
        detector = WarmupCVDetector()
        assert detector.window_size == 30
        assert detector.cv_threshold == 0.1

    def test_custom_parameters(self):
        """Custom parameters are stored correctly."""
        detector = WarmupCVDetector(window_size=50, cv_threshold=0.05)
        assert detector.window_size == 50
        assert detector.cv_threshold == 0.05

    def test_window_size_of_one(self):
        """window_size=1 is valid and functional."""
        detector = WarmupCVDetector(window_size=1, cv_threshold=0.1)
        detector.record(5.0)
        assert detector.current_cv() == 0.0


# ---------------------------------------------------------------------------
# 8. Realistic warmup scenario (integration-style)
# ---------------------------------------------------------------------------


class TestRealisticScenario:
    """Simulates a realistic warmup sequence."""

    def test_warmup_convergence_scenario(self):
        """Simulate system warming up: high variance initially, stabilizing over time."""
        detector = WarmupCVDetector(window_size=10, cv_threshold=0.05)
        rng = np.random.default_rng(42)

        # Phase 1: unstable throughput (mean=50, std=30)
        unstable_samples = rng.normal(50, 30, size=20).tolist()
        for sample in unstable_samples:
            detector.record(max(0.1, sample))  # keep positive

        # Should likely still be unstable (high CV)
        # (Not guaranteed with random, but very likely with std=30 on mean=50)

        # Phase 2: stable throughput (mean=100, std=2)
        stable_samples = rng.normal(100, 2, size=20).tolist()
        steady_reached = False
        for sample in stable_samples:
            detector.record(sample)
            if detector.is_steady_state():
                steady_reached = True
                break

        assert steady_reached, (
            "Detector did not reach steady state after 20 stable samples "
            f"(window=10, threshold=0.05). Final CV = {detector.current_cv()}"
        )

    def test_total_recorded_tracks_full_history(self):
        """total_recorded reflects all samples, even after steady state."""
        detector = WarmupCVDetector(window_size=5, cv_threshold=0.1)
        count = 0
        for _ in range(20):
            detector.record(100.0)
            count += 1
        assert detector.total_recorded == count
