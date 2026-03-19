"""Warm-up CV detection for Neural Pub/Sub experiments.

Implements a sliding-window throughput monitor that tracks the coefficient
of variation (CV) of throughput measurements. The warm-up phase extends
until CV drops below a configurable threshold (default 0.1), indicating
that the system has reached steady state.

Usage:
    detector = WarmupCVDetector(window_size=30, cv_threshold=0.1)
    for throughput_sample in throughput_stream:
        detector.record(throughput_sample)
        if detector.is_steady_state():
            break  # warm-up complete
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


class WarmupCVDetector:
    """Sliding-window CV detector for warm-up phase termination.

    Records throughput samples (one per second) and computes the coefficient
    of variation (CV = std / mean) over the most recent ``window_size``
    samples. Steady state is declared when CV < ``cv_threshold``.

    Attributes:
        window_size: Number of samples in the sliding window.
        cv_threshold: CV threshold below which steady state is declared.
    """

    def __init__(
        self,
        window_size: int = 30,
        cv_threshold: float = 0.1,
    ) -> None:
        self.window_size = window_size
        self.cv_threshold = cv_threshold
        self._samples: deque[float] = deque(maxlen=window_size)
        self._total_recorded: int = 0

    def record(self, throughput: float) -> None:
        """Record a throughput measurement (e.g., pipelines/sec).

        Args:
            throughput: Throughput value for the current time window.
        """
        self._samples.append(throughput)
        self._total_recorded += 1

    def current_cv(self) -> Optional[float]:
        """Return the current coefficient of variation, or None if insufficient data.

        Returns:
            CV value (std / mean), or None if fewer than window_size samples
            have been recorded or mean is zero.
        """
        if len(self._samples) < self.window_size:
            return None
        arr = np.array(self._samples)
        mean = np.mean(arr)
        if mean == 0:
            return None
        return float(np.std(arr) / mean)

    def is_steady_state(self) -> bool:
        """Return True if the system has reached steady state (CV < threshold).

        Returns False if fewer than window_size samples have been recorded.

        Returns:
            True if CV is below the configured threshold.
        """
        cv = self.current_cv()
        if cv is None:
            return False
        return cv < self.cv_threshold

    @property
    def total_recorded(self) -> int:
        """Return the total number of samples recorded."""
        return self._total_recorded
