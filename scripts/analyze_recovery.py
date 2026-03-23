#!/usr/bin/env python3
"""Post-hoc recovery time analysis for Phase D experiment data.

Computes recovery metrics from existing Phase D CSVs without re-running
experiments. Each CSV contains per-pipeline rows in time order with
timestamps inferred from row position and known throughput.

Metrics:
    - Detection time: injection to first success=False
    - Recovery time: first failure to 90% pre-failure throughput restoration
    - Degradation depth: min(post-failure throughput / pre-failure throughput)
    - Failed pipelines: count of success=False in post-injection window
    - Steady-state comparison: pre vs post latency and throughput

Usage:
    python scripts/analyze_recovery.py \\
        --results-dir results/phase_d/ \\
        --output-csv results/phase_d/recovery_summary.csv \\
        --timeseries-csv results/phase_d/recovery_timeseries.csv \\
        --injection-s 300.0 \\
        --total-duration-s 720.0
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Elapsed time assignment
# ---------------------------------------------------------------------------


def assign_elapsed_time(
    df: pd.DataFrame, total_duration_s: float | None = None,
) -> pd.DataFrame:
    """Assign elapsed_s column based on row index and total duration.

    If total_duration_s is None, infers it from total_rows / throughput_pps.

    Args:
        df: DataFrame with rows in time order. Must have throughput_pps column
            if total_duration_s is None.
        total_duration_s: Total measurement duration in seconds.

    Returns:
        Copy of df with an elapsed_s column added.
    """
    df = df.copy()
    n = len(df)
    if total_duration_s is None:
        tput = df["throughput_pps"].iloc[0]
        total_duration_s = n / tput

    # Each row represents one pipeline completion, evenly spaced in time
    df["elapsed_s"] = np.linspace(0, total_duration_s, n, endpoint=False)
    return df


# ---------------------------------------------------------------------------
# Windowed throughput
# ---------------------------------------------------------------------------


def compute_windowed_throughput(
    df: pd.DataFrame, window_s: float = 5.0,
) -> pd.DataFrame:
    """Compute throughput in fixed-width time windows.

    Args:
        df: DataFrame with elapsed_s and success columns.
        window_s: Window width in seconds.

    Returns:
        DataFrame with columns: window_start_s, window_end_s, throughput_pps,
        n_success, n_total.
    """
    max_t = df["elapsed_s"].max()
    windows = []
    t = 0.0
    while t < max_t:
        t_end = t + window_s
        mask = (df["elapsed_s"] >= t) & (df["elapsed_s"] < t_end)
        window_rows = df[mask]
        n_total = len(window_rows)
        n_success = int(window_rows["success"].sum())
        throughput = n_success / window_s
        windows.append({
            "window_start_s": t,
            "window_end_s": t_end,
            "throughput_pps": throughput,
            "n_success": n_success,
            "n_total": n_total,
        })
        t = t_end

    return pd.DataFrame(windows)


# ---------------------------------------------------------------------------
# Detection time
# ---------------------------------------------------------------------------


def compute_detection_time(
    df: pd.DataFrame, injection_s: float,
) -> float:
    """Time from injection to first failed pipeline.

    Args:
        df: DataFrame with elapsed_s and success columns.
        injection_s: Injection time in seconds.

    Returns:
        Detection time in seconds, or NaN if no failures after injection.
    """
    post_injection = df[df["elapsed_s"] >= injection_s]
    failed = post_injection[~post_injection["success"]]
    if failed.empty:
        return float("nan")
    first_fail_time = failed["elapsed_s"].iloc[0]
    return first_fail_time - injection_s


# ---------------------------------------------------------------------------
# Recovery time
# ---------------------------------------------------------------------------


def compute_recovery_time(
    df: pd.DataFrame,
    injection_s: float,
    window_s: float = 5.0,
    threshold: float = 0.9,
) -> float:
    """Time from first failure to 90% pre-failure throughput restoration.

    Pre-failure throughput is the mean throughput over windows ending before
    injection_s. Recovery is the first post-failure window where throughput
    reaches threshold * pre_failure_throughput.

    Args:
        df: DataFrame with elapsed_s and success columns.
        injection_s: Injection time in seconds.
        window_s: Window width in seconds.
        threshold: Recovery threshold as fraction of pre-failure throughput.

    Returns:
        Recovery time in seconds, or NaN if no recovery.
    """
    windows = compute_windowed_throughput(df, window_s=window_s)

    # Pre-failure throughput: windows that end before injection
    pre_windows = windows[windows["window_end_s"] <= injection_s]
    if pre_windows.empty:
        return float("nan")
    pre_throughput = pre_windows["throughput_pps"].mean()
    if pre_throughput <= 0:
        return float("nan")

    recovery_target = threshold * pre_throughput

    # Find first failure time
    post_injection = df[df["elapsed_s"] >= injection_s]
    failed = post_injection[~post_injection["success"]]
    if failed.empty:
        return float("nan")
    first_fail_time = failed["elapsed_s"].iloc[0]

    # Find first window after failure where throughput >= target
    post_fail_windows = windows[windows["window_start_s"] > first_fail_time]
    recovered = post_fail_windows[
        post_fail_windows["throughput_pps"] >= recovery_target
    ]
    if recovered.empty:
        return float("nan")

    recovery_window_start = recovered["window_start_s"].iloc[0]
    return recovery_window_start - first_fail_time


# ---------------------------------------------------------------------------
# Degradation depth
# ---------------------------------------------------------------------------


def compute_degradation_depth(
    df: pd.DataFrame,
    injection_s: float,
    window_s: float = 5.0,
) -> float:
    """Minimum throughput ratio after failure vs pre-failure.

    Returns the ratio of the worst post-injection window throughput to the
    pre-failure mean throughput. 1.0 means no degradation, 0.0 means
    complete outage.

    Args:
        df: DataFrame with elapsed_s and success columns.
        injection_s: Injection time in seconds.
        window_s: Window width in seconds.

    Returns:
        Degradation depth ratio in [0, 1].
    """
    windows = compute_windowed_throughput(df, window_s=window_s)

    pre_windows = windows[windows["window_end_s"] <= injection_s]
    if pre_windows.empty:
        return 1.0
    pre_throughput = pre_windows["throughput_pps"].mean()
    if pre_throughput <= 0:
        return 1.0

    post_windows = windows[windows["window_start_s"] >= injection_s]
    if post_windows.empty:
        return 1.0

    min_post = post_windows["throughput_pps"].min()
    return min_post / pre_throughput


# ---------------------------------------------------------------------------
# Single CSV analysis
# ---------------------------------------------------------------------------


def analyze_single_csv(
    df: pd.DataFrame,
    injection_s: float,
    total_duration_s: float | None = None,
    window_s: float = 5.0,
    threshold: float = 0.9,
) -> dict:
    """Analyze a single Phase D CSV and return all recovery metrics.

    Args:
        df: Raw DataFrame from one Phase D CSV.
        injection_s: Injection time in seconds.
        total_duration_s: Total run duration in seconds (inferred if None).
        window_s: Window width for throughput computation.
        threshold: Recovery threshold fraction.

    Returns:
        Dict with detection_time_s, recovery_time_s, degradation_depth,
        failed_pipelines, pre_throughput, post_throughput,
        pre_p50_latency, post_p50_latency.
    """
    df = assign_elapsed_time(df, total_duration_s=total_duration_s)

    detection = compute_detection_time(df, injection_s=injection_s)
    recovery = compute_recovery_time(
        df, injection_s=injection_s, window_s=window_s, threshold=threshold,
    )
    depth = compute_degradation_depth(
        df, injection_s=injection_s, window_s=window_s,
    )

    # Count failed pipelines after injection
    post_injection = df[df["elapsed_s"] >= injection_s]
    failed_count = int((~post_injection["success"]).sum())

    # Steady-state comparison: pre-failure vs post-recovery
    # Pre-failure: everything before injection with success=True
    pre = df[(df["elapsed_s"] < injection_s) & (df["success"])]
    pre_throughput = len(pre) / injection_s if injection_s > 0 else 0.0
    pre_p50 = float(pre["e2e_latency_ms"].median()) if not pre.empty else float("nan")

    # Post-recovery: successful rows after recovery point
    # Use last 30% of measurement as post-recovery window
    total_s = df["elapsed_s"].max()
    post_start = total_s * 0.7
    post = df[(df["elapsed_s"] >= post_start) & (df["success"])]
    post_duration = total_s - post_start
    post_throughput = len(post) / post_duration if post_duration > 0 else 0.0
    post_p50 = float(post["e2e_latency_ms"].median()) if not post.empty else float("nan")

    return {
        "detection_time_s": detection,
        "recovery_time_s": recovery,
        "degradation_depth": depth,
        "failed_pipelines": failed_count,
        "pre_throughput": pre_throughput,
        "post_throughput": post_throughput,
        "pre_p50_latency": pre_p50,
        "post_p50_latency": post_p50,
    }


# ---------------------------------------------------------------------------
# Phase D directory analysis
# ---------------------------------------------------------------------------


def _parse_filename(fname: str) -> tuple[str, int] | None:
    """Extract config and seed from Phase D filename.

    Expected pattern: D1_failure-worker_seed-42.csv
    Returns (config, seed) or None if pattern doesn't match.
    """
    match = re.match(r"^(D\d+)_failure-\w+_seed-(\d+)\.csv$", fname)
    if match:
        return match.group(1), int(match.group(2))
    return None


def analyze_phase_d_recovery(
    results_dir: str,
    output_csv: str,
    timeseries_csv: str | None = None,
    injection_s: float = 300.0,
    total_duration_s: float | None = None,
    window_s: float = 5.0,
    threshold: float = 0.9,
) -> pd.DataFrame:
    """Analyze all Phase D CSVs in a directory.

    Args:
        results_dir: Directory containing Phase D CSV files.
        output_csv: Path for output summary CSV.
        timeseries_csv: Optional path for time-series throughput data.
        injection_s: Injection time in seconds from run start.
        total_duration_s: Total run duration (inferred if None).
        window_s: Window width in seconds.
        threshold: Recovery threshold fraction.

    Returns:
        Summary DataFrame.
    """
    results_path = Path(results_dir)
    rows = []
    timeseries_frames = []

    for csv_file in sorted(results_path.glob("D*_failure-*_seed-*.csv")):
        parsed = _parse_filename(csv_file.name)
        if parsed is None:
            continue
        config, seed = parsed

        logger.info("Analyzing %s", csv_file.name)
        df = pd.read_csv(csv_file)
        metrics = analyze_single_csv(
            df,
            injection_s=injection_s,
            total_duration_s=total_duration_s,
            window_s=window_s,
            threshold=threshold,
        )
        metrics["config"] = config
        metrics["seed"] = seed
        rows.append(metrics)

        # Time-series data for TikZ plots
        if timeseries_csv is not None:
            tdf = assign_elapsed_time(df, total_duration_s=total_duration_s)
            windows = compute_windowed_throughput(tdf, window_s=window_s)
            windows["config"] = config
            windows["seed"] = seed
            timeseries_frames.append(windows)

    summary = pd.DataFrame(rows)
    # Reorder columns
    col_order = [
        "config", "seed", "detection_time_s", "recovery_time_s",
        "degradation_depth", "failed_pipelines",
        "pre_throughput", "post_throughput",
        "pre_p50_latency", "post_p50_latency",
    ]
    summary = summary[[c for c in col_order if c in summary.columns]]
    summary.to_csv(output_csv, index=False)
    logger.info("Summary written to %s", output_csv)

    # Aggregate time-series: mean +/- std across seeds per config
    if timeseries_csv is not None and timeseries_frames:
        ts_all = pd.concat(timeseries_frames, ignore_index=True)
        ts_agg = (
            ts_all.groupby(["config", "window_start_s"])
            .agg(
                throughput_pps_mean=("throughput_pps", "mean"),
                throughput_pps_std=("throughput_pps", "std"),
                window_end_s=("window_end_s", "first"),
            )
            .reset_index()
        )
        ts_agg.to_csv(timeseries_csv, index=False)
        logger.info("Time-series written to %s", timeseries_csv)

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Post-hoc recovery time analysis for Phase D"
    )
    parser.add_argument(
        "--results-dir", type=str, required=True,
        help="Directory containing Phase D CSV files",
    )
    parser.add_argument(
        "--output-csv", type=str, required=True,
        help="Path for output summary CSV",
    )
    parser.add_argument(
        "--timeseries-csv", type=str, default=None,
        help="Optional path for time-series throughput data (TikZ)",
    )
    parser.add_argument(
        "--injection-s", type=float, default=300.0,
        help="Injection time in seconds from run start (default: 300)",
    )
    parser.add_argument(
        "--total-duration-s", type=float, default=None,
        help="Total run duration in seconds (inferred from data if omitted)",
    )
    parser.add_argument(
        "--window-s", type=float, default=5.0,
        help="Time window width in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.9,
        help="Recovery threshold as fraction of pre-failure throughput (default: 0.9)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    summary = analyze_phase_d_recovery(
        results_dir=args.results_dir,
        output_csv=args.output_csv,
        timeseries_csv=args.timeseries_csv,
        injection_s=args.injection_s,
        total_duration_s=args.total_duration_s,
        window_s=args.window_s,
        threshold=args.threshold,
    )

    print("\n=== Phase D Recovery Analysis ===\n")
    for config in sorted(summary["config"].unique()):
        cfg_data = summary[summary["config"] == config]
        print(f"Config {config} (n={len(cfg_data)} seeds):")
        for col in ["detection_time_s", "recovery_time_s", "degradation_depth",
                     "failed_pipelines"]:
            vals = cfg_data[col].dropna()
            if len(vals) > 0:
                print(f"  {col}: median={vals.median():.2f}, "
                      f"mean={vals.mean():.2f}, std={vals.std():.2f}")
        print()


if __name__ == "__main__":
    main()
