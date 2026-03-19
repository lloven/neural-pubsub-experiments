#!/usr/bin/env python3
"""Statistical analysis pipeline for Neural Pub/Sub experiment results.

Implements the manuscript's statistical methodology (Section 5):
- KS test with Holm-Bonferroni correction for 3 planned contrasts
- Wasserstein distance in milliseconds
- Vargha-Delaney A12 effect size
- Bootstrap 95% CIs for median and p95
- Wilcoxon signed-rank test for Phase D recovery times
- One-sample Wilcoxon for H4 (governance overhead)
- Output: LaTeX table fragments + JSON summary

Usage:
    python scripts/analyze_results.py --phase A --csv results/phase_a/A4_rate-medium_stages-3_seed-42.csv
    python scripts/analyze_results.py --phase A --results-dir results/phase_a/
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Vargha-Delaney A12 effect size
# ---------------------------------------------------------------------------


def vargha_delaney_a12(x: Sequence[float], y: Sequence[float]) -> float:
    """Compute the Vargha-Delaney A12 effect size statistic.

    A12 measures the probability that a randomly chosen observation from x
    is larger than a randomly chosen observation from y. Values:
        0.5 = no effect (identical distributions)
        1.0 = x always dominates y
        0.0 = y always dominates x

    Args:
        x: First sample (treatment group).
        y: Second sample (control group).

    Returns:
        A12 statistic in [0, 1].
    """
    m, n = len(x), len(y)
    if m == 0 or n == 0:
        return 0.5

    count = 0.0
    for xi in x:
        for yj in y:
            if xi > yj:
                count += 1.0
            elif xi == yj:
                count += 0.5
    return count / (m * n)


def a12_effect_label(a12: float) -> str:
    """Classify an A12 value using Vargha-Delaney thresholds.

    Thresholds (symmetric around 0.5):
        |A12 - 0.5| < 0.06  -> negligible
        |A12 - 0.5| < 0.14  -> small
        |A12 - 0.5| < 0.21  -> medium
        otherwise            -> large

    Args:
        a12: The A12 statistic.

    Returns:
        One of "negligible", "small", "medium", "large".
    """
    diff = abs(a12 - 0.5)
    if diff < 0.064:
        return "negligible"
    elif diff < 0.14:
        return "small"
    elif diff < 0.21:
        return "medium"
    else:
        return "large"


# ---------------------------------------------------------------------------
# Holm-Bonferroni correction
# ---------------------------------------------------------------------------


def holm_bonferroni(
    p_values: Sequence[float], alpha: float = 0.05
) -> list[tuple[float, bool]]:
    """Apply Holm-Bonferroni step-down correction to a list of p-values.

    Returns results in the ORIGINAL input order (not sorted order).

    Args:
        p_values: Raw p-values from independent tests.
        alpha: Family-wise error rate (default 0.05).

    Returns:
        List of (adjusted_p, is_significant) tuples in the same order
        as the input p_values.
    """
    n = len(p_values)
    # Create (original_index, p_value) pairs and sort by p-value
    indexed = sorted(enumerate(p_values), key=lambda x: x[1])

    # Apply step-down correction
    adjusted = [0.0] * n
    significant = [False] * n
    cumulative_max = 0.0

    for rank, (orig_idx, p) in enumerate(indexed):
        k = n - rank  # number of remaining hypotheses
        adj_p = min(p * k, 1.0)
        # Enforce monotonicity: adjusted p can never decrease as we move down
        cumulative_max = max(cumulative_max, adj_p)
        adjusted[orig_idx] = cumulative_max
        significant[orig_idx] = bool(cumulative_max < alpha)

    return [(adjusted[i], significant[i]) for i in range(n)]


# ---------------------------------------------------------------------------
# Bootstrap CI
# ---------------------------------------------------------------------------


def bootstrap_ci(
    data: np.ndarray,
    statistic: str = "median",
    confidence: float = 0.95,
    n_resamples: int = 10_000,
    seed: int = 42,
) -> tuple[float, float]:
    """Compute a bootstrap confidence interval for a given statistic.

    Uses scipy.stats.bootstrap with the BCa method.

    Args:
        data: 1-D array of observations.
        statistic: One of "median" or "p95".
        confidence: Confidence level (default 0.95).
        n_resamples: Number of bootstrap resamples (default 10,000).
        seed: Random seed for reproducibility.

    Returns:
        (lower_bound, upper_bound) as a tuple of floats.
    """
    if statistic == "median":
        stat_fn = np.median
    elif statistic == "p95":
        stat_fn = lambda x: float(np.percentile(x, 95))
    else:
        raise ValueError(f"Unknown statistic: {statistic}")

    result = stats.bootstrap(
        (data,),
        statistic=stat_fn,
        n_resamples=n_resamples,
        confidence_level=confidence,
        random_state=np.random.default_rng(seed),
        method="percentile",
    )
    return (float(result.confidence_interval.low),
            float(result.confidence_interval.high))


# ---------------------------------------------------------------------------
# Phase A analysis pipeline
# ---------------------------------------------------------------------------

# The 3 planned contrasts for Holm-Bonferroni correction
PLANNED_CONTRASTS = [
    ("S4", "S1"),
    ("S4", "S2"),
    ("S4", "S3"),
]


def _load_phase_csvs(csv_path: str) -> pd.DataFrame:
    """Load CSV(s) for Phase A analysis.

    If csv_path is a directory, load all CSVs and extract config_name from
    filenames (e.g., "A2_rate-medium_stages-3_seed-42.csv" -> "A2").
    If csv_path is a single file, extract config_name from the filename,
    or use an existing config_name column.

    Args:
        csv_path: Path to a CSV file or a directory containing CSVs.

    Returns:
        DataFrame with a config_name column.
    """
    path = Path(csv_path)
    if path.is_dir():
        frames = []
        for f in sorted(path.glob("*.csv")):
            if f.name.endswith("_summary.csv") or f.name.startswith("."):
                continue
            df = pd.read_csv(f)
            # Extract config name from filename prefix (e.g., "A2")
            config = f.stem.split("_")[0]
            df["config_name"] = config
            frames.append(df)
        if not frames:
            raise FileNotFoundError(f"No CSV files found in {path}")
        return pd.concat(frames, ignore_index=True)
    else:
        df = pd.read_csv(path)
        if "config_name" not in df.columns:
            config = path.stem.split("_")[0]
            df["config_name"] = config
        return df


def analyze_phase_a(csv_path: str) -> dict:
    """Run the full Phase A statistical analysis on a CSV file or directory.

    Accepts either a single CSV with a config_name column, a single CSV
    where the config is inferred from the filename, or a directory of
    per-config CSVs.

    Config names A1-A4 are mapped to S1-S4 for hypothesis labeling.

    Args:
        csv_path: Path to the CSV file or directory with experiment results.

    Returns:
        Dictionary with keys: contrasts, bootstrap_cis, descriptive_stats.
    """
    df = _load_phase_csvs(csv_path)

    # Map A1-A4 to S1-S4 if needed
    config_map = {"A1": "S1", "A2": "S2", "A3": "S3", "A4": "S4"}
    df["config_name"] = df["config_name"].map(
        lambda x: config_map.get(x, x)
    )

    # Filter to successful pipelines with valid latency
    df = df[df["success"].astype(str).str.lower() == "true"].copy()
    df["e2e_latency_ms"] = pd.to_numeric(df["e2e_latency_ms"], errors="coerce")
    df = df.dropna(subset=["e2e_latency_ms"])

    # Group by config
    configs = {}
    for name, group in df.groupby("config_name"):
        configs[name] = group["e2e_latency_ms"].values

    # --- Planned contrasts ---
    raw_p_values = []
    contrast_results = []

    for treatment, control in PLANNED_CONTRASTS:
        if treatment not in configs or control not in configs:
            logger.warning("Missing config %s or %s, skipping contrast", treatment, control)
            continue

        x = configs[treatment]
        y = configs[control]

        # KS test
        ks_stat, ks_p = stats.ks_2samp(x, y)
        raw_p_values.append(ks_p)

        # Wasserstein distance
        w_dist = stats.wasserstein_distance(x, y)

        # A12 effect size
        a12 = vargha_delaney_a12(x, y)

        contrast_results.append({
            "comparison": f"{treatment} vs {control}",
            "ks_statistic": float(ks_stat),
            "p_value": float(ks_p),
            "a12": float(a12),
            "a12_label": a12_effect_label(a12),
            "wasserstein_ms": float(w_dist),
        })

    # Holm-Bonferroni correction
    if raw_p_values:
        corrections = holm_bonferroni(raw_p_values)
        for i, (adj_p, sig) in enumerate(corrections):
            contrast_results[i]["adjusted_p"] = adj_p
            contrast_results[i]["holm_significant"] = sig

    # --- Bootstrap CIs per config ---
    bootstrap_results = []
    for config_name in sorted(configs.keys()):
        data = configs[config_name]
        med_ci = bootstrap_ci(data, statistic="median")
        p95_ci = bootstrap_ci(data, statistic="p95")
        bootstrap_results.append({
            "config": config_name,
            "median_ci": list(med_ci),
            "p95_ci": list(p95_ci),
        })

    # --- Descriptive statistics per config ---
    desc_stats = []
    for config_name in sorted(configs.keys()):
        data = configs[config_name]
        desc_stats.append({
            "config": config_name,
            "n": len(data),
            "mean": float(np.mean(data)),
            "std": float(np.std(data)),
            "median": float(np.median(data)),
            "p95": float(np.percentile(data, 95)),
            "p99": float(np.percentile(data, 99)),
        })

    return {
        "contrasts": contrast_results,
        "bootstrap_cis": bootstrap_results,
        "descriptive_stats": desc_stats,
    }


# ---------------------------------------------------------------------------
# Phase D analysis (Wilcoxon signed-rank for recovery times)
# ---------------------------------------------------------------------------


def analyze_phase_d(recovery_times: np.ndarray) -> dict:
    """Run Wilcoxon signed-rank test on Phase D recovery times.

    Tests whether recovery times are symmetrically distributed around zero
    (i.e., whether recovery is significantly different from no-effect).

    Args:
        recovery_times: Array of recovery time measurements in ms.

    Returns:
        Dictionary with wilcoxon_statistic, p_value, median, n.
    """
    if len(recovery_times) < 5:
        return {
            "wilcoxon_statistic": None,
            "p_value": None,
            "median": float(np.median(recovery_times)) if len(recovery_times) > 0 else None,
            "n": len(recovery_times),
        }

    w_stat, w_p = stats.wilcoxon(recovery_times)
    return {
        "wilcoxon_statistic": float(w_stat),
        "p_value": float(w_p),
        "median": float(np.median(recovery_times)),
        "n": len(recovery_times),
    }


def analyze_governance_overhead(
    overhead_ms: np.ndarray, hypothesized_median: float = 0.0
) -> dict:
    """One-sample Wilcoxon test for H4: governance overhead is bounded.

    Tests whether governance overhead differs significantly from zero.

    Args:
        overhead_ms: Array of governance overhead measurements in ms.
        hypothesized_median: The hypothesized median (default 0.0).

    Returns:
        Dictionary with wilcoxon_statistic, p_value, median.
    """
    if len(overhead_ms) < 5:
        return {
            "wilcoxon_statistic": None,
            "p_value": None,
            "median": float(np.median(overhead_ms)) if len(overhead_ms) > 0 else None,
            "n": len(overhead_ms),
        }

    shifted = overhead_ms - hypothesized_median
    w_stat, w_p = stats.wilcoxon(shifted)
    return {
        "wilcoxon_statistic": float(w_stat),
        "p_value": float(w_p),
        "median": float(np.median(overhead_ms)),
        "n": len(overhead_ms),
    }


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------


def to_latex_table(result: dict) -> str:
    """Convert analysis results to a LaTeX tabular fragment.

    Produces a table with columns: Comparison, KS stat, p (adj.), A12, Label, W-dist.

    Args:
        result: Output of analyze_phase_a.

    Returns:
        LaTeX string with \\begin{tabular}...\\end{tabular}.
    """
    lines = []
    lines.append(r"\begin{tabular}{lccccc}")
    lines.append(r"\toprule")
    lines.append(r"Comparison & KS stat. & $p$ (adj.) & A12 & Effect & $W$ (ms) \\")
    lines.append(r"\midrule")

    for c in result["contrasts"]:
        sig_marker = "*" if c.get("holm_significant", False) else ""
        lines.append(
            f"{c['comparison']} & "
            f"{c['ks_statistic']:.3f} & "
            f"{c.get('adjusted_p', c['p_value']):.4f}{sig_marker} & "
            f"{c['a12']:.3f} & "
            f"{c['a12_label']} & "
            f"{c['wasserstein_ms']:.1f} \\\\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    return "\n".join(lines)


class _NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy types."""

    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def to_json_summary(result: dict) -> str:
    """Convert analysis results to a JSON string.

    Args:
        result: Output of analyze_phase_a.

    Returns:
        Pretty-printed JSON string.
    """
    return json.dumps(result, indent=2, cls=_NumpyEncoder)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Statistical analysis of Neural Pub/Sub experiment results"
    )
    parser.add_argument(
        "--csv", type=str, required=True,
        help="Path to the CSV file with results",
    )
    parser.add_argument(
        "--output-dir", type=str, default=".",
        help="Directory for output files (default: current directory)",
    )
    parser.add_argument(
        "--phase", type=str, default="A", choices=["A", "D"],
        help="Analysis phase (default: A)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.phase == "A":
        result = analyze_phase_a(args.csv)

        # Write LaTeX table
        latex_path = output_dir / "contrast_table.tex"
        latex_path.write_text(to_latex_table(result))
        logger.info("LaTeX table written to %s", latex_path)

        # Write JSON summary
        json_path = output_dir / "analysis_summary.json"
        json_path.write_text(to_json_summary(result))
        logger.info("JSON summary written to %s", json_path)

        # Print summary to stdout
        print("\n=== Phase A Statistical Analysis ===\n")
        print("Descriptive Statistics:")
        for ds in result["descriptive_stats"]:
            print(
                f"  {ds['config']}: n={ds['n']}, "
                f"median={ds['median']:.1f}ms, "
                f"p95={ds['p95']:.1f}ms, "
                f"mean={ds['mean']:.1f}ms"
            )

        print("\nPlanned Contrasts (Holm-Bonferroni corrected):")
        for c in result["contrasts"]:
            sig = "***" if c.get("holm_significant") else "n.s."
            print(
                f"  {c['comparison']}: "
                f"KS={c['ks_statistic']:.3f}, "
                f"p(adj)={c.get('adjusted_p', c['p_value']):.4f} {sig}, "
                f"A12={c['a12']:.3f} ({c['a12_label']}), "
                f"W={c['wasserstein_ms']:.1f}ms"
            )

        print("\nBootstrap 95% CIs:")
        for ci in result["bootstrap_cis"]:
            print(
                f"  {ci['config']}: "
                f"median=[{ci['median_ci'][0]:.1f}, {ci['median_ci'][1]:.1f}], "
                f"p95=[{ci['p95_ci'][0]:.1f}, {ci['p95_ci'][1]:.1f}]"
            )

    else:
        logger.info("Phase D analysis: provide recovery times CSV.")


if __name__ == "__main__":
    main()
