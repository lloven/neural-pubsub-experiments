#!/usr/bin/env python3
"""Generate all publication figures from Phase A-E result CSVs.

Produces figures for the Neural Pub/Sub paper (Elsevier DCN), Section 5:

1. Latency CDF (Phase A): overlay of A1-A4 per workload rate
2. Throughput vs arrival rate (Phase A): line plot per config
3. Routing accuracy table (Phase A): A4 F1 vs Neural Router single-node
4. Latency breakdown (Phase B): stacked bar per stage
5. Adaptation time histogram (Phase B4)
6. Cross-site latency comparison (Phase C): single-site vs federated
7. Bandwidth overhead (Phase C): summary vs forwarded publications
8. Recovery timeline (Phase D): per failure type
9. Scaling plots (Phase E): latency and throughput vs nodes/domains

Usage:
    python scripts/generate_figures.py --results-dir results/ --output-dir figs/
    python scripts/generate_figures.py --phase A --results-dir results/phase_a/
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Publication defaults
plt.rcParams.update({
    "figure.figsize": (7, 4),
    "figure.dpi": 150,
    "font.size": 10,
    "font.family": "serif",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "legend.framealpha": 0.8,
})

# Shared colour palette
COLOURS = {
    "A1": "#d62728",   # Kafka (red)
    "A2": "#ff7f0e",   # Static (orange)
    "A3": "#7f7f7f",   # Random (grey)
    "A4": "#1f77b4",   # Neural Pub/Sub (blue)
    "B1": "#1f77b4",
    "B2": "#ff7f0e",
    "B3": "#2ca02c",
    "B4": "#d62728",
    "C1": "#d62728",
    "C2": "#1f77b4",
    "C3": "#2ca02c",
    "C4": "#ff7f0e",
}

MARKERS = {
    "A1": "^", "A2": "s", "A3": "x", "A4": "o",
    "B1": "o", "B2": "s", "B3": "D", "B4": "v",
    "C1": "^", "C2": "o", "C3": "D", "C4": "v",
}


def load_phase_results(phase_dir: Path) -> pd.DataFrame:
    """Load and concatenate all CSV files from a phase results directory."""
    frames = []
    for csv_file in sorted(phase_dir.glob("*.csv")):
        if csv_file.name.endswith("_summary.csv"):
            continue
        try:
            df = pd.read_csv(csv_file)
            # Extract config name from filename (e.g. "A4_rate-medium_stages-3_seed-42.csv")
            parts = csv_file.stem.split("_")
            if parts and parts[0] in COLOURS:
                df["config_name"] = parts[0]
            frames.append(df)
        except Exception as e:
            logger.warning("Failed to load %s: %s", csv_file, e)
    if not frames:
        raise FileNotFoundError(f"No result CSVs in {phase_dir}")
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Phase A figures
# ---------------------------------------------------------------------------

def fig_latency_cdf(df: pd.DataFrame, output: Path, rate: str = "medium"):
    """CDF of end-to-end latency, configs A1-A4 overlaid."""
    fig, ax = plt.subplots()

    subset = df[df["rate_label"] == rate] if "rate_label" in df.columns else df

    for config in sorted(subset["config_name"].unique()):
        data = subset[subset["config_name"] == config]
        if "e2e_latency_ms" not in data.columns:
            continue
        latencies = sorted(data["e2e_latency_ms"].dropna().values)
        if not latencies:
            continue
        cdf = np.arange(1, len(latencies) + 1) / len(latencies)
        colour = COLOURS.get(config, None)
        ax.plot(latencies, cdf, label=config, color=colour)

    ax.set_xlabel("End-to-End Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"Latency Distribution ({rate} rate)")
    ax.legend()
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


def fig_throughput_vs_rate(df: pd.DataFrame, output: Path):
    """Throughput vs arrival rate for configs A1-A4."""
    fig, ax = plt.subplots()

    if "arrival_rate" not in df.columns or "throughput_per_sec" not in df.columns:
        logger.warning("Skipping throughput plot: missing columns")
        return

    for config in sorted(df["config_name"].unique()):
        subset = df[df["config_name"] == config]
        grouped = subset.groupby("arrival_rate")["throughput_per_sec"].agg(
            ["mean", "std"]
        ).reset_index()
        colour = COLOURS.get(config, None)
        marker = MARKERS.get(config, "o")
        ax.errorbar(
            grouped["arrival_rate"], grouped["mean"], yerr=grouped["std"],
            label=config, color=colour, marker=marker, capsize=3,
        )

    ax.set_xlabel("Arrival Rate (req/s)")
    ax.set_ylabel("Throughput (pipelines/s)")
    ax.set_title("Throughput vs Workload Intensity")
    ax.legend()
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Phase B figures
# ---------------------------------------------------------------------------

def fig_latency_breakdown(df: pd.DataFrame, output: Path):
    """Stacked bar chart: latency breakdown by stage (compute + network)."""
    fig, ax = plt.subplots()

    configs = sorted(df["config_name"].unique())
    stage_cols = [c for c in df.columns if c.startswith("stage_") and c.endswith("_ms")]

    if not stage_cols:
        logger.warning("Skipping latency breakdown: no stage columns")
        return

    x = np.arange(len(configs))
    bottom = np.zeros(len(configs))
    width = 0.6

    for col in stage_cols:
        stage_name = col.replace("stage_", "").replace("_ms", "")
        means = [df[df["config_name"] == c][col].mean() for c in configs]
        ax.bar(x, means, width, bottom=bottom, label=stage_name)
        bottom += np.array(means)

    ax.set_xlabel("Configuration")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency Breakdown by Stage")
    ax.set_xticks(x)
    ax.set_xticklabels(configs)
    ax.legend(loc="upper left", fontsize=8)
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Phase C figures
# ---------------------------------------------------------------------------

def fig_cross_site_latency(df: pd.DataFrame, output: Path):
    """Cross-site latency comparison: single-site (C1) vs federated (C2-C4)."""
    fig, ax = plt.subplots()

    if "e2e_latency_ms" not in df.columns:
        logger.warning("Skipping cross-site latency plot: missing e2e_latency_ms")
        return

    configs = sorted(df["config_name"].unique())
    data = []
    labels = []
    for config in configs:
        latencies = df[df["config_name"] == config]["e2e_latency_ms"].dropna().values
        if len(latencies) > 0:
            data.append(latencies)
            labels.append(config)

    if not data:
        logger.warning("Skipping cross-site latency plot: no data")
        return

    ax.boxplot(data, labels=labels)
    ax.set_xlabel("Configuration")
    ax.set_ylabel("End-to-End Latency (ms)")
    ax.set_title("Cross-Site Latency: Single-Site vs Federated")
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


def fig_federation_bandwidth(df: pd.DataFrame, output: Path):
    """Federation bandwidth overhead: summary propagation vs forwarded publications."""
    fig, ax = plt.subplots()

    bandwidth_col = "federation_bytes_sent"
    if bandwidth_col not in df.columns:
        logger.warning("Skipping bandwidth plot: missing %s column", bandwidth_col)
        return

    configs = sorted(df["config_name"].unique())
    means = []
    stds = []
    for config in configs:
        subset = df[df["config_name"] == config][bandwidth_col].dropna()
        if len(subset) > 0:
            means.append(subset.mean() / 1024)  # Convert to KB
            stds.append(subset.std() / 1024)
        else:
            means.append(0)
            stds.append(0)

    x = np.arange(len(configs))
    ax.bar(x, means, yerr=stds, capsize=3,
           color=[COLOURS.get(c, "#333") for c in configs])
    ax.set_xlabel("Configuration")
    ax.set_ylabel("Federation Bandwidth (KB)")
    ax.set_title("Federation Bandwidth Overhead")
    ax.set_xticks(x)
    ax.set_xticklabels(configs)
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Phase D figures
# ---------------------------------------------------------------------------

def fig_recovery_timeline(df: pd.DataFrame, output: Path):
    """Recovery time distribution per failure type."""
    fig, ax = plt.subplots()

    if "failure_type" not in df.columns or "recovery_time_ms" not in df.columns:
        logger.warning("Skipping recovery plot: missing columns")
        return

    failure_types = sorted(df["failure_type"].unique())
    data = [
        df[df["failure_type"] == ft]["recovery_time_ms"].dropna().values
        for ft in failure_types
    ]

    ax.boxplot(data, labels=failure_types)
    ax.set_xlabel("Failure Type")
    ax.set_ylabel("Recovery Time (ms)")
    ax.set_title("Recovery Time by Failure Type")
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Phase E figures
# ---------------------------------------------------------------------------

def fig_scaling(df: pd.DataFrame, output: Path, y_col: str = "e2e_latency_ms"):
    """Latency/throughput vs number of nodes/domains."""
    fig, ax = plt.subplots()

    x_col = "n_nodes" if "n_nodes" in df.columns else "n_domains"
    if x_col not in df.columns or y_col not in df.columns:
        logger.warning("Skipping scaling plot: missing columns")
        return

    topologies = sorted(df["topology"].unique()) if "topology" in df.columns else ["default"]

    for topo in topologies:
        subset = df[df["topology"] == topo] if "topology" in df.columns else df
        grouped = subset.groupby(x_col)[y_col].agg(["mean", "std"]).reset_index()
        ax.errorbar(
            grouped[x_col], grouped["mean"], yerr=grouped["std"],
            label=topo, marker="o", capsize=3,
        )

    ax.set_xlabel(x_col.replace("_", " ").title())
    ax.set_ylabel(y_col.replace("_", " ").title())
    ax.set_title(f"Scaling: {y_col.replace('_', ' ').title()} vs {x_col.replace('_', ' ').title()}")
    ax.legend()
    plt.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    logger.info("Saved: %s", output)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

PHASE_GENERATORS = {
    "A": [
        ("latency_cdf_low.pdf", lambda df, o: fig_latency_cdf(df, o, "low")),
        ("latency_cdf_medium.pdf", lambda df, o: fig_latency_cdf(df, o, "medium")),
        ("latency_cdf_high.pdf", lambda df, o: fig_latency_cdf(df, o, "high")),
        ("throughput_vs_rate.pdf", fig_throughput_vs_rate),
    ],
    "B": [
        ("latency_breakdown.pdf", fig_latency_breakdown),
    ],
    "C": [
        ("cross_site_latency.pdf", fig_cross_site_latency),
        ("federation_bandwidth.pdf", fig_federation_bandwidth),
    ],
    "D": [
        ("recovery_timeline.pdf", fig_recovery_timeline),
    ],
    "E": [
        ("scaling_latency.pdf", lambda df, o: fig_scaling(df, o, "e2e_latency_ms")),
        ("scaling_throughput.pdf", lambda df, o: fig_scaling(df, o, "throughput_per_sec")),
    ],
}


def main():
    parser = argparse.ArgumentParser(description="Generate Neural Pub/Sub paper figures")
    parser.add_argument(
        "--results-dir", type=str, default="results",
        help="Root results directory (default: results/)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="figs",
        help="Output directory for figures (default: figs/)",
    )
    parser.add_argument(
        "--phase", type=str, default=None,
        help="Generate figures for a specific phase only (A, B, C, D, E)",
    )
    parser.add_argument(
        "--format", choices=["pdf", "png", "svg"], default="pdf",
        help="Figure format (default: pdf)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    results_root = Path(args.results_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    phases = [args.phase.upper()] if args.phase else list(PHASE_GENERATORS.keys())

    for phase in phases:
        phase_dir = results_root / f"phase_{phase.lower()}"
        if not phase_dir.exists():
            logger.info("Skipping Phase %s: %s not found", phase, phase_dir)
            continue

        try:
            df = load_phase_results(phase_dir)
        except FileNotFoundError:
            logger.info("Skipping Phase %s: no CSV results", phase)
            continue

        generators = PHASE_GENERATORS.get(phase, [])
        for filename_template, gen_func in generators:
            filename = filename_template.replace(".pdf", f".{args.format}")
            output_path = output_dir / f"phase_{phase.lower()}_{filename}"
            try:
                gen_func(df, output_path)
            except Exception as e:
                logger.error("Failed to generate %s: %s", output_path, e)

    logger.info("Figure generation complete. Output: %s", output_dir)


if __name__ == "__main__":
    main()
