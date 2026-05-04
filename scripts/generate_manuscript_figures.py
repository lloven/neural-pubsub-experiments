#!/usr/bin/env python3
"""Generate the four key §5 figures from the campaign data.

Reads CSVs from results/<results-dir>/ and emits PDFs into the configured
manuscript figure directory.

Override the defaults via environment variables:
    NPUBSUB_RESULTS_DIR=results/remote-fetch-20260501  (campaign data dir, relative to repo root)
    NPUBSUB_FIG_DIR=fig                                 (output dir, relative to repo root)
"""
from __future__ import annotations

import csv
import os
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS = REPO_ROOT / os.environ.get("NPUBSUB_RESULTS_DIR", "results/remote-fetch-20260501")
FIG_DIR = Path(os.environ.get("NPUBSUB_FIG_DIR", REPO_ROOT / "fig"))

LOAD_TO_PPS = {"low": 2, "medium": 5, "high": 10}
PIPELINE_LABEL = {
    "cqi-chain": "CQI chain (tree)",
    "anomaly-sp": "Anomaly (SP)",
    "ran-entangled": "RAN suite (ent.)",
}


def load_market(strategies):
    """Per (strategy, pipeline, load): list of seed-level mean latencies."""
    out = defaultdict(list)
    for f in sorted((RESULTS / "market").glob("*.csv")):
        parts = f.stem.split("_")
        if len(parts) < 4:
            continue
        seed = parts[-1]
        load = parts[-2]
        pipe = parts[-3]
        strat = "_".join(parts[:-3])
        if strat not in strategies:
            continue
        if load not in LOAD_TO_PPS:
            continue
        lats = []
        with f.open() as fp:
            r = csv.DictReader(fp)
            for row in r:
                if row.get("warmup", "False") == "True":
                    continue
                if row.get("success") == "True":
                    try:
                        lats.append(float(row["e2e_latency_ms"]))
                    except Exception:
                        pass
        if lats:
            out[(strat, pipe, load)].append(statistics.mean(lats))
    return out


def load_governance():
    """Per (governance-scenario, pipeline): list of seed-level mean latencies."""
    out = defaultdict(list)
    for f in sorted((RESULTS / "market").glob("gov-*.csv")):
        parts = f.stem.split("_")
        seed = parts[-1]
        load = parts[-2]
        pipe = parts[-3]
        scenario = "_".join(parts[:-3])
        lats = []
        with f.open() as fp:
            r = csv.DictReader(fp)
            for row in r:
                if row.get("warmup", "False") == "True":
                    continue
                if row.get("success") == "True":
                    try:
                        lats.append(float(row["e2e_latency_ms"]))
                    except Exception:
                        pass
        if lats:
            out[(scenario, pipe)].append(statistics.mean(lats))
    return out


def load_ablation_rr():
    """Per scenario name: (CR%, mean latency) tuple aggregated over cells."""
    out = {}
    for scenario in [
        "sat-5_rr-global_cqi-chain",
        "sat-10_rr-global_cqi-chain",
        "sat-15_rr-global_cqi-chain",
        "sat-5_market-quad_cqi-chain",
        "sat-10_market-quad_cqi-chain",
        "sat-15_market-quad_cqi-chain",
        "sat-5_oracle-global_cqi-chain",
        "sat-10_oracle-global_cqi-chain",
        "sat-15_oracle-global_cqi-chain",
    ]:
        n_total = n_succ = 0
        lats = []
        for f in sorted((RESULTS / "ablation").glob(f"{scenario}_*.csv")):
            with f.open() as fp:
                r = csv.DictReader(fp)
                for row in r:
                    if row.get("warmup", "False") == "True":
                        continue
                    n_total += 1
                    if row.get("success") == "True":
                        n_succ += 1
                        try:
                            lats.append(float(row["e2e_latency_ms"]))
                        except Exception:
                            pass
        cr = 100 * n_succ / n_total if n_total else 0
        out[scenario] = (cr, statistics.mean(lats) if lats else 0)
    return out


def load_latency_distributions(strategies, pipe, load):
    """Per strategy: full list of pipeline latencies (across all cells)."""
    out = defaultdict(list)
    for f in sorted((RESULTS / "market").glob(f"*_{pipe}_{load}_*.csv")):
        parts = f.stem.split("_")
        strat = "_".join(parts[:-3])
        if strat not in strategies:
            continue
        with f.open() as fp:
            r = csv.DictReader(fp)
            for row in r:
                if row.get("warmup", "False") == "True":
                    continue
                if row.get("success") == "True":
                    try:
                        out[strat].append(float(row["e2e_latency_ms"]))
                    except Exception:
                        pass
    return out


# ---------------------------------------------------------------------------
# Figure 1: Near-optimality bar chart (oracle vs market across 9 configs)
# ---------------------------------------------------------------------------
def fig_near_optimality(out_path):
    data = load_market(["oracle-global", "market-quad"])
    pipes = ["cqi-chain", "anomaly-sp", "ran-entangled"]
    loads = ["low", "medium", "high"]

    fig, ax = plt.subplots(figsize=(6.5, 3.0))
    x = np.arange(len(pipes) * len(loads))
    oracle_means = []
    market_means = []
    labels = []
    for pipe in pipes:
        for load in loads:
            ovals = data.get(("oracle-global", pipe, load), [])
            mvals = data.get(("market-quad", pipe, load), [])
            oracle_means.append(np.mean(ovals) if ovals else 0)
            market_means.append(np.mean(mvals) if mvals else 0)
            labels.append(f"{LOAD_TO_PPS[load]}\\,pps")

    width = 0.4
    ax.bar(x - width / 2, oracle_means, width, label="Oracle (centralised)", color="#aa4444")
    ax.bar(x + width / 2, market_means, width, label="Market (4 brokers)", color="#4477aa")

    # Section labels under x axis
    section_centres = [1, 4, 7]
    for i, pipe in enumerate(pipes):
        ax.text(section_centres[i], -180, PIPELINE_LABEL[pipe],
                ha="center", va="top", fontsize=9, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Mean end-to-end latency (ms)")
    ax.set_xlabel("")
    ax.legend(loc="upper right", frameon=False, fontsize=8)
    # Section dividers
    ax.axvline(x=2.5, color="grey", lw=0.5, ls=":")
    ax.axvline(x=5.5, color="grey", lw=0.5, ls=":")
    ax.set_ylim(0, max(oracle_means) * 1.15)
    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------------------------------------------------------------------------
# Figure 2: Round-robin saturation collapse + market+oracle for comparison
# ---------------------------------------------------------------------------
def fig_rr_saturation(out_path):
    data = load_ablation_rr()
    rates = [5, 10, 15]
    rr_cr = [data[f"sat-{r}_rr-global_cqi-chain"][0] for r in rates]
    market_cr = [data[f"sat-{r}_market-quad_cqi-chain"][0] for r in rates]
    oracle_cr = [data[f"sat-{r}_oracle-global_cqi-chain"][0] for r in rates]

    fig, ax = plt.subplots(figsize=(5.0, 3.0))
    ax.plot(rates, oracle_cr, "o-", color="#aa4444", label="Oracle (centralised)")
    ax.plot(rates, market_cr, "s-", color="#4477aa", label="Market (4 brokers)")
    ax.plot(rates, rr_cr, "v-", color="#dd8855", label="Round-robin (centralised)")

    # Annotate the calibrated knee
    ax.axvspan(13.0, 14.5, alpha=0.15, color="orange", label="Calibrated knee ${\\sim}13.8$\\,pps")

    ax.set_xlabel("Arrival rate (pps)")
    ax.set_ylabel("Pipeline completion rate (%)")
    ax.set_xticks(rates)
    ax.set_ylim(-3, 105)
    ax.legend(loc="lower left", frameon=False, fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------------------------------------------------------------------------
# Figure 3: Heuristic parity (latency CDF for market vs 3 heuristics)
# ---------------------------------------------------------------------------
def fig_heuristic_parity(out_path):
    strats = ["market-quad", "locality-only", "latency-greedy", "spillover"]
    # Use ran-entangled medium load — the spec's "interesting" cell for H-HEURISTIC
    dists = load_latency_distributions(strats, "ran-entangled", "medium")

    fig, ax = plt.subplots(figsize=(5.0, 3.0))
    colors = {"market-quad": "#4477aa", "locality-only": "#669944",
              "latency-greedy": "#cc8833", "spillover": "#884488"}
    label_map = {
        "market-quad": "Market (4 brokers)",
        "locality-only": "Locality-only",
        "latency-greedy": "Latency-greedy",
        "spillover": "Spillover",
    }
    for s in strats:
        v = dists.get(s, [])
        if not v:
            continue
        v = np.sort(np.array(v))
        cdf = np.arange(1, len(v) + 1) / len(v)
        ax.plot(v, cdf, color=colors[s], label=label_map[s], lw=1.5)

    ax.set_xlabel("End-to-end latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title("RAN Intelligence Suite (entangled), 5\\,pps", fontsize=10)
    ax.legend(loc="lower right", frameon=False, fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(900, 1100)
    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------------------------------------------------------------------------
# Figure 4: Governance grid (4 scenarios × 3 pipelines)
# ---------------------------------------------------------------------------
def fig_governance_grid(out_path):
    data = load_governance()
    pipes = ["cqi-chain", "anomaly-sp", "ran-entangled"]
    scenarios = ["gov-none", "gov-edge-only", "gov-cloud-only", "gov-both"]
    scenario_label = {
        "gov-none": "Neither",
        "gov-edge-only": "Edge only",
        "gov-cloud-only": "Cloud only",
        "gov-both": "Both",
    }

    fig, axes = plt.subplots(1, 3, figsize=(7.5, 2.6), sharey=False)
    for ax, pipe in zip(axes, pipes):
        means = []
        errs = []
        for sc in scenarios:
            v = data.get((sc, pipe), [])
            if v:
                means.append(np.mean(v))
                errs.append(np.std(v, ddof=1) if len(v) > 1 else 0)
            else:
                means.append(0)
                errs.append(0)
        x = np.arange(len(scenarios))
        ax.bar(x, means, yerr=errs, capsize=3, color=["#bbbbbb", "#cc9966", "#669999", "#5577aa"])
        ax.set_xticks(x)
        ax.set_xticklabels([scenario_label[s] for s in scenarios], rotation=30, ha="right", fontsize=8)
        ax.set_title(PIPELINE_LABEL[pipe], fontsize=9)
        # Tight ylim to show the absence of effect
        if means and any(m > 0 for m in means):
            mid = sum(means) / len(means)
            ax.set_ylim(mid - 30, mid + 30)
        ax.set_ylabel("Mean latency (ms)" if pipe == "cqi-chain" else "")
        ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def main():
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Reading from: {RESULTS}")
    print(f"Writing to:   {FIG_DIR}")
    fig_near_optimality(FIG_DIR / "fig5-near-optimality.pdf")
    fig_rr_saturation(FIG_DIR / "fig6-rr-collapse.pdf")
    fig_heuristic_parity(FIG_DIR / "fig7-heuristic-parity.pdf")
    fig_governance_grid(FIG_DIR / "fig8-governance-grid.pdf")
    print("done.")


if __name__ == "__main__":
    main()
