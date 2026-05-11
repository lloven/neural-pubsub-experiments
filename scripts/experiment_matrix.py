"""Single source of truth for the experiment matrix structure.

Every phase's configs, seeds, transports, and expected run counts are
defined here.  Test files import from this module instead of hardcoding
matrix sizes.  When a config is added or removed, only this file changes.

Usage in tests::

    from scripts.experiment_matrix import expected_run_count, get_configs, get_seeds

    matrix = build_run_matrix(get_configs("slicing"), get_seeds("slicing"))
    assert len(matrix) == expected_run_count("slicing")

    # Legacy letter names still work via LEGACY_MAP:
    assert get_configs("B") == get_configs("slicing")
"""

from __future__ import annotations

from scripts._common import DEFAULT_SEEDS, EXTENDED_SEEDS, TRANSPORTS

# ---------------------------------------------------------------------------
# Run timing (single source of truth, consumed by run_market and run_ablation)
# ---------------------------------------------------------------------------
#
# Main-campaign run length: 4 min warmup + 10 min measurement = 14 min/run.
# Used by all market-class phases (market, governance, ablation). The longer
# warmup is needed because the 4-domain 48-worker topology with 8-stage
# pipelines takes ~3 min to reach steady state. The ablation phase MUST use
# the same values so its CR / latency / p95 distributions are directly
# comparable to the main campaign data (same statistical window, same
# warmup convergence, same sample basis).
#
# To rescale all market-class phases at once, edit these two constants.
MAIN_CAMPAIGN_WARMUP_S = 240
MAIN_CAMPAIGN_MEASUREMENT_S = 600

# ---------------------------------------------------------------------------
# Experiment definitions (new descriptive names are the primary keys)
# ---------------------------------------------------------------------------

EXPERIMENTS: dict[str, dict] = {
    "baseline": {
        "description": "Single-site baselines (dual-transport factorial)",
        "configs": ["rr", "random", "neural"],
        "seeds": DEFAULT_SEEDS,
        "transports": TRANSPORTS,
        "notes": (
            "Core factorial: configs x transports x seeds at medium rate, "
            "plus rate sensitivity arm (neural/http x all rates). "
            "Run count is computed by build_run_matrix, not a simple product."
        ),
    },
    "contention": {
        "description": "Resource contention",
        "configs": ["20pps", "50pps", "10pps-kill"],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        # Simple product: 3 configs x 5 seeds = 15
    },
    "slicing": {
        "description": "Slice-aware placement",
        "configs": ["flat", "neural", "rr", "gov", "gov-fail"],
        "seeds": DEFAULT_SEEDS,
        "transports": TRANSPORTS,
        # 5 configs x 2 transports x 5 seeds = 50
    },
    "federation": {
        "description": "Cross-site federation",
        "configs": ["static", "neural", "gov", "broker-kill", "net-part"],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        # 5 configs x 5 seeds = 25
    },
    "resilience": {
        "description": "Failure and adaptation",
        "configs": ["embb-kill", "urllc-kill", "funnel-wait", "funnel-proceed", "funnel-abort"],
        "seeds": EXTENDED_SEEDS,
        "transports": ["http"],
        # Default (S3 only): 5 configs x 10 seeds = 50
        # Strategy-all (S1+S2+S3): 5 configs x 3 strategies x seeds
        "strategies": ["S1", "S2", "S3"],
        "default_strategy": ["S3"],
    },
    "market": {
        "description": "Market-based allocation (4-domain O-RAN, 2+2 edge/cloud topology)",
        "configs": [
            # --- Allocation strategies (run at all 3 pipeline types x 3 loads) ---
            "oracle-global",       # centralised upper bound: single broker on VM1, all 48 workers
            "oracle-sharded",      # fair process-count comparator: 4 brokers, designated coordinator (F1 round-2)
            "rr-global",           # conventional centralised baseline: single broker, round-robin
            "market-quad",         # 4 federated brokers, price-signal coordination
            "locality-only",       # each domain handles own traffic, no cross-domain
            "latency-greedy",      # always pick lowest-latency worker (allows cross-domain)
            "spillover",           # local-first, overflow to other site when full
        ],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        "pipelines": ["cqi-chain", "anomaly-sp", "ran-entangled"],
        "loads": ["low", "medium", "high"],  # 2, 5, 10 pps
        "warmup_s": MAIN_CAMPAIGN_WARMUP_S,
        "measurement_s": MAIN_CAMPAIGN_MEASUREMENT_S,
        # 6 strategies x 3 pipelines x 3 loads x 5 seeds = 270 runs
        "notes": (
            "4-domain O-RAN topology: DU (VM1) + CU/near-RT-RIC (VM2) = edge site; "
            "non-RT-RIC (VM3) + SMO (VM4) = cloud site. 1 WAN link (50ms) between sites. "
            "48 workers total (12 per domain). Tests Paper 2 Walrasian convergence prediction."
        ),
    },
    "governance": {
        "description": "Governance composition (edge-vs-cloud enforcement, TEAC prediction)",
        "configs": [
            "gov-none",            # neither site enforces data sovereignty
            "gov-edge-only",       # edge site enforces, cloud does not
            "gov-cloud-only",      # cloud enforces, edge does not
            "gov-both",            # both sites enforce
        ],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        "pipelines": ["cqi-chain", "anomaly-sp", "ran-entangled"],
        "loads": ["medium"],  # 5 pps only (sufficient for composition test)
        "warmup_s": MAIN_CAMPAIGN_WARMUP_S,
        "measurement_s": MAIN_CAMPAIGN_MEASUREMENT_S,
        # 4 scenarios x 3 pipelines x 1 load x 5 seeds = 60 runs
        "notes": (
            "Tests TEAC supermodularity prediction: partial governance is worse than "
            "both-enforce or neither-enforce. Edge = DU+CU enforce raw data sovereignty; "
            "Cloud = non-RT-RIC+SMO enforce model output sovereignty."
        ),
    },
    "stress": {
        "description": "Combined H3+H6 contention + failure",
        "configs": [
            "10pps-rr-nofail", "10pps-neural-nofail",
            "10pps-rr-fail", "10pps-neural-fail",
            "20pps-rr-nofail", "20pps-neural-nofail",
            "20pps-rr-fail", "20pps-neural-fail",
            "50pps-rr-nofail", "50pps-neural-nofail",
            "50pps-rr-fail", "50pps-neural-fail",
        ],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        # 12 configs x 5 seeds = 60
    },
    "ablation": {
        "description": "Stress scenarios where rr-global breaks down",
        "configs": [
            "failure-5-12", "failure-8-12", "failure-10-12",
            "failure-5-24", "failure-8-24", "failure-10-24",
            "sat-5", "sat-10", "sat-15",
            "heterogeneous",
        ],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        "strategies": [
            "oracle-global", "oracle-sharded", "rr-global", "market-quad",
        ],
        "pipelines": ["cqi-chain", "anomaly-sp", "ran-entangled"],
        # Ablation MUST use the main campaign's run length (not a shorter
        # window) so that CR / latency / p95 are directly comparable across
        # the two phases. The two values below intentionally reference the
        # same constants as EXPERIMENTS["market"]; do not override.
        "warmup_s": MAIN_CAMPAIGN_WARMUP_S,
        "measurement_s": MAIN_CAMPAIGN_MEASUREMENT_S,
        # 10 scenarios x 3 strategies x 3 pipelines x 5 seeds = 450
        "notes": (
            "Tier 2c ablation. Tests H-RR-RECOVER (3×2 factorial: "
            "load 5/8/10 pps × kill 12/24 workers), H-RR-SATURATE "
            "(rate sweep 5/10/15 pps spanning the knee), H-RR-HETERO "
            "(edge 2x slower / cloud 1.5x faster at 5 pps). Rates were "
            "calibrated against the 2026-04-18 saturation sweep "
            "(results/calibration/SWEEP-RESULTS.md): oracle-global × "
            "cqi-chain × 48 workers saturates at ~13.8 pps effective "
            "throughput; the knee sits at 10-15 pps (CR 100%→80.7%, "
            "p99 2s→15s across the 10→15 pps step). Previous rates "
            "(50/100/150 pps and 100/150/200 pps) all sat in the "
            "complete-collapse regime. Uses docker-compose.vm-ablation.yaml "
            "and src.worker.ablation_worker so the main campaign is "
            "unchanged. Broker runs with MARKET_LOAD_AWARE=true and "
            "DYNAMIC_BIDDING=true env vars. Run length matches main "
            "market campaign for cross-phase comparability."
        ),
    },
}


# ---------------------------------------------------------------------------
# Legacy name mapping (old letter names -> new descriptive names)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Manuscript hypothesis -> experiment mapping
# ---------------------------------------------------------------------------

HYPOTHESIS_MAP: dict[str, dict] = {
    # Tier 1 (confirmed)
    "H-OVERHEAD": {"phase": "baseline", "configs": ["rr", "random", "neural"], "status": "confirmed"},
    "H-TRANSPORT": {"phase": "slicing", "test": "ANOVA placement x transport", "status": "confirmed"},
    "H-SLICE": {"phase": "slicing", "configs": ["flat", "neural"], "status": "confirmed"},
    "H-GOV": {"phase": "slicing", "configs": ["neural", "gov"], "status": "confirmed"},
    # Tier 2a — Market allocation
    "H-NEAR": {"phase": "market", "configs": ["oracle-global", "market-quad"]},
    "H-EDGE": {"phase": "market", "filter": "pipelines in [cqi-chain, anomaly-sp]"},
    "H-ENTANGLE": {"phase": "market", "filter": "pipeline = ran-entangled"},
    "H-OVERLOAD": {"phase": "market", "configs": ["oracle-global", "market-quad"], "test": "Delta_eff vs load"},
    "H-HEURISTIC": {"phase": "market", "configs": ["market-quad", "locality-only", "latency-greedy", "spillover"]},
    "H-ADAPT": {"phase": "market", "test": "load step 5->10 pps at t=5min"},
    # Tier 2b — Governance + resilience
    "H-COMPOSE": {"phase": "governance", "configs": ["gov-none", "gov-edge-only", "gov-cloud-only", "gov-both"]},
    "H-FEDERATION": {"phase": "federation", "configs": ["neural", "gov", "broker-kill"]},
    "H-RESILIENCE": {"phase": "resilience", "configs": ["embb-kill", "urllc-kill"]},
    # Tier 2c — Ablation: rr-global stress scenarios
    # For ablation phase, "configs" refers to the scenarios axis (matching
    # EXPERIMENTS["ablation"]["configs"]), and "strategies" lists the
    # strategies compared within each scenario.
    "H-RR-RECOVER": {
        "phase": "ablation",
        "configs": [
            "failure-5-12", "failure-8-12", "failure-10-12",
            "failure-5-24", "failure-8-24", "failure-10-24",
        ],
        "strategies": ["oracle-global", "rr-global", "market-quad"],
        "test": "3x2 factorial: load (5/8/10 pps) × kill ratio (12/24 workers); measure CR drop and latency divergence",
        "theory": "Information completeness: prices encode worker availability; effect grows with stress (load × kill interaction). Loads chosen so survivors span sub-saturation (5 pps, 12-kill) to severe over-saturation (10 pps, 24-kill).",
    },
    "H-RR-SATURATE": {
        "phase": "ablation",
        "configs": ["sat-5", "sat-10", "sat-15"],
        "strategies": ["oracle-global", "rr-global", "market-quad"],
        "test": "rate sweep 5/10/15 pps spans below / at / above the knee (calibrated at ~13.8 pps throughput)",
        "theory": "Walrasian admission control: prices clear excess demand at 15 pps; rr-global queues unboundedly while market-quad rejects via congestion pricing",
    },
    "H-RR-HETERO": {
        "phase": "ablation",
        "configs": ["heterogeneous"],
        "strategies": ["oracle-global", "rr-global", "market-quad"],
        "test": "edge 2x slower / cloud 1.5x faster, mean latency gap",
        "theory": "Walrasian price discovery: scarce workers become expensive",
    },
}


LEGACY_MAP: dict[str, str] = {
    "A": "baseline",
    "A6": "contention",
    "B": "slicing",
    "C": "federation",
    "D": "resilience",
    "E": "stress",
}


def resolve_phase(phase: str) -> str:
    """Translate a phase name to the canonical (new) name.

    Accepts both old letter names (A, B, ...) and new descriptive names
    (baseline, slicing, ...).  Returns the canonical new name.

    Raises:
        KeyError: If the phase is not recognized.
    """
    if phase in EXPERIMENTS:
        return phase
    if phase in LEGACY_MAP:
        return LEGACY_MAP[phase]
    raise KeyError(
        f"Unknown phase {phase!r}. "
        f"Valid names: {sorted(EXPERIMENTS)} or legacy: {sorted(LEGACY_MAP)}"
    )


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------


def get_configs(phase: str) -> list[str]:
    """Return the config names for the given phase.

    Args:
        phase: Phase identifier, accepts both old (A, B, ...) and new
               (baseline, slicing, ...) names.

    Returns:
        List of config name strings.

    Raises:
        KeyError: If the phase is not defined.
    """
    return list(EXPERIMENTS[resolve_phase(phase)]["configs"])


def get_seeds(phase: str) -> list[int]:
    """Return the seed list for the given phase.

    Args:
        phase: Phase identifier (old or new name).

    Returns:
        List of integer seeds.
    """
    return list(EXPERIMENTS[resolve_phase(phase)]["seeds"])


def get_transports(phase: str) -> list[str]:
    """Return the transport list for the given phase.

    Args:
        phase: Phase identifier (old or new name).

    Returns:
        List of transport strings.
    """
    return list(EXPERIMENTS[resolve_phase(phase)]["transports"])


def expected_run_count(
    phase: str,
    *,
    configs: list[str] | None = None,
    seeds: list[int] | None = None,
    transports: list[str] | None = None,
    strategies: list[str] | None = None,
) -> int:
    """Compute the expected number of runs for a phase (or subset).

    For most phases this is a simple Cartesian product.  The baseline phase
    is special (factorial + rate-sensitivity arm) and is not handled here;
    use the actual build_run_matrix function for baseline counts.

    Args:
        phase: Phase identifier (old or new name).
        configs: Override config list (default: all configs for the phase).
        seeds: Override seed list (default: phase default).
        transports: Override transport list (default: phase default).
        strategies: For resilience phase only, override strategies (default: ["S3"]).

    Returns:
        Integer run count.
    """
    canonical = resolve_phase(phase)
    exp = EXPERIMENTS[canonical]
    n_configs = len(configs) if configs is not None else len(exp["configs"])
    n_seeds = len(seeds) if seeds is not None else len(exp["seeds"])

    if canonical == "slicing":
        n_transports = len(transports) if transports is not None else len(exp["transports"])
        return n_configs * n_transports * n_seeds

    if canonical == "resilience":
        n_strategies = (
            len(strategies)
            if strategies is not None
            else len(exp.get("default_strategy", ["S3"]))
        )
        return n_configs * n_strategies * n_seeds

    if canonical in ("contention", "federation", "stress"):
        return n_configs * n_seeds

    if canonical in ("market", "governance"):
        n_pipelines = len(exp.get("pipelines", ["default"]))
        n_loads = len(exp.get("loads", ["medium"]))
        return n_configs * n_pipelines * n_loads * n_seeds

    # baseline: not a simple product; caller should use build_run_matrix
    raise ValueError(
        f"Phase {phase!r} (canonical: {canonical!r}) does not support simple "
        f"run-count calculation. Use the phase's build_run_matrix() function directly."
    )


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------


def print_summary() -> None:
    """Print a summary table of all experiments."""
    print(f"{'Phase':<14} {'Description':<45} {'Configs':>7}  {'Seeds':>5}  {'Transports'}")
    print("-" * 90)
    for name, exp in EXPERIMENTS.items():
        configs = exp["configs"]
        seeds = exp["seeds"]
        transports = exp["transports"]
        print(
            f"{name:<14} {exp['description']:<45} {len(configs):>7}  "
            f"{len(seeds):>5}  {', '.join(transports)}"
        )
    print()
    print("Legacy name mapping:")
    for old, new in sorted(LEGACY_MAP.items()):
        print(f"  {old:<4} -> {new}")


if __name__ == "__main__":
    print_summary()
