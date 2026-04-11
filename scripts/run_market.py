#!/usr/bin/env python3
"""Market: Market-based decentralized allocation + governance composition.

Tier 2 experiments on 4-domain O-RAN topology (4 VMs, 48 workers).

Allocation (270 runs): 6 strategies x 3 pipeline types x 3 loads x 5 seeds.
Governance  (60 runs):  4 scenarios x 3 pipeline types x 1 load  x 5 seeds.
Total: 330 runs (~80h on 4 VMs, measured wall clock).

Usage:
    python -m scripts.run_market --dry-run
    python -m scripts.run_market --topology distributed --resume
    python -m scripts.run_market --configs market-quad --pipelines cqi-chain --loads medium --seeds 99
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from pathlib import Path

from scripts._common import (
    DEFAULT_SEEDS,
    PROJECT_ROOT,
    phase_main,
)
from scripts.experiment_matrix import EXPERIMENTS

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "market"

# Run timing inherited from the experiment matrix SSoT.
_MARKET_META = EXPERIMENTS["market"]
DEFAULT_WARMUP_S = _MARKET_META["warmup_s"]
DEFAULT_MEASUREMENT_S = _MARKET_META["measurement_s"]

# ---------------------------------------------------------------------------
# Pipeline and load mappings
# ---------------------------------------------------------------------------

# Maps experiment-matrix slug (hyphenated) to internal template name (underscored).
PIPELINE_MAP: dict[str, str] = {
    "cqi-chain": "cqi_chain",
    "anomaly-sp": "anomaly_sp",
    "ran-entangled": "ran_entangled",
}

# Reverse: internal name to slug (for run_id formatting).
_SLUG_MAP: dict[str, str] = {v: k for k, v in PIPELINE_MAP.items()}

PIPELINE_SLUGS: list[str] = list(PIPELINE_MAP.keys())
PIPELINE_INTERNAL: list[str] = list(PIPELINE_MAP.values())

LOADS: dict[str, float] = {
    "low": 2.0,
    "medium": 5.0,
    "high": 10.0,
}

ALL_LOAD_LABELS: list[str] = list(LOADS.keys())
GOV_LOAD_LABELS: list[str] = ["medium"]

# ---------------------------------------------------------------------------
# Config definitions
# ---------------------------------------------------------------------------

MARKET_CONFIGS: dict[str, dict] = {
    "oracle-global": {
        "placement_mode": "neural",
        "governance_config": "all",
        "oracle_mode": True,
        "description": "Oracle: single broker on VM1, all 48 workers (upper bound)",
    },
    "market-quad": {
        "placement_mode": "market",
        "governance_config": "all",
        "description": "Market: 4 federated brokers, price-signal coordination",
    },
    "locality-only": {
        "placement_mode": "locality",
        "governance_config": "all",
        "description": "Heuristic: each domain handles own traffic only",
    },
    "latency-greedy": {
        "placement_mode": "latency",
        "governance_config": "all",
        "description": "Heuristic: lowest-latency worker (cross-domain OK)",
    },
    "spillover": {
        "placement_mode": "spillover",
        "governance_config": "all",
        "description": "Heuristic: local-first, overflow to other site",
    },
    "rr-global": {
        "placement_mode": "neural",
        "governance_config": "all",
        "oracle_mode": True,
        "broker_module": "src.broker.static_broker",
        "placement": "round_robin",
        "description": "Conventional centralized: single broker, round-robin, 48 workers",
    },
}

GOV_CONFIGS: dict[str, dict] = {
    "gov-none": {
        "placement_mode": "market",
        "governance_config": "none",
        "description": "Governance: neither site enforces (Scenario A)",
    },
    "gov-edge-only": {
        "placement_mode": "market",
        "governance_config": "edge-only",
        "description": "Governance: edge site enforces only (Scenario B)",
    },
    "gov-cloud-only": {
        "placement_mode": "market",
        "governance_config": "cloud-only",
        "description": "Governance: cloud site enforces only (Scenario C)",
    },
    "gov-both": {
        "placement_mode": "market",
        "governance_config": "all",
        "description": "Governance: both sites enforce (Scenario D)",
    },
}

CONFIGS: dict[str, dict] = {**MARKET_CONFIGS, **GOV_CONFIGS}


# ---------------------------------------------------------------------------
# RunConfig
# ---------------------------------------------------------------------------


@dataclass
class MarketRunConfig:
    """A single market/governance run configuration."""

    config_name: str
    pipeline_type: str  # internal name (e.g. "cqi_chain")
    load_label: str  # "low", "medium", "high"
    arrival_rate: float
    seed: int
    warmup_s: int = DEFAULT_WARMUP_S
    measurement_s: int = DEFAULT_MEASUREMENT_S

    @property
    def run_id(self) -> str:
        slug = _SLUG_MAP.get(self.pipeline_type, self.pipeline_type)
        return f"{self.config_name}_{slug}_{self.load_label}_seed-{self.seed}"


# ---------------------------------------------------------------------------
# Matrix builder
# ---------------------------------------------------------------------------


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    pipelines: list[str] | None = None,
    loads: list[str] | None = None,
    warmup_s: int | None = None,
    measurement_s: int | None = None,
) -> list[MarketRunConfig]:
    """Build the run matrix: configs x pipelines x loads x seeds.

    Market configs (in MARKET_CONFIGS) run at all load levels.
    Governance configs (in GOV_CONFIGS) run at medium only.
    """
    overrides: dict = {}
    if warmup_s is not None:
        overrides["warmup_s"] = warmup_s
    if measurement_s is not None:
        overrides["measurement_s"] = measurement_s

    pipe_internal = (
        [PIPELINE_MAP[p] for p in pipelines]
        if pipelines is not None
        else PIPELINE_INTERNAL
    )

    runs: list[MarketRunConfig] = []
    for config_name in configs:
        if config_name in GOV_CONFIGS:
            load_labels = loads if loads is not None else GOV_LOAD_LABELS
        else:
            load_labels = loads if loads is not None else ALL_LOAD_LABELS

        for ptype, load_label, seed in itertools.product(
            pipe_internal, load_labels, seeds
        ):
            runs.append(
                MarketRunConfig(
                    config_name=config_name,
                    pipeline_type=ptype,
                    load_label=load_label,
                    arrival_rate=LOADS[load_label],
                    seed=seed,
                    **overrides,
                )
            )
    return runs


# ---------------------------------------------------------------------------
# Run functions
# ---------------------------------------------------------------------------


def _run_distributed(run: MarketRunConfig, dry_run: bool) -> dict:
    """Execute a market run on the distributed 4-VM cluster."""
    from scripts import multi_vm_runner

    cfg = CONFIGS[run.config_name]

    result = multi_vm_runner.run_single(
        config=run.config_name,
        run_id=run.run_id,
        seed=run.seed,
        placement_mode=cfg["placement_mode"],
        governance_config=cfg["governance_config"],
        broker_module=cfg.get("broker_module"),
        placement=cfg.get("placement"),
        workload_env={
            "PIPELINE_TYPE": run.pipeline_type,
            "ARRIVAL_RATE": str(run.arrival_rate),
        },
        results_subdir="market",
        warmup_s=run.warmup_s,
        measurement_s=run.measurement_s,
        wan_emulation=True,
        oracle_mode=cfg.get("oracle_mode", False),
        dry_run=dry_run,
    )
    # Propagate failure from run_single (e.g. federation timeout)
    if result and result.get("status") == "failed":
        return result
    return {
        "run_id": run.run_id,
        "status": "completed" if not dry_run else "dry_run",
        "result_file": f"results/market/{run.run_id}",
    }


def _run(run: MarketRunConfig, dry_run: bool, **kwargs) -> dict:
    """Execute a single market run (dispatch local vs distributed)."""
    topology = kwargs.get("topology", "local")
    if topology == "distributed":
        return _run_distributed(run, dry_run)

    # Local mode: use Docker Compose on the current machine.
    # For now, only distributed mode is supported for market experiments
    # (requires 4 domains / 4 VMs).
    logger.warning(
        "Market experiments require 4-domain topology. "
        "Use --topology distributed for real runs. "
        "Local mode runs a single-domain approximation."
    )
    return _run_distributed(run, dry_run)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _extra_args(parser):
    """Add market-specific CLI arguments."""
    parser.add_argument(
        "--pipelines",
        type=str,
        default=None,
        help=(
            "Comma-separated pipeline slugs to run "
            f"(default: all; valid: {','.join(PIPELINE_SLUGS)})"
        ),
    )
    parser.add_argument(
        "--loads",
        type=str,
        default=None,
        help=(
            "Comma-separated load labels to run "
            f"(default: all for market, medium for gov; valid: {','.join(ALL_LOAD_LABELS)})"
        ),
    )


def _parse_extra(args):
    """Parse market-specific CLI arguments into build_run_matrix kwargs."""
    result = {}
    if args.pipelines:
        slugs = [s.strip() for s in args.pipelines.split(",")]
        for s in slugs:
            if s not in PIPELINE_MAP:
                raise SystemExit(
                    f"Unknown pipeline slug: {s!r}. Valid: {PIPELINE_SLUGS}"
                )
        result["pipelines"] = slugs
    if args.loads:
        labels = [s.strip() for s in args.loads.split(",")]
        for lbl in labels:
            if lbl not in LOADS:
                raise SystemExit(
                    f"Unknown load label: {lbl!r}. Valid: {ALL_LOAD_LABELS}"
                )
        result["loads"] = labels
    return result


def main() -> None:
    """Entry point for the market experiment runner."""
    phase_main(
        phase_name="Market",
        description="Market-based decentralized allocation + governance composition",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
        default_seeds=DEFAULT_SEEDS,
        extra_args_fn=_extra_args,
        parse_extra_fn=_parse_extra,
    )


if __name__ == "__main__":
    main()
