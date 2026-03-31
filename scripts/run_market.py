#!/usr/bin/env python3
"""Market: Market-based decentralized allocation experiments.

Runs 9 configurations testing market-based allocation vs oracle/heuristic
baselines, plus 4 governance composition scenarios from TEAC Section 4:

  Allocation baselines:
    oracle-single   -- Single broker, full global visibility (upper bound)
    market-dual     -- Two brokers, price-signal coordination (test condition)
    locality-only   -- Each domain handles own traffic only
    latency-greedy  -- Lowest-latency worker (including remote)
    spillover       -- Local-first, overflow when full

  Governance composition (TEAC prediction: supermodular interaction):
    gov-neither     -- Neither domain enforces governance
    gov-d1-only     -- Domain 1 enforces, Domain 2 doesn't
    gov-d2-only     -- Domain 2 enforces, Domain 1 doesn't
    gov-both        -- Both domains enforce governance

Per configuration:
  5 seeds x medium workload x 3-stage pipeline.

Outputs CSV results to results/market/.

Usage:
    python -m scripts.run_market [--dry-run] [--seeds 42,123,456,789,0]
    python -m scripts.run_market --configs market-dual,gov-both --seeds 42
"""

from __future__ import annotations

import logging
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    COMPOSE_GOVERNANCE,
    DEFAULT_MEASUREMENT_S,
    DEFAULT_WARMUP_S,
    PROJECT_ROOT,
    phase_main,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "market"

COMPOSE_MARKET = PROJECT_ROOT / "docker-compose.market.yaml"
COMPOSE_GOV_D1 = PROJECT_ROOT / "docker-compose.gov-d1-only.yaml"
COMPOSE_GOV_D2 = PROJECT_ROOT / "docker-compose.gov-d2-only.yaml"

DEFAULT_RATE = "medium"
DEFAULT_RATE_VALUE = 5.0
DEFAULT_COMPLEXITY = 3

# Config definitions: each maps to compose files + env overrides
CONFIGS = {
    # --- Allocation baselines ---
    "oracle-single": {
        "compose_files": [COMPOSE_FILE],  # Single broker, full visibility
        "placement_mode": "neural",  # S3 = the oracle upper bound
        "governance": False,
        "description": "Oracle: single broker, neural placement (upper bound)",
    },
    "market-dual": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET],
        "placement_mode": "market",
        "governance": False,
        "description": "Market: two brokers, price-signal coordination",
    },
    "locality-only": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET],
        "placement_mode": "locality",
        "governance": False,
        "description": "Heuristic: local-only, no cross-domain placement",
    },
    "latency-greedy": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET],
        "placement_mode": "latency",
        "governance": False,
        "description": "Heuristic: lowest-latency worker (including remote)",
    },
    "spillover": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET],
        "placement_mode": "spillover",
        "governance": False,
        "description": "Heuristic: local-first, overflow to remote",
    },
    # --- Governance composition (TEAC Section 4) ---
    "gov-neither": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET],
        "placement_mode": "market",
        "governance": False,
        "description": "Governance: neither domain enforces (Scenario A)",
    },
    "gov-d1-only": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET, COMPOSE_GOV_D1],
        "placement_mode": "market",
        "governance": "d1",
        "description": "Governance: D1 enforces only (Scenario B)",
    },
    "gov-d2-only": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET, COMPOSE_GOV_D2],
        "placement_mode": "market",
        "governance": "d2",
        "description": "Governance: D2 enforces only (Scenario C)",
    },
    "gov-both": {
        "compose_files": [COMPOSE_FILE, COMPOSE_MARKET, COMPOSE_GOVERNANCE],
        "placement_mode": "market",
        "governance": True,
        "description": "Governance: both enforce (Scenario D)",
    },
}


def build_run(config_name: str, seed: int) -> dict:
    """Build a single run specification."""
    cfg = CONFIGS[config_name]
    run_id = f"{config_name}_seed-{seed}"

    env = {
        "SEED": str(seed),
        "ARRIVAL_RATE": str(DEFAULT_RATE_VALUE),
        "DURATION_S": str(DEFAULT_WARMUP_S + DEFAULT_MEASUREMENT_S),
        "WARMUP_S": str(DEFAULT_WARMUP_S),
        "PLACEMENT_MODE": cfg["placement_mode"],
    }

    return {
        "run_id": run_id,
        "config": config_name,
        "seed": seed,
        "compose_files": [str(f) for f in cfg["compose_files"]],
        "env": env,
        "result_file": str(RESULTS_DIR / f"{run_id}.csv"),
        "description": cfg["description"],
    }


def main() -> None:
    """Entry point for the market experiment runner."""
    phase_main(
        phase_name="market",
        description="Market-based decentralized allocation",
        configs=CONFIGS,
        build_run_fn=build_run,
        results_dir=RESULTS_DIR,
        default_configs=list(CONFIGS.keys()),
    )


if __name__ == "__main__":
    main()
