#!/usr/bin/env python3
"""Phase B: Slice-aware placement.

Runs 4 configurations on a single site with multiple network slices:
  B1 -- Neural Pub/Sub, 1 slice (no slice awareness baseline)
  B2 -- Neural Pub/Sub, 3 slices, no governance
  B3 -- Neural Pub/Sub, 3 slices + governance constraints
  B4 -- Neural Pub/Sub, 3 slices + governance + failure injection at t=15min

Per configuration:
  5 seeds x medium workload x 3-stage pipeline.
  B4 additionally injects a node failure at t=15min and measures adaptation time.

Outputs CSV results to results/phase_b/.

Usage:
    python scripts/run_phase_b.py [--dry-run] [--seeds 42,123,456,789,0]
    python scripts/run_phase_b.py --configs B3,B4 --seeds 42
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    PROJECT_ROOT,
    inject_compose_kill,
    phase_main,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_b"

DEFAULT_RATE = "medium"
DEFAULT_RATE_VALUE = 5.0
DEFAULT_COMPLEXITY = 3
COMPLEXITY_MIX = {"cqi_prediction": 0.5, "anomaly_detection": 0.5, "sensor_fusion": 0.0}

# Config definitions
CONFIGS = {
    "B1": {"num_slices": 1, "governance": False, "failure_injection": False},
    "B2": {"num_slices": 3, "governance": False, "failure_injection": False},
    "B3": {"num_slices": 3, "governance": True, "failure_injection": False},
    "B4": {"num_slices": 3, "governance": True, "failure_injection": True},
}


@dataclass
class RunConfig:
    """A single Phase B run configuration."""
    config_name: str
    seed: int
    num_slices: int = 1
    governance: bool = False
    failure_injection: bool = False
    failure_delay_s: int = 900
    warmup_s: int = 600
    measurement_s: int = 1800


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
) -> list[RunConfig]:
    """Build the run matrix (medium rate, 3-stage complexity only)."""
    runs = []
    for config_name, seed in itertools.product(configs, seeds):
        cfg = CONFIGS[config_name]
        runs.append(RunConfig(
            config_name=config_name,
            seed=seed,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        ))
    return runs


def _run(run: RunConfig, dry_run: bool) -> dict:
    run_id = (
        f"{run.config_name}_rate-{DEFAULT_RATE}_"
        f"stages-{DEFAULT_COMPLEXITY}_seed-{run.seed}"
    )
    total_duration = run.warmup_s + run.measurement_s

    logger.info(
        "Run: %s (rate=%.1f, stages=%d, seed=%d, slices=%d, "
        "governance=%s, failure=%s, duration=%ds)",
        run_id, DEFAULT_RATE_VALUE, DEFAULT_COMPLEXITY, run.seed,
        run.num_slices, run.governance, run.failure_injection, total_duration,
    )

    env = {
        "PLACEMENT_STRATEGY": "neural",
        "ARRIVAL_RATE": str(DEFAULT_RATE_VALUE),
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "PIPELINE_MIX_CQI": str(COMPLEXITY_MIX["cqi_prediction"]),
        "PIPELINE_MIX_ANOMALY": str(COMPLEXITY_MIX["anomaly_detection"]),
        "PIPELINE_MIX_FUSION": str(COMPLEXITY_MIX["sensor_fusion"]),
        "WARMUP_S": str(run.warmup_s),
        "NUM_SLICES": str(run.num_slices),
        "GOVERNANCE_ENABLED": str(run.governance).lower(),
    }

    if run.failure_injection:
        env["FAILURE_DELAY_S"] = str(run.failure_delay_s)

    failure_fn = None
    if run.failure_injection:
        failure_fn = partial(
            inject_compose_kill,
            project_name=f"npubsub-{run_id}",
            compose_file=COMPOSE_FILE,
            env=env,
            target="worker",
            delay_s=run.failure_delay_s,
            label="worker",
        )

    return run_single(
        run_id=run_id,
        env=env,
        results_dir=RESULTS_DIR,
        total_duration=total_duration,
        dry_run=dry_run,
        failure_fn=failure_fn,
    )


def main():
    phase_main(
        phase_name="Phase B",
        description="Phase B: Slice-aware placement",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
    )


if __name__ == "__main__":
    main()
