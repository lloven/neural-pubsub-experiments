#!/usr/bin/env python3
"""Phase C: Cross-site federation.

Runs 4 configurations across two federated domains (Tokyo + Oulu):
  C1 -- Kafka at each site, static routing (cross-site baseline)
  C2 -- Neural Pub/Sub, federated (2 brokers)
  C3 -- C2 + governance (raw radio data stays in domain 1)
  C4 -- C3 + broker failure at one site

Per configuration:
  5 seeds x medium workload.
  Pipeline: CQI prediction where collect+preprocess must stay in domain 1
  (governance), predict can be in either domain.

Outputs CSV results to results/phase_c/.

Usage:
    python scripts/run_phase_c.py [--dry-run] [--seeds 42,123,456,789,0]
    python scripts/run_phase_c.py --configs C2,C3 --seeds 42
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

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_c"

# Config definitions
CONFIGS = {
    "C1": {
        "placement_strategy": "kafka",
        "federation": True,
        "governance": False,
        "broker_failure": False,
    },
    "C2": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": False,
        "broker_failure": False,
    },
    "C3": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": True,
        "broker_failure": False,
    },
    "C4": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": True,
        "broker_failure": True,
    },
}


@dataclass
class RunConfig:
    """A single Phase C run configuration."""
    config_name: str
    seed: int
    placement_strategy: str = "neural"
    federation: bool = True
    governance: bool = False
    broker_failure: bool = False
    failure_delay_s: int = 900
    warmup_s: int = 120
    measurement_s: int = 600


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
) -> list[RunConfig]:
    """Build the run matrix (medium rate, CQI prediction pipeline)."""
    runs = []
    for config_name, seed in itertools.product(configs, seeds):
        cfg = CONFIGS[config_name]
        runs.append(RunConfig(
            config_name=config_name,
            seed=seed,
            placement_strategy=cfg["placement_strategy"],
            federation=cfg["federation"],
            governance=cfg["governance"],
            broker_failure=cfg["broker_failure"],
        ))
    return runs


def _run(run: RunConfig, dry_run: bool) -> dict:
    run_id = f"{run.config_name}_rate-medium_seed-{run.seed}"
    total_duration = run.warmup_s + run.measurement_s

    logger.info(
        "Run: %s (rate=%.1f, seed=%d, strategy=%s, federation=%s, "
        "governance=%s, broker_failure=%s, duration=%ds)",
        run_id, 5.0, run.seed, run.placement_strategy,
        run.federation, run.governance, run.broker_failure, total_duration,
    )

    env = {
        "PLACEMENT_STRATEGY": run.placement_strategy,
        "ARRIVAL_RATE": "5.0",
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "PIPELINE_MIX_CQI": "1.0",
        "PIPELINE_MIX_ANOMALY": "0.0",
        "PIPELINE_MIX_FUSION": "0.0",
        "WARMUP_S": str(run.warmup_s),
        "FEDERATION_ENABLED": str(run.federation).lower(),
        "GOVERNANCE_ENABLED": str(run.governance).lower(),
        "NUM_DOMAINS": "2",
    }

    if run.broker_failure:
        env["FAILURE_DELAY_S"] = str(run.failure_delay_s)

    failure_fn = None
    if run.broker_failure:
        failure_fn = partial(
            inject_compose_kill,
            project_name=f"npubsub-{run_id}",
            compose_file=COMPOSE_FILE,
            env=env,
            target="broker-domain2",
            delay_s=run.failure_delay_s,
            label="broker",
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
        phase_name="Phase C",
        description="Phase C: Cross-site federation",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
    )


if __name__ == "__main__":
    main()
