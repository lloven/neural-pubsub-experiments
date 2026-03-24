#!/usr/bin/env python3
"""Slicing: Slice-aware placement.

Runs 6 configurations on a single site with multiple network slices:
  flat     -- Neural Pub/Sub, 1 slice, 5 workers (equalized flat baseline)
  neural   -- Neural Pub/Sub, 3 slices, no governance (neural placement)
  rr       -- Neural Pub/Sub, 3 slices, no governance (round-robin placement)
  gov      -- Neural Pub/Sub, 3 slices + governance constraints
  gov-fail -- Neural Pub/Sub, 3 slices + governance + failure injection at t=15min

Per configuration:
  5 seeds x medium workload x 3-stage pipeline.
  gov-fail additionally injects a node failure at t=15min and measures adaptation time.

Outputs CSV results to results/slicing/.

Usage:
    python -m scripts.run_slicing [--dry-run] [--seeds 42,123,456,789,0]
    python -m scripts.run_slicing --configs gov,gov-fail --seeds 42
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    COMPOSE_FLAT,
    COMPOSE_FLAT_EQ,
    COMPOSE_GOVERNANCE,
    DEFAULT_MEASUREMENT_S,
    DEFAULT_WARMUP_S,
    PROJECT_ROOT,
    inject_compose_kill,
    phase_main,
    resolve_config,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "slicing"

DEFAULT_RATE = "medium"
DEFAULT_RATE_VALUE = 5.0
DEFAULT_COMPLEXITY = 3
COMPLEXITY_MIX = {"cqi_prediction": 0.5, "anomaly_detection": 0.5, "sensor_fusion": 0.0}

# Config definitions
CONFIGS = {
    "flat": {"num_slices": 1, "governance": False, "failure_injection": False},
    "neural": {"num_slices": 3, "governance": False, "failure_injection": False},
    "rr": {"num_slices": 3, "governance": False, "failure_injection": False},
    "gov": {"num_slices": 3, "governance": True, "failure_injection": False},
    "gov-fail": {"num_slices": 3, "governance": True, "failure_injection": True},
}

# Mapping from new config names to _CONFIG_TABLE keys
_CONFIG_KEY_MAP = {
    "flat": "B1eq",
    "neural": "B2",
    "rr": "B2flat",
    "gov": "B3",
    "gov-fail": "B4",
}


@dataclass
class RunConfig:
    """A single slicing run configuration."""
    config_name: str
    seed: int
    num_slices: int = 1
    governance: bool = False
    failure_injection: bool = False
    transport: str = "http"
    failure_delay_s: int = 300
    warmup_s: int = DEFAULT_WARMUP_S
    measurement_s: int = DEFAULT_MEASUREMENT_S

    @property
    def run_id(self) -> str:
        return f"{self.config_name}_{self.transport}_rate-{DEFAULT_RATE}_stages-{DEFAULT_COMPLEXITY}_seed-{self.seed}"


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    transports: list[str] | None = None,
) -> list[RunConfig]:
    """Build the run matrix: configs x transports x seeds."""
    if transports is None:
        from scripts._common import TRANSPORTS
        transports = TRANSPORTS
    runs = []
    for config_name, transport, seed in itertools.product(configs, transports, seeds):
        cfg = CONFIGS[config_name]
        runs.append(RunConfig(
            config_name=config_name,
            seed=seed,
            transport=transport,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        ))
    return runs


def _run(run: RunConfig, dry_run: bool) -> dict:
    run_id = run.run_id
    total_duration = run.warmup_s + run.measurement_s

    logger.info(
        "Run: %s (rate=%.1f, stages=%d, seed=%d, transport=%s, slices=%d, "
        "governance=%s, failure=%s, duration=%ds)",
        run_id, DEFAULT_RATE_VALUE, DEFAULT_COMPLEXITY, run.seed,
        run.transport, run.num_slices, run.governance, run.failure_injection,
        total_duration,
    )

    # Use resolve_config for compose files (handles transport overlay)
    config_key = _CONFIG_KEY_MAP[run.config_name]
    resolved = resolve_config(
        config_key, rate=DEFAULT_RATE, stages=DEFAULT_COMPLEXITY,
        seed=run.seed, transport=run.transport,
    )

    # Determine placement strategy: configs that specify a static broker
    # (e.g., rr) use their own PLACEMENT env var; others default to neural.
    placement = resolved.env.get("PLACEMENT", "neural")

    env = {
        **resolved.env,
        "PLACEMENT_STRATEGY": placement,
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

    compose_files = resolved.compose_files

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
        compose_files=compose_files,
    )


def main():
    phase_main(
        phase_name="Slicing",
        description="Slicing: Slice-aware placement",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
    )


if __name__ == "__main__":
    main()
