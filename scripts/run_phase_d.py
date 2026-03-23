#!/usr/bin/env python3
"""Phase D: Failure and adaptation.

Systematically tests failure injection and measures recovery:
  D1 -- Execution unit (worker) failure: kill a worker, measure re-placement time
  D2 -- Broker failure: kill domain broker, measure proxy recovery
  D3 -- Network partition: disconnect federation network, measure degradation
  D4 -- Sensor-worker (URLLC) failure: kill URLLC worker, measure CQI pipeline degradation

Per test:
  5 runs, inject failure at t=15min, measure recovery time and pipeline
  completion rate.

Outputs CSV results to results/phase_d/.

Usage:
    python scripts/run_phase_d.py [--dry-run] [--seeds 42,123,456,789,0]
    python scripts/run_phase_d.py --configs D1,D2 --seeds 42
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
    EXTENDED_SEEDS,
    PROJECT_ROOT,
    inject_compose_kill,
    inject_network_partition,
    phase_main,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_d"
COMPOSE_FAILURE = PROJECT_ROOT / "docker-compose.failure.yaml"

# Config definitions: each maps to a failure type and docker target.
# CRITICAL: targets MUST match actual compose service/network names.
# Validated by tests/test_failure_targets.py against compose YAML files.
CONFIGS = {
    "D1": {"failure_type": "worker", "failure_target": "worker-d1-embb-1"},
    "D2": {"failure_type": "broker", "failure_target": "broker-d1"},
    "D3": {"failure_type": "network", "failure_target": "federation"},
    "D4": {"failure_type": "worker", "failure_target": "worker-d1-urllc-1"},
}


@dataclass
class RunConfig:
    """A single Phase D run configuration."""
    config_name: str
    seed: int
    failure_type: str
    failure_target: str
    warmup_s: int = 120
    measurement_s: int = 600
    failure_delay_s: int = 300  # 5min from run start (3min into measurement), consistent with B4

    @property
    def run_id(self) -> str:
        return f"{self.config_name}_failure-{self.failure_type}_seed-{self.seed}"


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
) -> list[RunConfig]:
    """Build the run matrix."""
    runs = []
    for config_name, seed in itertools.product(configs, seeds):
        cfg = CONFIGS[config_name]
        runs.append(RunConfig(
            config_name=config_name,
            seed=seed,
            failure_type=cfg["failure_type"],
            failure_target=cfg["failure_target"],
        ))
    return runs


def _make_failure_fn(
    run: RunConfig,
    project_name: str,
    env: dict[str, str],
    compose_files: list[Path] | None = None,
):
    """Return the appropriate failure injection callable for this run."""
    if run.failure_type in ("worker", "broker"):
        return partial(
            inject_compose_kill,
            project_name=project_name,
            compose_file=COMPOSE_FILE,
            env=env,
            target=run.failure_target,
            delay_s=run.failure_delay_s,
            label=run.failure_type,
            compose_files=compose_files,
        )
    elif run.failure_type == "network":
        return partial(
            inject_network_partition,
            project_name=project_name,
            target=run.failure_target,
            delay_s=run.failure_delay_s,
        )
    else:
        raise ValueError(f"Unknown failure type: {run.failure_type}")


def _run(run: RunConfig, dry_run: bool) -> dict:
    run_id = run.run_id
    total_duration = run.warmup_s + run.measurement_s
    # Must match run_single's normalization: lowercase + replace _ with -
    project_name = f"npubsub-{run_id.lower().replace('_', '-')}"

    logger.info(
        "Run: %s (failure=%s, target=%s, seed=%d, "
        "inject_at=%ds, duration=%ds)",
        run_id, run.failure_type, run.failure_target,
        run.seed, run.failure_delay_s, total_duration,
    )

    env = {
        "PLACEMENT_STRATEGY": "neural",
        "ARRIVAL_RATE": "5.0",
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "PIPELINE_MIX_CQI": "0.5",
        "PIPELINE_MIX_ANOMALY": "0.5",
        "PIPELINE_MIX_FUSION": "0.0",
        "WARMUP_S": str(run.warmup_s),
        "FEDERATION_ENABLED": "true",
        "GOVERNANCE_ENABLED": "true",
        "NUM_DOMAINS": "2",
        "FAILURE_TYPE": run.failure_type,
        "FAILURE_DELAY_S": str(run.failure_delay_s),
    }

    # Phase D uses the failure compose overlay to disable restart: unless-stopped.
    # Without this, Docker auto-restarts killed containers, making injection invisible.
    # The compose_files list must be passed to BOTH run_single AND the failure fn,
    # so that `docker compose kill` uses the same file stack as `docker compose up`.
    compose_files = [COMPOSE_FILE, COMPOSE_FAILURE]

    failure_fn = _make_failure_fn(run, project_name, env, compose_files=compose_files)

    return run_single(
        run_id=run_id,
        env=env,
        results_dir=RESULTS_DIR,
        total_duration=total_duration,
        dry_run=dry_run,
        failure_fn=failure_fn,
        compose_files=compose_files,
        detached=True,  # Don't abort when killed container exits (L38)
    )


def main():
    phase_main(
        phase_name="Phase D",
        description="Phase D: Failure and adaptation",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
        default_seeds=EXTENDED_SEEDS,
    )


if __name__ == "__main__":
    main()
