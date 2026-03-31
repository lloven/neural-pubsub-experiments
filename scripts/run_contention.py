#!/usr/bin/env python3
"""Contention: Resource contention under overload.

Stresses the system beyond capacity to measure graceful degradation.

  | Config    | Arrival rate        | Workers | Expected behaviour                     |
  |-----------|--------------------:|--------:|----------------------------------------|
  | 20pps     | 20/s (2x capacity)  |       5 | Queue buildup, graceful degradation    |
  | 50pps     | 50/s (5x capacity)  |       5 | Saturation, measure failure rate       |
  | 10pps-kill| 10/s (at capacity)  |   5->3  | Dynamic contention from worker loss    |

  Each config: 2-min warmup + 10-min measurement = 12 min.
  Matrix: 3 configs x 5 seeds = 15 runs. Total: ~3h.

Usage:
    python -m scripts.run_contention [--dry-run]
    python -m scripts.run_contention --configs 20pps,50pps --seeds 42
"""

from __future__ import annotations

import csv
import itertools
import logging
from dataclasses import dataclass
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    DEFAULT_SEEDS,
    PROJECT_ROOT,
    inject_compose_kill,
    phase_main,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "contention"

# Failure mode constants
FAILURE_KILL_WORKERS = "kill_2_workers"

# Contention configs
CONTENTION_CONFIGS = {
    "20pps": {
        "arrival_rate": 20.0,
        "n_workers": 5,
        "description": "2x capacity overload",
        "failure": None,
    },
    "50pps": {
        "arrival_rate": 50.0,
        "n_workers": 5,
        "description": "5x capacity saturation",
        "failure": None,
    },
    "10pps-kill": {
        "arrival_rate": 10.0,
        "n_workers": 5,
        "description": "At capacity, then kill 2 workers at t=5min",
        "failure": FAILURE_KILL_WORKERS,
    },
}


@dataclass
class ContentionRunConfig:
    """A single contention run."""

    config_name: str
    arrival_rate: float
    n_workers: int
    seed: int
    failure: str | None = None
    warmup_s: int = 120
    measurement_s: int = 600
    failure_delay_s: int = 300  # 5min from run start; consistent with resilience phase


def build_contention_matrix(
    configs: list[str],
    seeds: list[int],
    warmup_s: int | None = None,
    measurement_s: int | None = None,
) -> list[ContentionRunConfig]:
    """Build the contention run matrix.  Optional timing overrides for smoke tests."""
    runs = []
    overrides = {}
    if warmup_s is not None:
        overrides["warmup_s"] = warmup_s
    if measurement_s is not None:
        overrides["measurement_s"] = measurement_s
    for config_name, seed in itertools.product(configs, seeds):
        cfg = CONTENTION_CONFIGS[config_name]
        runs.append(ContentionRunConfig(
            config_name=config_name,
            arrival_rate=cfg["arrival_rate"],
            n_workers=cfg["n_workers"],
            seed=seed,
            failure=cfg["failure"],
            **overrides,
        ))
    return runs


def _run_contention_distributed(run: ContentionRunConfig, dry_run: bool) -> dict:
    """Execute a contention run on the distributed 4-VM cluster."""
    from scripts import multi_vm_runner
    from functools import partial

    run_id = f"{run.config_name}_seed-{run.seed}"

    failure_fn = None
    if run.failure == FAILURE_KILL_WORKERS:
        failure_fn = partial(
            multi_vm_runner.inject_remote_kill,
            vm=multi_vm_runner.VMS[0],
            container="deploy-worker-0-1",
            delay_s=run.failure_delay_s,
        )

    multi_vm_runner.run_single(
        config=run_id,
        seed=run.seed,
        placement_mode="neural",
        governance_config="none",
        workload_env={
            "ARRIVAL_RATE": str(run.arrival_rate),
            "PIPELINE_MIX_CQI": "0.34",
            "PIPELINE_MIX_ANOMALY": "0.33",
            "PIPELINE_MIX_FUSION": "0.33",
        },
        results_subdir="contention",
        warmup_s=run.warmup_s,
        measurement_s=run.measurement_s,
        failure_fn=failure_fn,
        wan_emulation=False,
        dry_run=dry_run,
    )
    return {"run_id": run_id, "status": "completed" if not dry_run else "dry_run",
            "result_file": f"results/contention/{run_id}.csv"}


def _run_contention(run: ContentionRunConfig, dry_run: bool, **kwargs) -> dict:
    """Execute one contention run."""
    topology = kwargs.get("topology", "local")
    if topology == "distributed":
        return _run_contention_distributed(run, dry_run)

    run_id = f"{run.config_name}_seed-{run.seed}"
    total_duration = run.warmup_s + run.measurement_s

    logger.info(
        "Run: %s (rate=%.1f, workers=%d, seed=%d, failure=%s, duration=%ds)",
        run_id, run.arrival_rate, run.n_workers, run.seed,
        run.failure or "none", total_duration,
    )

    env = {
        "PLACEMENT_STRATEGY": "neural",
        "ARRIVAL_RATE": str(run.arrival_rate),
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "N_WORKERS": str(run.n_workers),
        "WARMUP_S": str(run.warmup_s),
        # Equal mix of all pipeline types
        "PIPELINE_MIX_CQI": "0.34",
        "PIPELINE_MIX_ANOMALY": "0.33",
        "PIPELINE_MIX_FUSION": "0.33",
    }

    failure_fn = None
    if run.failure == FAILURE_KILL_WORKERS:
        # Kill 2 workers at failure_delay_s into the run
        failure_delay = run.failure_delay_s

        def _kill_workers():
            inject_compose_kill(
                project_name=f"npubsub-{run_id}",
                compose_file=COMPOSE_FILE,
                env=env,
                target="worker",
                delay_s=failure_delay,
                label="contention-worker-kill",
            )
        failure_fn = _kill_workers

    return run_single(
        run_id=run_id,
        env=env,
        results_dir=RESULTS_DIR,
        total_duration=total_duration,
        dry_run=dry_run,
        failure_fn=failure_fn,
    )


def main():
    """Run contention experiments."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Contention: Resource contention under overload"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print runs without executing")
    parser.add_argument(
        "--configs", default=",".join(CONTENTION_CONFIGS.keys()),
        help="Comma-separated config names (default: all)",
    )
    parser.add_argument(
        "--seeds", default=",".join(str(s) for s in DEFAULT_SEEDS),
        help=f"Comma-separated seeds (default: {DEFAULT_SEEDS})",
    )
    parser.add_argument("--warmup", type=int, default=None,
                        help="Override warmup_s (default: 120)")
    parser.add_argument("--measurement", type=int, default=None,
                        help="Override measurement_s (default: 600)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    logger.info("=" * 60)
    logger.info("Contention: Resource contention under overload")
    logger.info("=" * 60)

    config_names = [c.strip() for c in args.configs.split(",")]
    seeds = [int(s.strip()) for s in args.seeds.split(",")]

    for c in config_names:
        if c not in CONTENTION_CONFIGS:
            parser.error(
                f"Unknown config: {c}. Valid: {list(CONTENTION_CONFIGS.keys())}"
            )

    runs = build_contention_matrix(
        config_names, seeds,
        warmup_s=args.warmup,
        measurement_s=args.measurement,
    )
    logger.info("Contention: %d runs planned", len(runs))

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    results = []
    for i, run in enumerate(runs, 1):
        logger.info("--- Contention Run %d/%d ---", i, len(runs))
        result = _run_contention(run, args.dry_run)
        results.append(result)

    # Write summary CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_file = RESULTS_DIR / "contention_summary.csv"
    with open(summary_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["run_id", "status", "result_file"])
        writer.writeheader()
        writer.writerows(results)
    logger.info("Contention summary: %s", summary_file)

    completed = sum(1 for r in results if r["status"] == "completed")
    failed = sum(1 for r in results if r["status"] not in ("completed", "dry_run"))
    logger.info(
        "Contention complete: %d/%d runs successful, %d failed",
        completed, len(runs), failed,
    )

    logger.info("Done.")


if __name__ == "__main__":
    main()
