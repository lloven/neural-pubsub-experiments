#!/usr/bin/env python3
"""Stress: Combined H3+H6 contention + failure experiment.

At medium load (resilience phase), all placement strategies recover equally
from worker failure because the broker's health check handles rerouting.  At
HIGH load (20 pps, 2x capacity), S3's load-aware re-placement should
outperform S1's blind round-robin because surviving workers are near
saturation and intelligent load distribution matters.

Stress phase combines contention rates with failure injection and strategy
comparison to test this hypothesis.

Config matrix (12 configs):
  10pps-rr-nofail:     10 pps, S1, no failure      (H3 baseline)
  10pps-neural-nofail: 10 pps, S3, no failure      (H3 baseline)
  10pps-rr-fail:       10 pps, S1, eMBB kill @300s  (H6 medium-load)
  10pps-neural-fail:   10 pps, S3, eMBB kill @300s  (H6 medium-load)
  20pps-rr-nofail:     20 pps, S1, no failure      (H3 overload)
  20pps-neural-nofail: 20 pps, S3, no failure      (H3 overload)
  20pps-rr-fail:       20 pps, S1, eMBB kill @300s  (H3+H6 key cell)
  20pps-neural-fail:   20 pps, S3, eMBB kill @300s  (H3+H6 key cell)
  50pps-rr-nofail:     50 pps, S1, no failure      (extreme overload baseline)
  50pps-neural-nofail: 50 pps, S3, no failure      (extreme overload neural)
  50pps-rr-fail:       50 pps, S1, eMBB kill @300s  (extreme overload + failure, rr)
  50pps-neural-fail:   50 pps, S3, eMBB kill @300s  (extreme overload + failure, neural)

12 configs x 5 seeds = 60 runs x 12 min = ~12 hours.  HTTP only.

Usage:
    python -m scripts.run_stress [--dry-run] [--seeds 42,123,456,789,0]
    python -m scripts.run_stress --configs 20pps-rr-fail,20pps-neural-fail --seeds 42
    python -m scripts.run_stress --warmup 30 --measurement 120  # smoke
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    DEFAULT_SEEDS,
    PROJECT_ROOT,
    inject_compose_kill,
    phase_main,
    run_single,
)
from scripts.run_resilience import STRATEGIES, _strategy_env

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "stress"
COMPOSE_FAILURE = PROJECT_ROOT / "docker-compose.failure.yaml"

# ---------------------------------------------------------------------------
# Config definitions
# ---------------------------------------------------------------------------

CONFIGS = {
    "10pps-rr-nofail":     {"arrival_rate": 10.0, "strategy": "S1", "failure_target": None},
    "10pps-neural-nofail": {"arrival_rate": 10.0, "strategy": "S3", "failure_target": None},
    "10pps-rr-fail":       {"arrival_rate": 10.0, "strategy": "S1", "failure_target": "worker-d1-embb-1"},
    "10pps-neural-fail":   {"arrival_rate": 10.0, "strategy": "S3", "failure_target": "worker-d1-embb-1"},
    "20pps-rr-nofail":     {"arrival_rate": 20.0, "strategy": "S1", "failure_target": None},
    "20pps-neural-nofail": {"arrival_rate": 20.0, "strategy": "S3", "failure_target": None},
    "20pps-rr-fail":       {"arrival_rate": 20.0, "strategy": "S1", "failure_target": "worker-d1-embb-1"},
    "20pps-neural-fail":   {"arrival_rate": 20.0, "strategy": "S3", "failure_target": "worker-d1-embb-1"},
    "50pps-rr-nofail":     {"arrival_rate": 50.0, "strategy": "S1", "failure_target": None},
    "50pps-neural-nofail": {"arrival_rate": 50.0, "strategy": "S3", "failure_target": None},
    "50pps-rr-fail":       {"arrival_rate": 50.0, "strategy": "S1", "failure_target": "worker-d1-embb-1"},
    "50pps-neural-fail":   {"arrival_rate": 50.0, "strategy": "S3", "failure_target": "worker-d1-embb-1"},
}


@dataclass
class StressRunConfig:
    """A single stress run configuration."""
    config_name: str
    seed: int
    arrival_rate: float
    strategy: str
    failure_target: str | None
    warmup_s: int = 120
    measurement_s: int = 600
    failure_delay_s: int = 300

    @property
    def run_id(self) -> str:
        return f"{self.config_name}_seed-{self.seed}"


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    warmup_s: int | None = None,
    measurement_s: int | None = None,
    failure_delay_s: int | None = None,
) -> list[StressRunConfig]:
    """Build the stress run matrix.

    Args:
        configs: Config names (subset of the 12 configs).
        seeds: List of random seeds.
        warmup_s: Override warmup duration.
        measurement_s: Override measurement duration.
        failure_delay_s: Override failure injection delay.

    Returns:
        List of StressRunConfig, one per config x seed combination.
    """
    overrides: dict = {}
    if warmup_s is not None:
        overrides["warmup_s"] = warmup_s
    if measurement_s is not None:
        overrides["measurement_s"] = measurement_s
    if failure_delay_s is not None:
        overrides["failure_delay_s"] = failure_delay_s

    runs = []
    for config_name in configs:
        cfg = CONFIGS[config_name]
        for seed in seeds:
            runs.append(StressRunConfig(
                config_name=config_name,
                seed=seed,
                arrival_rate=cfg["arrival_rate"],
                strategy=cfg["strategy"],
                failure_target=cfg["failure_target"],
                **overrides,
            ))
    return runs


def _run_distributed(run: StressRunConfig, dry_run: bool) -> dict:
    """Execute a stress run on the distributed 4-VM cluster."""
    from scripts import multi_vm_runner
    run_id = run.run_id
    strat_env = _strategy_env(run.strategy)

    failure_fn = None
    if run.failure_target is not None:
        failure_fn = _partial(
            multi_vm_runner.inject_remote_kill,
            vm=multi_vm_runner.VMS[0],
            container="deploy-worker-0-1",
            delay_s=run.failure_delay_s,
        )

    multi_vm_runner.run_single(
        config=run.config_name,
        run_id=run_id,
        seed=run.seed,
        placement_mode=strat_env.get("PLACEMENT_STRATEGY", "neural"),
        governance_config="all",
        broker_module=strat_env.get("BROKER_MODULE"),
        placement=strat_env.get("PLACEMENT"),
        workload_env={"ARRIVAL_RATE": str(run.arrival_rate)},
        results_subdir="stress",
        warmup_s=run.warmup_s,
        measurement_s=run.measurement_s,
        failure_fn=failure_fn,
        wan_emulation=False,
        dry_run=dry_run,
    )
    return {"run_id": run_id, "status": "completed" if not dry_run else "dry_run",
            "result_file": f"results/stress/{run_id}"}


def _run(run: StressRunConfig, dry_run: bool, **kwargs) -> dict:
    """Execute a single stress run."""
    topology = kwargs.get("topology", "local")
    if topology == "distributed":
        return _run_distributed(run, dry_run)

    run_id = run.run_id
    total_duration = run.warmup_s + run.measurement_s
    project_name = f"npubsub-{run_id.lower().replace('_', '-')}"

    logger.info(
        "Run: %s (rate=%.0f, strategy=%s, failure=%s, seed=%d, "
        "inject_at=%ds, duration=%ds)",
        run_id, run.arrival_rate, run.strategy,
        run.failure_target or "none", run.seed,
        run.failure_delay_s, total_duration,
    )

    # Strategy env vars (S1=round-robin, S3=neural)
    strat_env = _strategy_env(run.strategy)

    env = {
        **strat_env,
        "ARRIVAL_RATE": str(run.arrival_rate),
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "WARMUP_S": str(run.warmup_s),
        "FEDERATION_ENABLED": "true",
        "GOVERNANCE_ENABLED": "true",
        "NUM_DOMAINS": "2",
    }

    # Failure configs use the compose failure overlay and inject a kill
    compose_files = [COMPOSE_FILE]
    failure_fn = None

    if run.failure_target is not None:
        env["FAILURE_TYPE"] = "worker"
        env["FAILURE_DELAY_S"] = str(run.failure_delay_s)
        compose_files.append(COMPOSE_FAILURE)
        failure_fn = partial(
            inject_compose_kill,
            project_name=project_name,
            compose_file=COMPOSE_FILE,
            env=env,
            target=run.failure_target,
            delay_s=run.failure_delay_s,
            label="worker",
            compose_files=compose_files,
        )

    return run_single(
        run_id=run_id,
        env=env,
        results_dir=RESULTS_DIR,
        total_duration=total_duration,
        dry_run=dry_run,
        failure_fn=failure_fn,
        compose_files=compose_files,
        detached=True,
    )


def _extra_args(parser):
    """Add stress timing overrides."""
    parser.add_argument("--warmup", type=int, default=None,
                        help="Override warmup_s (default: 120)")
    parser.add_argument("--measurement", type=int, default=None,
                        help="Override measurement_s (default: 600)")
    parser.add_argument("--failure-delay", type=int, default=None,
                        help="Override failure_delay_s (default: 300)")


def _parse_extra(args):
    """Pass timing overrides to build_run_matrix."""
    return dict(
        warmup_s=args.warmup,
        measurement_s=args.measurement,
        failure_delay_s=args.failure_delay,
    )


def main():
    phase_main(
        phase_name="Stress",
        description="Stress: Combined H3+H6 contention + failure",
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
