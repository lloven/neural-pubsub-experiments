#!/usr/bin/env python3
"""Phase E: Combined H3+H6 contention + failure experiment.

At medium load (Phase D), all placement strategies recover equally from
worker failure because the broker's health check handles rerouting.  At
HIGH load (20 pps, 2x capacity), S3's load-aware re-placement should
outperform S1's blind round-robin because surviving workers are near
saturation and intelligent load distribution matters.

Phase E combines A.6 contention rates with D failure injection and
strategy comparison to test this hypothesis.

Config matrix (8 configs):
  E1: 10 pps, S1, no failure      (H3 baseline)
  E2: 10 pps, S3, no failure      (H3 baseline)
  E3: 10 pps, S1, eMBB kill @300s (H6 medium-load)
  E4: 10 pps, S3, eMBB kill @300s (H6 medium-load)
  E5: 20 pps, S1, no failure      (H3 overload)
  E6: 20 pps, S3, no failure      (H3 overload)
  E7: 20 pps, S1, eMBB kill @300s (H3+H6 key cell)
  E8: 20 pps, S3, eMBB kill @300s (H3+H6 key cell)

8 configs x 5 seeds = 40 runs x 12 min = ~8 hours.  HTTP only.

Usage:
    python -m scripts.run_phase_e [--dry-run] [--seeds 42,123,456,789,0]
    python -m scripts.run_phase_e --configs E7,E8 --seeds 42
    python -m scripts.run_phase_e --warmup 30 --measurement 120  # smoke
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
from scripts.run_phase_d import STRATEGIES, _strategy_env

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_e"
COMPOSE_FAILURE = PROJECT_ROOT / "docker-compose.failure.yaml"

# ---------------------------------------------------------------------------
# Config definitions
# ---------------------------------------------------------------------------

CONFIGS = {
    "E1": {"arrival_rate": 10.0, "strategy": "S1", "failure_target": None},
    "E2": {"arrival_rate": 10.0, "strategy": "S3", "failure_target": None},
    "E3": {"arrival_rate": 10.0, "strategy": "S1", "failure_target": "worker-d1-embb-1"},
    "E4": {"arrival_rate": 10.0, "strategy": "S3", "failure_target": "worker-d1-embb-1"},
    "E5": {"arrival_rate": 20.0, "strategy": "S1", "failure_target": None},
    "E6": {"arrival_rate": 20.0, "strategy": "S3", "failure_target": None},
    "E7": {"arrival_rate": 20.0, "strategy": "S1", "failure_target": "worker-d1-embb-1"},
    "E8": {"arrival_rate": 20.0, "strategy": "S3", "failure_target": "worker-d1-embb-1"},
}


@dataclass
class PhaseERunConfig:
    """A single Phase E run configuration."""
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
        rate_label = f"rate-{self.arrival_rate:.0f}"
        fail_label = "fail" if self.failure_target else "nofail"
        return (
            f"{self.config_name}_{rate_label}_{self.strategy}_"
            f"{fail_label}_seed-{self.seed}"
        )


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    warmup_s: int | None = None,
    measurement_s: int | None = None,
    failure_delay_s: int | None = None,
) -> list[PhaseERunConfig]:
    """Build the Phase E run matrix.

    Args:
        configs: Config names (subset of E1-E8).
        seeds: List of random seeds.
        warmup_s: Override warmup duration.
        measurement_s: Override measurement duration.
        failure_delay_s: Override failure injection delay.

    Returns:
        List of PhaseERunConfig, one per config x seed combination.
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
            runs.append(PhaseERunConfig(
                config_name=config_name,
                seed=seed,
                arrival_rate=cfg["arrival_rate"],
                strategy=cfg["strategy"],
                failure_target=cfg["failure_target"],
                **overrides,
            ))
    return runs


def _run(run: PhaseERunConfig, dry_run: bool) -> dict:
    """Execute a single Phase E run."""
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
    """Add Phase E timing overrides."""
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
        phase_name="Phase E",
        description="Phase E: Combined H3+H6 contention + failure",
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
