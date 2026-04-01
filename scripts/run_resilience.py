#!/usr/bin/env python3
"""Resilience: Failure and adaptation.

Systematically tests worker failure injection and measures recovery:
  embb-kill  -- eMBB worker failure (strategy baked in: embb-neural, embb-rr, embb-random)
  urllc-kill -- URLLC worker failure (strategy baked in: urllc-neural, urllc-rr, urllc-random)
  funnel-wait    -- Funnel resilience (wait): kill a sensor-input worker
  funnel-proceed -- Funnel resilience (proceed): kill a sensor-input worker
  funnel-abort   -- Funnel resilience (abort): kill a sensor-input worker

Per test:
  10 runs, inject failure at t=15min, measure recovery time and pipeline
  completion rate.

Outputs CSV results to results/resilience/.

Usage:
    python -m scripts.run_resilience [--dry-run] [--seeds 42,123,456,789,0]
    python -m scripts.run_resilience --configs embb-kill,urllc-kill --seeds 42
    python -m scripts.run_resilience --strategy S1          # round-robin only
    python -m scripts.run_resilience --strategy all          # S1+S2+S3 comparison
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    EXTENDED_SEEDS,
    PROJECT_ROOT,
    inject_compose_kill,
    inject_network_partition,
    phase_main,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "resilience"
COMPOSE_FAILURE = PROJECT_ROOT / "docker-compose.failure.yaml"

# Config definitions: each maps to a failure type and docker target.
# CRITICAL: targets MUST match actual compose service/network names.
# Validated by tests/test_failure_targets.py against compose YAML files.
CONFIGS = {
    "embb-kill": {"failure_type": "worker", "failure_target": "worker-d1-embb-1"},
    "urllc-kill": {"failure_type": "worker", "failure_target": "worker-d1-urllc-1"},
    # Funnel resilience modes (Section 4.4.3): kill a sensor-input worker
    # feeding the fuse stage of the sensor_fusion pipeline with each mode.
    # Target: worker-d1-urllc-2 (distinct from embb-kill/urllc-kill targets).
    # pipeline_mix_fusion=1.0 ensures only sensor_fusion pipelines are submitted.
    "funnel-wait": {"failure_type": "worker", "failure_target": "worker-d1-urllc-2",
            "funnel_mode": "wait", "pipeline_mix_fusion": "1.0",
            "funnel_bypass_replace": "true"},
    "funnel-proceed": {"failure_type": "worker", "failure_target": "worker-d1-urllc-2",
            "funnel_mode": "proceed", "pipeline_mix_fusion": "1.0",
            "funnel_bypass_replace": "true"},
    "funnel-abort": {"failure_type": "worker", "failure_target": "worker-d1-urllc-2",
            "funnel_mode": "abort", "pipeline_mix_fusion": "1.0",
            "funnel_bypass_replace": "true"},
}

# Placement strategies (mirrors baseline naming: S1=round-robin, S2=random, S3=neural).
# S1/S2 use the static broker with a placement algorithm; S3 uses the default neural broker.
STRATEGIES = {
    "S1": {"placement": "round_robin"},
    "S2": {"placement": "random"},
    "S3": {"placement": "neural"},
}


def _strategy_env(strategy: str) -> dict[str, str]:
    """Return environment variable overrides for the given strategy.

    S1 (round-robin) and S2 (random) override BROKER_MODULE to use the
    static broker and set PLACEMENT accordingly.  S3 (neural) uses the
    default neural broker (no BROKER_MODULE override) and sets
    PLACEMENT_STRATEGY=neural for downstream compatibility.
    """
    if strategy not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {strategy}. Valid: {sorted(STRATEGIES.keys())}")
    placement = STRATEGIES[strategy]["placement"]
    if placement in ("round_robin", "random"):
        return {
            "BROKER_MODULE": "src.broker.static_broker",
            "PLACEMENT": placement,
            "PLACEMENT_STRATEGY": placement,
        }
    else:
        # Neural: default broker, no BROKER_MODULE override
        return {"PLACEMENT_STRATEGY": "neural"}


@dataclass
class RunConfig:
    """A single resilience run configuration."""
    config_name: str
    seed: int
    failure_type: str
    failure_target: str
    strategy: str = "S3"
    warmup_s: int = 120
    measurement_s: int = 600
    failure_delay_s: int = 300  # 5min from run start (3min into measurement)

    @property
    def run_id(self) -> str:
        return f"{self.config_name}_failure-{self.failure_type}_{self.strategy}_seed-{self.seed}"


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    warmup_s: int | None = None,
    measurement_s: int | None = None,
    failure_delay_s: int | None = None,
    strategies: list[str] | None = None,
) -> list[RunConfig]:
    """Build the run matrix.  Optional timing overrides for smoke tests.

    Args:
        strategies: List of strategy labels (e.g. ["S1", "S2", "S3"]).
            Defaults to ["S3"] (neural only, preserving current behavior).
    """
    if strategies is None:
        strategies = ["S3"]
    runs = []
    overrides = {}
    if warmup_s is not None:
        overrides["warmup_s"] = warmup_s
    if measurement_s is not None:
        overrides["measurement_s"] = measurement_s
    if failure_delay_s is not None:
        overrides["failure_delay_s"] = failure_delay_s
    for config_name, strategy, seed in itertools.product(configs, strategies, seeds):
        cfg = CONFIGS[config_name]
        runs.append(RunConfig(
            config_name=config_name,
            seed=seed,
            failure_type=cfg["failure_type"],
            failure_target=cfg["failure_target"],
            strategy=strategy,
            **overrides,
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


def _run_distributed(run: RunConfig, dry_run: bool) -> dict:
    """Execute a resilience run on the distributed 4-VM cluster."""
    from scripts import multi_vm_runner
    run_id = run.run_id
    cfg = CONFIGS[run.config_name]
    strat_env = _strategy_env(run.strategy)

    pipeline_mix_fusion = cfg.get("pipeline_mix_fusion", "0.0")
    mix_cqi = "0.0" if pipeline_mix_fusion != "0.0" else "0.5"
    mix_anomaly = "0.0" if pipeline_mix_fusion != "0.0" else "0.5"

    # Map failure target to distributed VM + container
    failure_fn = None
    if run.failure_type == "worker":
        # Kill a worker on VM1 (domain 1)
        failure_fn = partial(
            multi_vm_runner.inject_remote_kill,
            vm=multi_vm_runner.VMS[0],
            container="deploy-worker-0-1",
            delay_s=run.failure_delay_s,
        )

    workload_env = {
        "PIPELINE_MIX_CQI": mix_cqi,
        "PIPELINE_MIX_ANOMALY": mix_anomaly,
        "PIPELINE_MIX_FUSION": pipeline_mix_fusion,
    }
    if cfg.get("funnel_mode"):
        workload_env["FUNNEL_MODE"] = cfg["funnel_mode"]
    if cfg.get("funnel_bypass_replace"):
        workload_env["FUNNEL_BYPASS_REPLACE"] = cfg["funnel_bypass_replace"]

    multi_vm_runner.run_single(
        config=run.config_name,
        run_id=run_id,
        seed=run.seed,
        placement_mode=strat_env.get("PLACEMENT_STRATEGY", "neural"),
        governance_config="all",
        broker_module=strat_env.get("BROKER_MODULE"),
        placement=strat_env.get("PLACEMENT"),
        workload_env=workload_env,
        results_subdir="resilience",
        warmup_s=run.warmup_s,
        measurement_s=run.measurement_s,
        failure_fn=failure_fn,
        wan_emulation=False,
        dry_run=dry_run,
    )
    return {"run_id": run_id, "status": "completed" if not dry_run else "dry_run",
            "result_file": f"results/resilience/{run_id}"}


def _run(run: RunConfig, dry_run: bool, **kwargs) -> dict:
    topology = kwargs.get("topology", "local")
    if topology == "distributed":
        return _run_distributed(run, dry_run)

    run_id = run.run_id
    total_duration = run.warmup_s + run.measurement_s
    # Must match run_single's normalization: lowercase + replace _ with -
    project_name = f"npubsub-{run_id.lower().replace('_', '-')}"

    logger.info(
        "Run: %s (failure=%s, target=%s, strategy=%s, seed=%d, "
        "inject_at=%ds, duration=%ds)",
        run_id, run.failure_type, run.failure_target, run.strategy,
        run.seed, run.failure_delay_s, total_duration,
    )

    # Look up config-level overrides (funnel mode, pipeline mix)
    cfg = CONFIGS[run.config_name]
    pipeline_mix_fusion = cfg.get("pipeline_mix_fusion", "0.0")
    # When fusion is 1.0, the other mixes must be 0.0
    if pipeline_mix_fusion != "0.0":
        mix_cqi = "0.0"
        mix_anomaly = "0.0"
    else:
        mix_cqi = "0.5"
        mix_anomaly = "0.5"

    # Strategy env vars (S1=round-robin, S2=random, S3=neural)
    strat_env = _strategy_env(run.strategy)

    env = {
        **strat_env,
        "ARRIVAL_RATE": "5.0",
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "PIPELINE_MIX_CQI": mix_cqi,
        "PIPELINE_MIX_ANOMALY": mix_anomaly,
        "PIPELINE_MIX_FUSION": pipeline_mix_fusion,
        "WARMUP_S": str(run.warmup_s),
        "FEDERATION_ENABLED": "true",
        "GOVERNANCE_ENABLED": "true",
        "NUM_DOMAINS": "2",
        "FAILURE_TYPE": run.failure_type,
        "FAILURE_DELAY_S": str(run.failure_delay_s),
    }

    # Funnel resilience mode (Section 4.4.3): passed to broker containers
    funnel_mode = cfg.get("funnel_mode")
    if funnel_mode is not None:
        env["FUNNEL_MODE"] = funnel_mode

    # Funnel bypass replace: skip re-placement for funnel predecessors so
    # the funnel policy actually engages (see funnel_resilience.py docstring).
    funnel_bypass = cfg.get("funnel_bypass_replace")
    if funnel_bypass is not None:
        env["FUNNEL_BYPASS_REPLACE"] = funnel_bypass

    # Resilience uses the failure compose overlay to disable restart: unless-stopped.
    # Without this, Docker auto-restarts killed containers, making injection invisible.
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


def _extra_args(parser):
    """Add resilience timing overrides and strategy selection."""
    parser.add_argument("--warmup", type=int, default=None,
                        help="Override warmup_s (default: 120)")
    parser.add_argument("--measurement", type=int, default=None,
                        help="Override measurement_s (default: 600)")
    parser.add_argument("--failure-delay", type=int, default=None,
                        help="Override failure_delay_s (default: 300)")
    parser.add_argument(
        "--strategy", default="S3",
        help="Placement strategy: S1 (round-robin), S2 (random), S3 (neural, default), or 'all'",
    )


def _parse_extra(args):
    """Pass timing overrides and strategy list to build_run_matrix."""
    if args.strategy.lower() == "all":
        strategies = list(STRATEGIES.keys())
    else:
        strategies = [s.strip() for s in args.strategy.split(",")]
        for s in strategies:
            if s not in STRATEGIES:
                raise SystemExit(
                    f"Unknown strategy: {s}. Valid: {sorted(STRATEGIES.keys())} or 'all'"
                )
    return dict(
        warmup_s=args.warmup,
        measurement_s=args.measurement,
        failure_delay_s=args.failure_delay,
        strategies=strategies,
    )


def main():
    phase_main(
        phase_name="Resilience",
        description="Resilience: Failure and adaptation",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
        default_seeds=EXTENDED_SEEDS,
        extra_args_fn=_extra_args,
        parse_extra_fn=_parse_extra,
    )


if __name__ == "__main__":
    main()
