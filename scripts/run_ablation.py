#!/usr/bin/env python3
"""Ablation: stress scenarios where rr-global's limitations emerge.

The main market campaign (run_market.py) uses uniform conditions that
favour rr-global on simple pipelines. This phase tests five stress
scenarios that isolate distinct Walrasian properties round-robin lacks
by construction:

  1. **failure**: kill a worker on VM2 mid-run (information completeness:
     prices encode worker availability; dead worker has infinite price)
  2. **sat-20 / sat-25 / sat-30**: arrival rate sweep around the
     empirical 25 pps inflection point of the 48-worker testbed
     (admission control: clearing prices reject unaffordable pipelines)
  3. **heterogeneous**: edge VMs use 2x slower workers
     (processing_speed=2.0), cloud VMs use 0.67x faster workers
     (processing_speed=0.67) (price discovery: scarce slow workers
     accumulate load and become expensive at equilibrium)

Three strategies (oracle-global, rr-global, market-quad) x five scenarios
x three pipelines (cqi-chain, anomaly-sp, ran-entangled) x five seeds
= 225 runs.

Run length: identical to the main market campaign (warmup + measurement
inherited from EXPERIMENTS["ablation"] in scripts.experiment_matrix, which
references the same constants as EXPERIMENTS["market"]). At the current
240 s + 600 s = 14 min/run setting, the ablation phase takes ~54 hours
on the 4-VM cluster. Identical timing is required so that CR / latency /
p95 distributions are directly comparable to the main campaign data.

Uses a separate compose file (deploy/docker-compose.vm-ablation.yaml) and
worker module (src.worker.ablation_worker) to avoid touching the main
campaign infrastructure. The ablation broker runs with --market-load-aware
(BrokerConfig.market_load_aware=True), enabling load-aware worker
selection in market_mode_placement; the main campaign's compose file
does NOT set this flag, preserving reproducibility of the already-
collected market runs.

Usage:
    python -m scripts.run_ablation --topology distributed --resume
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from pathlib import Path

from scripts._common import DEFAULT_SEEDS, PROJECT_ROOT, phase_main
from scripts.experiment_matrix import EXPERIMENTS

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "ablation"
COMPOSE_FILE = "deploy/docker-compose.vm-ablation.yaml"

# Run timing inherited from the experiment matrix SSoT. EXPERIMENTS["ablation"]
# explicitly references the same constants as EXPERIMENTS["market"] so that
# ablation data is directly comparable to the main market campaign.
_ABLATION_META = EXPERIMENTS["ablation"]
_WARMUP_S = _ABLATION_META["warmup_s"]
_MEASUREMENT_S = _ABLATION_META["measurement_s"]
# Failure injection occurs halfway through the measurement window so that
# both pre- and post-failure observation are equal length, regardless of
# how the run length is rescaled in experiment_matrix.py.
_FAILURE_DELAY_S = _MEASUREMENT_S // 2

# ---------------------------------------------------------------------------
# Strategies (subset of run_market.py MARKET_CONFIGS)
# ---------------------------------------------------------------------------

STRATEGIES = ["oracle-global", "rr-global", "market-quad"]

STRATEGY_CONFIG: dict[str, dict] = {
    "oracle-global": {
        "placement_mode": "neural",
        "governance_config": "all",
        "oracle_mode": True,
    },
    "rr-global": {
        "placement_mode": "neural",
        "governance_config": "all",
        "oracle_mode": True,
        "broker_module": "src.broker.static_broker",
        "placement": "round_robin",
    },
    "market-quad": {
        "placement_mode": "market",
        "governance_config": "all",
        # Oracle mode: single broker on VM1 with all 48 workers visible.
        # This isolates the PRICING mechanism from the FEDERATION plumbing.
        # H-RR-HETERO tests whether market clearing prices discover fast
        # workers (Walrasian price discovery), not whether federation
        # forwarding routes correctly — that's tested by H-FEDERATION.
        "oracle_mode": True,
    },
}

# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

PIPELINE_MAP: dict[str, str] = {
    "cqi-chain": "cqi_chain",
    "anomaly-sp": "anomaly_sp",
    "ran-entangled": "ran_entangled",
}
PIPELINE_SLUGS = list(PIPELINE_MAP)

# Scenario timing values (warmup_s, measurement_s, failure_delay_s) are
# inherited from EXPERIMENTS["ablation"] in scripts.experiment_matrix so
# rescaling the main campaign automatically rescales the ablation. Each
# scenario only owns scenario-specific parameters (arrival rate, failure
# target, speed factors).
# Failure factorial: 3 loads × 2 kill ratios = 6 scenarios.
# 12-kill = all VM2 workers (25% capacity). 24-kill = VM1 + VM2 (50%, both edge VMs).
_VM2_WORKERS = [f"deploy-worker-0-{i}" for i in range(12)]
_VM1_WORKERS = [f"deploy-worker-0-{i}" for i in range(12)]  # same names, different host

def _failure_scenario(load: float, kill_count: int) -> dict:
    """Build a failure scenario config.

    kill_count == 12 → single-VM kill (VM2 only) via failure_target list.
    kill_count == 24 → multi-VM kill (VM1 + VM2) via failure_targets dict list.
    """
    base = {
        "arrival_rate": load,
        "failure_delay_s": _FAILURE_DELAY_S,
        "warmup_s": _WARMUP_S,
        "measurement_s": _MEASUREMENT_S,
    }
    if kill_count == 12:
        base["description"] = f"Kill 12 workers on VM2 (25% capacity) at {load:.0f} pps"
        base["failure_target"] = list(_VM2_WORKERS)
        base["failure_vm_index"] = 1  # VM2
    elif kill_count == 24:
        base["description"] = f"Kill 24 workers on VM1+VM2 (50% capacity) at {load:.0f} pps"
        base["failure_targets"] = [
            {"vm_idx": 0, "containers": list(_VM1_WORKERS)},
            {"vm_idx": 1, "containers": list(_VM2_WORKERS)},
        ]
    else:
        raise ValueError(f"Unsupported kill_count {kill_count}")
    return base


SCENARIOS: dict[str, dict] = {
    "failure-50-12": _failure_scenario(50.0, 12),
    "failure-100-12": _failure_scenario(100.0, 12),
    "failure-150-12": _failure_scenario(150.0, 12),
    "failure-50-24": _failure_scenario(50.0, 24),
    "failure-100-24": _failure_scenario(100.0, 24),
    "failure-150-24": _failure_scenario(150.0, 24),
    "sat-100": {
        "description": "Near-saturation (100 pps, ~47% util with 48 workers)",
        "arrival_rate": 100.0,
        "warmup_s": _WARMUP_S,
        "measurement_s": _MEASUREMENT_S,
    },
    "sat-150": {
        "description": "At-saturation (150 pps, ~70% util)",
        "arrival_rate": 150.0,
        "warmup_s": _WARMUP_S,
        "measurement_s": _MEASUREMENT_S,
    },
    "sat-200": {
        "description": "Above-saturation (200 pps, ~94% util)",
        "arrival_rate": 200.0,
        "warmup_s": _WARMUP_S,
        "measurement_s": _MEASUREMENT_S,
    },
    "heterogeneous": {
        "description": "Edge VMs 2x slower; cloud VMs 1.5x faster",
        "arrival_rate": 5.0,
        "speed_factors": {
            "vm1": 2.0,    # edge: slow
            "vm2": 2.0,    # edge: slow
            "vm3": 0.67,   # cloud: fast
            "vm4": 0.67,   # cloud: fast
        },
        "warmup_s": _WARMUP_S,
        "measurement_s": _MEASUREMENT_S,
    },
}


@dataclass
class AblationRunConfig:
    """A single ablation run."""
    scenario_name: str
    strategy: str
    pipeline_type: str    # internal name (e.g. "cqi_chain")
    seed: int
    arrival_rate: float
    warmup_s: int
    measurement_s: int

    @property
    def run_id(self) -> str:
        slug = next((k for k, v in PIPELINE_MAP.items() if v == self.pipeline_type),
                    self.pipeline_type)
        return f"{self.scenario_name}_{self.strategy}_{slug}_seed-{self.seed}"


def build_run_matrix(
    scenarios: list[str],
    strategies: list[str],
    seeds: list[int],
    pipelines: list[str] | None = None,
    **_kwargs,
) -> list[AblationRunConfig]:
    """Build the ablation matrix: scenarios x strategies x pipelines x seeds."""
    pipe_internal = (
        [PIPELINE_MAP[p] for p in pipelines]
        if pipelines is not None
        else list(PIPELINE_MAP.values())
    )
    runs: list[AblationRunConfig] = []
    for scenario_name, strat, pipe_name, seed in itertools.product(
        scenarios, strategies, pipe_internal, seeds,
    ):
        cfg = SCENARIOS[scenario_name]
        runs.append(AblationRunConfig(
            scenario_name=scenario_name,
            strategy=strat,
            pipeline_type=pipe_name,
            seed=seed,
            arrival_rate=cfg["arrival_rate"],
            warmup_s=cfg["warmup_s"],
            measurement_s=cfg["measurement_s"],
        ))
    return runs


def _run_distributed(run: AblationRunConfig, dry_run: bool) -> dict:
    """Execute an ablation run on the 4-VM cluster with the ablation compose."""
    from functools import partial
    from scripts import multi_vm_runner

    scenario = SCENARIOS[run.scenario_name]
    strat_cfg = STRATEGY_CONFIG[run.strategy]

    # Failure injection setup
    failure_fn = None
    if "failure_target" in scenario:
        # Single-VM kill (12 workers on one VM)
        vm_idx = scenario["failure_vm_index"]
        failure_fn = partial(
            multi_vm_runner.inject_remote_kill,
            vm=multi_vm_runner.VMS[vm_idx],
            container=scenario["failure_target"],
            delay_s=scenario["failure_delay_s"],
        )
    elif "failure_targets" in scenario:
        # Multi-VM kill (24 workers spanning two VMs).
        # All targets fire concurrently after the same delay so the
        # capacity loss is simultaneous, not staggered.
        targets = scenario["failure_targets"]
        delay_s = scenario["failure_delay_s"]

        def _multi_vm_kill():
            import threading
            threads = []
            for t in targets:
                vm = multi_vm_runner.VMS[t["vm_idx"]]
                th = threading.Thread(
                    target=multi_vm_runner.inject_remote_kill,
                    kwargs={
                        "vm": vm,
                        "container": t["containers"],
                        "delay_s": delay_s,
                    },
                    daemon=True,
                )
                th.start()
                threads.append(th)
            for th in threads:
                th.join()

        failure_fn = _multi_vm_kill

    # Heterogeneous capacity: per-VM WORKER_PROCESSING_SPEED env var
    per_vm_env = None
    if "speed_factors" in scenario:
        per_vm_env = {
            vm: {"WORKER_PROCESSING_SPEED": str(speed)}
            for vm, speed in scenario["speed_factors"].items()
        }

    result = multi_vm_runner.run_single(
        config=f"{run.scenario_name}_{run.strategy}",
        run_id=run.run_id,
        seed=run.seed,
        placement_mode=strat_cfg["placement_mode"],
        governance_config=strat_cfg["governance_config"],
        broker_module=strat_cfg.get("broker_module"),
        placement=strat_cfg.get("placement"),
        per_vm_env=per_vm_env,
        compose_file=COMPOSE_FILE,
        workload_env={
            "PIPELINE_TYPE": run.pipeline_type,
            "ARRIVAL_RATE": str(run.arrival_rate),
        },
        results_subdir="ablation",
        warmup_s=run.warmup_s,
        measurement_s=run.measurement_s,
        failure_fn=failure_fn,
        wan_emulation=True,
        oracle_mode=strat_cfg.get("oracle_mode", False),
        dry_run=dry_run,
    )
    # Propagate failure from run_single (e.g. federation timeout)
    if result and result.get("status") == "failed":
        return result
    return {
        "run_id": run.run_id,
        "status": "completed" if not dry_run else "dry_run",
        "result_file": f"results/ablation/{run.run_id}",
    }


def _run(run: AblationRunConfig, dry_run: bool, **kwargs) -> dict:
    """Dispatch a single ablation run."""
    topology = kwargs.get("topology", "local")
    if topology != "distributed":
        logger.warning(
            "Ablation experiments require 4-VM topology. "
            "Use --topology distributed for real runs."
        )
    return _run_distributed(run, dry_run)


def _extra_args(parser):
    """Phase-specific CLI args. --configs is reused as the scenario filter."""
    parser.add_argument(
        "--strategies", type=str, default=None,
        help="Comma-separated strategies (default: all). "
             "Valid: " + ",".join(STRATEGIES),
    )
    parser.add_argument(
        "--pipelines", type=str, default=None,
        help="Comma-separated pipelines (default: all). "
             "Valid: " + ",".join(PIPELINE_SLUGS),
    )


def _parse_extra(args):
    result: dict = {}
    if args.strategies:
        result["strategies"] = [s.strip() for s in args.strategies.split(",")]
    else:
        result["strategies"] = list(STRATEGIES)
    if args.pipelines:
        result["pipelines"] = [p.strip() for p in args.pipelines.split(",")]
    return result


def main():
    # phase_main expects configs as a dict; we use scenarios as the
    # phase_main "configs" since they parameterise the matrix.
    phase_main(
        phase_name="Ablation",
        description="Stress-scenario ablation (failure/saturation/heterogeneous)",
        configs=SCENARIOS,
        build_matrix_fn=_build_matrix_adapter,
        run_fn=_run,
        results_dir=RESULTS_DIR,
        extra_args_fn=_extra_args,
        parse_extra_fn=_parse_extra,
        default_seeds=DEFAULT_SEEDS,
    )


def _build_matrix_adapter(configs, seeds, **extra):
    """Adapt phase_main's (configs, seeds) interface to build_run_matrix.

    phase_main passes the user's --configs as the first argument; we
    treat those as scenario names. The strategies and pipelines come
    from --strategies / --pipelines (parsed via _parse_extra).
    """
    return build_run_matrix(
        scenarios=configs,
        strategies=extra.get("strategies", list(STRATEGIES)),
        seeds=seeds,
        pipelines=extra.get("pipelines"),
    )


if __name__ == "__main__":
    main()
