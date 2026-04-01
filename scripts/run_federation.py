#!/usr/bin/env python3
"""Federation: Cross-site federation.

Runs 5 configurations across two federated domains (Tokyo + Oulu):
  static     -- Kafka at each site, static routing (cross-site baseline)
  neural     -- Neural Pub/Sub, federated (2 brokers)
  gov        -- neural + governance (raw radio data stays in domain 1)
  broker-kill -- gov + broker failure at one site
  net-part   -- gov + network partition

Per configuration:
  5 seeds x medium workload.
  Pipeline: CQI prediction where collect+preprocess must stay in domain 1
  (governance), predict can be in either domain.

Outputs CSV results to results/federation/.

Usage:
    python -m scripts.run_federation [--dry-run] [--seeds 42,123,456,789,0]
    python -m scripts.run_federation --configs neural,gov --seeds 42
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
    inject_network_partition,
    phase_main,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "federation"

# Config definitions
CONFIGS = {
    "static": {
        "placement_strategy": "kafka",
        "federation": True,
        "governance": False,
        "broker_failure": False,
        "network_partition": False,
    },
    "neural": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": False,
        "broker_failure": False,
        "network_partition": False,
    },
    "gov": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": True,
        "broker_failure": False,
        "network_partition": False,
    },
    "broker-kill": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": True,
        "broker_failure": True,
        "network_partition": False,
    },
    "net-part": {
        "placement_strategy": "neural",
        "federation": True,
        "governance": True,
        "broker_failure": False,
        "network_partition": True,
    },
}


@dataclass
class RunConfig:
    """A single federation run configuration."""
    config_name: str
    seed: int
    placement_strategy: str = "neural"
    federation: bool = True
    governance: bool = False
    broker_failure: bool = False
    network_partition: bool = False
    failure_delay_s: int = 900
    warmup_s: int = 120
    measurement_s: int = 600

    @property
    def run_id(self) -> str:
        return f"{self.config_name}_rate-medium_seed-{self.seed}"


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
            network_partition=cfg.get("network_partition", False),
        ))
    return runs


def _run_distributed(run: RunConfig, dry_run: bool) -> dict:
    """Execute a federation run on the distributed 4-VM cluster."""
    from scripts import multi_vm_runner
    run_id = run.run_id

    failure_fn = None
    if run.broker_failure:
        failure_fn = partial(
            multi_vm_runner.inject_remote_kill,
            vm=multi_vm_runner.VMS[1],  # VM2 = domain 2
            container="deploy-broker-1",
            delay_s=run.failure_delay_s,
        )
    elif run.network_partition:
        failure_fn = partial(
            multi_vm_runner.inject_remote_partition,
            vm_src=multi_vm_runner.VMS[0],
            vm_dst=multi_vm_runner.VMS[1],
            delay_s=run.failure_delay_s,
        )

    gov_config = "all" if run.governance else "none"

    multi_vm_runner.run_single(
        config=run.config_name,
        run_id=run_id,
        seed=run.seed,
        placement_mode=run.placement_strategy,
        governance_config=gov_config,
        workload_env={
            "PIPELINE_MIX_CQI": "1.0",
            "PIPELINE_MIX_ANOMALY": "0.0",
            "PIPELINE_MIX_FUSION": "0.0",
        },
        results_subdir="federation",
        warmup_s=run.warmup_s,
        measurement_s=run.measurement_s,
        failure_fn=failure_fn,
        wan_emulation=False,
        dry_run=dry_run,
    )
    return {"run_id": run_id, "status": "completed" if not dry_run else "dry_run",
            "result_file": f"results/federation/{run_id}"}


def _run(run: RunConfig, dry_run: bool, **kwargs) -> dict:
    topology = kwargs.get("topology", "local")
    if topology == "distributed":
        return _run_distributed(run, dry_run)

    run_id = f"{run.config_name}_rate-medium_seed-{run.seed}"
    total_duration = run.warmup_s + run.measurement_s

    logger.info(
        "Run: %s (rate=%.1f, seed=%d, strategy=%s, federation=%s, "
        "governance=%s, broker_failure=%s, net_part=%s, duration=%ds)",
        run_id, 5.0, run.seed, run.placement_strategy,
        run.federation, run.governance, run.broker_failure,
        run.network_partition, total_duration,
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

    if run.broker_failure or run.network_partition:
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
    elif run.network_partition:
        failure_fn = partial(
            inject_network_partition,
            project_name=f"npubsub-{run_id}",
            target="federation-net",
            delay_s=run.failure_delay_s,
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
        phase_name="Federation",
        description="Federation: Cross-site federation",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
    )


if __name__ == "__main__":
    main()
