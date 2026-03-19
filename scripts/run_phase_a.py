#!/usr/bin/env python3
"""Phase A: Single-site baselines.

Runs 4 configurations on a single domain (no federation):
  A1 -- Kafka + static topic routing (centralised broker baseline)
  A2 -- Static placement (fixed pipeline-to-node mapping)
  A3 -- Random placement (lower bound)
  A4 -- Neural Pub/Sub (single broker, dynamic semantic routing)

Per configuration:
  5 seeds x 3 workload rates (low/medium/high) x 3 pipeline complexities
  (2/3/5 stages). Each run: 10-min warm-up, 30-min measurement window.

Outputs CSV results to results/phase_a/.

Usage:
    python scripts/run_phase_a.py [--dry-run] [--seeds 42,123,456,789,0]
    python scripts/run_phase_a.py --configs A4 --rates medium --seeds 42
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    PROJECT_ROOT,
    phase_main,
    run_single,
)

KAFKA_COMPOSE_FILE = PROJECT_ROOT / "docker-compose.kafka.yaml"

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_a"

# Rate presets
RATES = {
    "low": 1.0,
    "medium": 5.0,
    "high": 20.0,
}

# Pipeline complexity presets (used to select pipeline mix)
COMPLEXITIES = {
    2: {"cqi_prediction": 1.0, "anomaly_detection": 0.0, "sensor_fusion": 0.0},
    3: {"cqi_prediction": 0.5, "anomaly_detection": 0.5, "sensor_fusion": 0.0},
    5: {"cqi_prediction": 0.0, "anomaly_detection": 0.0, "sensor_fusion": 1.0},
}

# Config definitions
CONFIGS = {
    "A1": {"placement_strategy": "kafka"},
    "A2": {"placement_strategy": "static"},
    "A3": {"placement_strategy": "random"},
    "A4": {"placement_strategy": "neural"},
}


@dataclass
class RunConfig:
    """A single Phase A run configuration."""
    config_name: str
    rate_label: str
    arrival_rate: float
    pipeline_complexity: int
    seed: int
    warmup_s: int = 600
    measurement_s: int = 1800
    placement_strategy: str = "neural"


def _add_extra_args(parser):
    parser.add_argument(
        "--rates", default="low,medium,high",
        help="Comma-separated rate labels (default: low,medium,high)",
    )
    parser.add_argument(
        "--complexities", default="2,3,5",
        help="Comma-separated pipeline stage counts (default: 2,3,5)",
    )


def _parse_extra(args):
    rates = [r.strip() for r in args.rates.split(",")]
    complexities = [int(c.strip()) for c in args.complexities.split(",")]
    for r in rates:
        if r not in RATES:
            raise SystemExit(f"Unknown rate: {r}. Valid: {list(RATES.keys())}")
    return {"rates": rates, "complexities": complexities}


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    rates: list[str] | None = None,
    complexities: list[int] | None = None,
) -> list[RunConfig]:
    """Build the full combinatorial run matrix."""
    if rates is None:
        rates = list(RATES.keys())
    if complexities is None:
        complexities = list(COMPLEXITIES.keys())
    runs = []
    for config_name, rate_label, complexity, seed in itertools.product(
        configs, rates, complexities, seeds,
    ):
        cfg = CONFIGS[config_name]
        runs.append(RunConfig(
            config_name=config_name,
            rate_label=rate_label,
            arrival_rate=RATES[rate_label],
            pipeline_complexity=complexity,
            seed=seed,
            placement_strategy=cfg["placement_strategy"],
        ))
    return runs


def _run(run: RunConfig, dry_run: bool) -> dict:
    run_id = (
        f"{run.config_name}_rate-{run.rate_label}_"
        f"stages-{run.pipeline_complexity}_seed-{run.seed}"
    )
    total_duration = run.warmup_s + run.measurement_s
    mix = COMPLEXITIES[run.pipeline_complexity]

    logger.info(
        "Run: %s (rate=%.1f, stages=%d, seed=%d, strategy=%s, duration=%ds)",
        run_id, run.arrival_rate, run.pipeline_complexity, run.seed,
        run.placement_strategy, total_duration,
    )

    env = {
        "PLACEMENT_STRATEGY": run.placement_strategy,
        "ARRIVAL_RATE": str(run.arrival_rate),
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "PIPELINE_MIX_CQI": str(mix["cqi_prediction"]),
        "PIPELINE_MIX_ANOMALY": str(mix["anomaly_detection"]),
        "PIPELINE_MIX_FUSION": str(mix["sensor_fusion"]),
        "WARMUP_S": str(run.warmup_s),
    }

    # A1 (Kafka): use kafka_broker via docker-compose.kafka.yaml overlay
    # A2 (static): use static_broker with round_robin placement
    # A3 (random): use static_broker with random placement
    # A4 (neural): use neural_broker (default)
    compose_files = None
    if run.placement_strategy == "kafka":
        compose_files = [COMPOSE_FILE, KAFKA_COMPOSE_FILE]
    elif run.placement_strategy == "static":
        env["BROKER_MODULE"] = "src.broker.static_broker"
        env["PLACEMENT"] = "round_robin"
    elif run.placement_strategy == "random":
        env["BROKER_MODULE"] = "src.broker.static_broker"
        env["PLACEMENT"] = "random"

    return run_single(
        run_id=run_id,
        env=env,
        results_dir=RESULTS_DIR,
        total_duration=total_duration,
        dry_run=dry_run,
        compose_files=compose_files,
    )


def main():
    phase_main(
        phase_name="Phase A",
        description="Phase A: Single-site baselines",
        configs=CONFIGS,
        build_matrix_fn=build_run_matrix,
        run_fn=_run,
        results_dir=RESULTS_DIR,
        extra_args_fn=_add_extra_args,
        parse_extra_fn=_parse_extra,
    )


if __name__ == "__main__":
    main()
