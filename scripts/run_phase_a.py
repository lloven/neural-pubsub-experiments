#!/usr/bin/env python3
"""Phase A: Single-site baselines (dual-transport factorial).

Runs 3 placement strategies × 2 transports on a single domain (no federation):
  A2 -- Static round-robin placement
  A3 -- Random placement (lower bound)
  A4 -- Neural Pub/Sub (single broker, dynamic semantic routing)

Each runs under both HTTP and Kafka transport, creating a 3×2 factorial.

Per cell: 5 seeds × 3 rates (low/medium/high) × 1 complexity (3 stages).
  Each run: 10-min warm-up, 30-min measurement window.

Outputs CSV results to results/phase_a/.

Usage:
    python scripts/run_phase_a.py [--dry-run] [--seeds 42,123,456,789,0]
    python scripts/run_phase_a.py --configs A4 --rates medium --seeds 42
    python scripts/run_phase_a.py --transports kafka --configs A2,A3,A4
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from pathlib import Path

from scripts._common import (
    COMPOSE_FILE,
    COMPOSE_KAFKA,
    PROJECT_ROOT,
    TRANSPORTS,
    phase_main,
    resolve_config,
    run_single,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_a"

# Rate presets
RATES = {
    "low": 2.0,
    "medium": 5.0,
    "high": 10.0,
}

# Pipeline complexity presets (used to select pipeline mix)
COMPLEXITIES = {
    2: {"cqi_prediction": 1.0, "anomaly_detection": 0.0, "sensor_fusion": 0.0},
    3: {"cqi_prediction": 0.5, "anomaly_detection": 0.5, "sensor_fusion": 0.0},
    5: {"cqi_prediction": 0.0, "anomaly_detection": 0.0, "sensor_fusion": 1.0},
}

# Config definitions (renumbered: A1=round-robin, A2=random, A3=neural)
CONFIGS = {
    "A1": {"placement_strategy": "static"},
    "A2": {"placement_strategy": "random"},
    "A3": {"placement_strategy": "neural"},
}


@dataclass
class RunConfig:
    """A single Phase A run configuration."""
    config_name: str
    rate_label: str
    arrival_rate: float
    pipeline_complexity: int
    seed: int
    transport: str = "http"
    warmup_s: int = 120
    measurement_s: int = 600
    placement_strategy: str = "neural"

    @property
    def run_id(self) -> str:
        return f"{self.config_name}_{self.transport}_rate-{self.rate_label}_stages-{self.pipeline_complexity}_seed-{self.seed}"


def _add_extra_args(parser):
    parser.add_argument(
        "--rates", default="low,medium,high",
        help="Comma-separated rate labels (default: low,medium,high)",
    )
    parser.add_argument(
        "--complexities", default="3",
        help="Comma-separated pipeline stage counts (default: 3)",
    )
    parser.add_argument(
        "--transports", default="http,kafka",
        help="Comma-separated transport modes (default: http,kafka)",
    )


def _parse_extra(args):
    rates = [r.strip() for r in args.rates.split(",")]
    complexities = [int(c.strip()) for c in args.complexities.split(",")]
    transports = [t.strip() for t in args.transports.split(",")]
    for r in rates:
        if r not in RATES:
            raise SystemExit(f"Unknown rate: {r}. Valid: {list(RATES.keys())}")
    for t in transports:
        if t not in TRANSPORTS:
            raise SystemExit(f"Unknown transport: {t}. Valid: {TRANSPORTS}")
    return {"rates": rates, "complexities": complexities, "transports": transports}


def build_run_matrix(
    configs: list[str],
    seeds: list[int],
    rates: list[str] | None = None,
    complexities: list[int] | None = None,
    transports: list[str] | None = None,
) -> list[RunConfig]:
    """Build Phase A run matrix: core factorial + rate sensitivity.

    Core factorial (medium rate): all configs × all transports × all seeds
      → proves transport orthogonality and compares placement strategies.
    Rate sensitivity (A3/http only): all rates × all seeds
      → shows Neural Pub/Sub handles different loads.

    This avoids the full Cartesian product (which would be 90+ runs).
    """
    if rates is None:
        rates = list(RATES.keys())
    if complexities is None:
        complexities = list(COMPLEXITIES.keys())
    if transports is None:
        transports = TRANSPORTS

    runs = []
    seen = set()

    # Core factorial: all configs × all transports × medium rate only
    for config_name, transport, complexity, seed in itertools.product(
        configs, transports, complexities, seeds,
    ):
        cfg = CONFIGS[config_name]
        rc = RunConfig(
            config_name=config_name,
            rate_label="medium",
            arrival_rate=RATES["medium"],
            pipeline_complexity=complexity,
            seed=seed,
            transport=transport,
            placement_strategy=cfg["placement_strategy"],
        )
        if rc.run_id not in seen:
            runs.append(rc)
            seen.add(rc.run_id)

    # Rate sensitivity: A3 (neural) × http × all rates × all seeds
    neural_config = "A3"
    if neural_config in configs:
        cfg = CONFIGS[neural_config]
        for rate_label, complexity, seed in itertools.product(
            rates, complexities, seeds,
        ):
            rc = RunConfig(
                config_name=neural_config,
                rate_label=rate_label,
                arrival_rate=RATES[rate_label],
                pipeline_complexity=complexity,
                seed=seed,
                transport="http",
                placement_strategy=cfg["placement_strategy"],
            )
            if rc.run_id not in seen:
                runs.append(rc)
                seen.add(rc.run_id)

    return runs


def _run(run: RunConfig, dry_run: bool) -> dict:
    run_id = run.run_id
    total_duration = run.warmup_s + run.measurement_s
    mix = COMPLEXITIES[run.pipeline_complexity]

    logger.info(
        "Run: %s (rate=%.1f, stages=%d, seed=%d, strategy=%s, transport=%s, duration=%ds)",
        run_id, run.arrival_rate, run.pipeline_complexity, run.seed,
        run.placement_strategy, run.transport, total_duration,
    )

    # Use resolve_config for compose files (handles transport overlay)
    resolved = resolve_config(
        run.config_name, rate=run.rate_label, stages=run.pipeline_complexity,
        seed=run.seed, transport=run.transport,
    )

    env = {
        **resolved.env,
        "ARRIVAL_RATE": str(run.arrival_rate),
        "DURATION_S": str(total_duration),
        "SEED": str(run.seed),
        "PIPELINE_MIX_CQI": str(mix["cqi_prediction"]),
        "PIPELINE_MIX_ANOMALY": str(mix["anomaly_detection"]),
        "PIPELINE_MIX_FUSION": str(mix["sensor_fusion"]),
        "WARMUP_S": str(run.warmup_s),
    }

    compose_files = resolved.compose_files

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
