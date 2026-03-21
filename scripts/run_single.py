#!/usr/bin/env python3
"""Ad-hoc single experiment run.

Usage:
    python scripts/run_single.py A2 medium 3 42
    python scripts/run_single.py B3 low 5 123 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys

from scripts._common import (
    PROJECT_ROOT,
    resolve_config,
    run_single,
)

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a single experiment configuration",
    )
    parser.add_argument("config", help="Config name (A1, A2, ..., D4)")
    parser.add_argument("rate", help="Rate label (low, medium, high) or numeric")
    parser.add_argument("stages", type=int, help="Pipeline complexity (stages)")
    parser.add_argument("seed", type=int, help="Random seed")
    parser.add_argument("--dry-run", action="store_true", help="Print without executing")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))

    cfg = resolve_config(args.config, rate=args.rate, stages=args.stages, seed=args.seed)

    run_id = f"{args.config}_rate-{args.rate}_stages-{args.stages}_seed-{args.seed}"

    # Determine results directory from config prefix
    phase_letter = args.config[0].lower()
    phase_map = {"a": "phase_a", "b": "phase_b", "c": "phase_c", "d": "phase_d"}
    phase_dir = phase_map.get(phase_letter, "misc")
    results_dir = PROJECT_ROOT / "results" / phase_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    # Merge resolved env with duration info
    env = dict(cfg.env)
    warmup_s = 120
    measurement_s = 600
    total_duration = warmup_s + measurement_s
    env.setdefault("DURATION_S", str(total_duration))
    env.setdefault("WARMUP_S", str(warmup_s))

    logger.info(
        "Single run: %s (compose_files=%s, env keys=%s)",
        run_id,
        [f.name for f in cfg.compose_files],
        list(env.keys()),
    )

    result = run_single(
        run_id=run_id,
        env=env,
        results_dir=results_dir,
        total_duration=total_duration,
        dry_run=args.dry_run,
        compose_files=cfg.compose_files,
    )

    status = result["status"]
    logger.info("Result: %s → %s", run_id, status)

    if status not in ("completed", "dry_run"):
        sys.exit(1)


if __name__ == "__main__":
    main()
