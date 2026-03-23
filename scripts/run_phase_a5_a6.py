#!/usr/bin/env python3
"""Phase A.5 & A.6: Placement quality micro-benchmark and resource contention.

Phase A.5 (placement quality):
  Runs tests/test_placement_quality.py to evaluate the placement algorithm's
  optimality gap on small topologies where brute-force is feasible. Outputs a
  CSV with columns: topology, pipeline_type, algorithm_cost, optimal_cost,
  gap_ratio, constraint_violations.

Phase A.6 (contention):
  Stresses the system beyond capacity to measure graceful degradation.

  | Config | Arrival rate        | Workers | Expected behaviour                     |
  |--------|--------------------:|--------:|----------------------------------------|
  | A6.1   | 20/s (2x capacity)  |       5 | Queue buildup, graceful degradation    |
  | A6.2   | 50/s (5x capacity)  |       5 | Saturation, measure failure rate       |
  | A6.3   | 10/s (at capacity)  |   5->3  | Dynamic contention from worker loss    |

  Each config: 2-min warmup + 10-min measurement = 12 min.
  Matrix: 3 configs x 5 seeds = 15 runs. Total: ~3h.

Usage:
    python scripts/run_phase_a5_a6.py --dry-run
    python scripts/run_phase_a5_a6.py --phase a5 --dry-run
    python scripts/run_phase_a5_a6.py --phase a6 --dry-run
    python scripts/run_phase_a5_a6.py --phase a6 --configs A6.1,A6.2 --seeds 42
"""

from __future__ import annotations

import csv
import itertools
import logging
import subprocess
import sys
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
from tests.test_placement_quality import SCENARIO_NAMES

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "phase_a5_a6"

# Failure mode constants
FAILURE_KILL_WORKERS = "kill_2_workers"

# ---------------------------------------------------------------------------
# Phase A.5: Placement quality micro-benchmark
# ---------------------------------------------------------------------------


def run_phase_a5(dry_run: bool = False) -> Path:
    """Run the placement quality test suite and export results as CSV.

    Returns the path to the output CSV file.
    """
    output_csv = RESULTS_DIR / "phase_a5_placement_quality.csv"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if dry_run:
        logger.info("[DRY RUN] Would run: pytest tests/test_placement_quality.py -v")
        logger.info("[DRY RUN] Output CSV: %s", output_csv)
        # Write a header-only CSV so downstream tools can validate the schema
        with open(output_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "topology", "pipeline_type", "algorithm_cost",
                "optimal_cost", "gap_ratio", "constraint_violations",
            ])
        return output_csv

    # Run the test suite using pytest-subprocess capture
    logger.info("Running placement quality benchmark...")
    result = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "tests/test_placement_quality.py",
            "-v", "--tb=short", "-x",
        ],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    logger.info("pytest stdout:\n%s", result.stdout)
    if result.returncode != 0:
        logger.error("pytest stderr:\n%s", result.stderr)
        logger.error("Placement quality tests failed (exit code %d)", result.returncode)

    # Extract results by importing and running the evaluation directly
    # (avoids coupling to pytest output format)
    logger.info("Generating placement quality CSV...")
    try:
        from tests.test_placement_quality import _build_scenario, _evaluate

        scenarios = SCENARIO_NAMES
        rows = []
        for name in scenarios:
            dag, topo, gov, label, ptype = _build_scenario(name)
            res = _evaluate(label, ptype, dag, topo, gov)
            rows.append({
                "topology": res.topology,
                "pipeline_type": res.pipeline_type,
                "algorithm_cost": f"{res.algorithm_cost:.6f}",
                "optimal_cost": f"{res.optimal_cost:.6f}",
                "gap_ratio": f"{res.gap_ratio:.6f}",
                "constraint_violations": res.constraint_violations,
            })

        with open(output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "topology", "pipeline_type", "algorithm_cost",
                "optimal_cost", "gap_ratio", "constraint_violations",
            ])
            writer.writeheader()
            writer.writerows(rows)
        logger.info("Placement quality CSV written to %s", output_csv)
    except Exception:
        logger.exception("Failed to generate placement quality CSV")

    return output_csv


# ---------------------------------------------------------------------------
# Phase A.6: Resource contention
# ---------------------------------------------------------------------------

# Contention configs
CONTENTION_CONFIGS = {
    "A6.1": {
        "arrival_rate": 20.0,
        "n_workers": 5,
        "description": "2x capacity overload",
        "failure": None,
    },
    "A6.2": {
        "arrival_rate": 50.0,
        "n_workers": 5,
        "description": "5x capacity saturation",
        "failure": None,
    },
    "A6.3": {
        "arrival_rate": 10.0,
        "n_workers": 5,
        "description": "At capacity, then kill 2 workers at t=5min",
        "failure": FAILURE_KILL_WORKERS,
    },
}


@dataclass
class ContentionRunConfig:
    """A single Phase A.6 contention run."""

    config_name: str
    arrival_rate: float
    n_workers: int
    seed: int
    failure: str | None = None
    warmup_s: int = 120
    measurement_s: int = 600
    failure_delay_s: int = 300  # 5min from run start; consistent with Phase D


def build_contention_matrix(
    configs: list[str],
    seeds: list[int],
    warmup_s: int | None = None,
    measurement_s: int | None = None,
) -> list[ContentionRunConfig]:
    """Build the Phase A.6 run matrix.  Optional timing overrides for smoke tests."""
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


def _run_contention(run: ContentionRunConfig, dry_run: bool) -> dict:
    """Execute one contention run."""
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


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main():
    """Run Phase A.5 and/or A.6."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Phase A.5 (placement quality) & A.6 (contention)"
    )
    parser.add_argument(
        "--phase", default="all", choices=["all", "a5", "a6"],
        help="Which sub-phase to run (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print runs without executing")
    parser.add_argument(
        "--configs", default=",".join(CONTENTION_CONFIGS.keys()),
        help="Comma-separated A6 config names (default: all)",
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

    # --- Phase A.5 ---
    if args.phase in ("all", "a5"):
        logger.info("=" * 60)
        logger.info("Phase A.5: Placement algorithm quality benchmark")
        logger.info("=" * 60)
        csv_path = run_phase_a5(dry_run=args.dry_run)
        logger.info("Phase A.5 output: %s", csv_path)

    # --- Phase A.6 ---
    if args.phase in ("all", "a6"):
        logger.info("=" * 60)
        logger.info("Phase A.6: Resource contention")
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
        logger.info("Phase A.6: %d runs planned", len(runs))

        if args.dry_run:
            logger.info("[DRY RUN MODE]")

        results = []
        for i, run in enumerate(runs, 1):
            logger.info("--- A.6 Run %d/%d ---", i, len(runs))
            result = _run_contention(run, args.dry_run)
            results.append(result)

        # Write summary CSV
        summary_file = RESULTS_DIR / "phase_a6_summary.csv"
        with open(summary_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["run_id", "status", "result_file"])
            writer.writeheader()
            writer.writerows(results)
        logger.info("Phase A.6 summary: %s", summary_file)

        completed = sum(1 for r in results if r["status"] == "completed")
        failed = sum(1 for r in results if r["status"] not in ("completed", "dry_run"))
        logger.info(
            "Phase A.6 complete: %d/%d runs successful, %d failed",
            completed, len(runs), failed,
        )

    logger.info("Done.")


if __name__ == "__main__":
    main()
