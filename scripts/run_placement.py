#!/usr/bin/env python3
"""Placement: Placement quality micro-benchmark.

Runs tests/test_placement_quality.py to evaluate the placement algorithm's
optimality gap on small topologies where brute-force is feasible. Outputs a
CSV with columns: topology, pipeline_type, algorithm_cost, optimal_cost,
gap_ratio, constraint_violations.

Usage:
    python -m scripts.run_placement [--dry-run]
"""

from __future__ import annotations

import csv
import logging
import subprocess
import sys
from pathlib import Path

from scripts._common import PROJECT_ROOT
from tests.test_placement_quality import SCENARIO_NAMES

logger = logging.getLogger(__name__)

RESULTS_DIR = PROJECT_ROOT / "results" / "placement"


def run_placement(dry_run: bool = False) -> Path:
    """Run the placement quality test suite and export results as CSV.

    Returns the path to the output CSV file.
    """
    output_csv = RESULTS_DIR / "placement_quality.csv"
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


def main():
    """Run placement quality benchmark."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Placement: Placement algorithm quality benchmark"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print runs without executing")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    logger.info("=" * 60)
    logger.info("Placement: Placement algorithm quality benchmark")
    logger.info("=" * 60)
    csv_path = run_placement(dry_run=args.dry_run)
    logger.info("Placement output: %s", csv_path)
    logger.info("Done.")


if __name__ == "__main__":
    main()
