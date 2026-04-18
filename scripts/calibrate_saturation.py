#!/usr/bin/env python3
"""Saturation-point calibration sweep for the 4-VM / 48-worker testbed.

Purpose
-------
After the L53 (arrival-rate plumbing) fix on 2026-04-18, the previous
theoretical saturation estimate of ~200 pps turned out to be off by ~40x:
oracle-global at 50 pps already fails with "worker died" errors in smoke
tests. The real saturation point lies somewhere between 5 pps (clean 100%
completion) and 50 pps (total collapse).

This script sweeps rates 5, 10, 15, 20, 30, 50 pps with oracle-global +
cqi-chain (the longest, most stage-work pipeline and therefore the most
sensitive to saturation), one seed per rate, 30 s warmup + 60 s measurement.
The resulting (rate, CR, p95 latency) triples are used to redesign the
sat-* and failure-* scenarios in run_ablation.py.

Usage
-----
    python3 -m scripts.calibrate_saturation \
        [--topology distributed] \
        [--rates 5,10,15,20,30,50] \
        [--pipeline cqi-chain] \
        [--strategy oracle-global] \
        [--seed 99] [--warmup 30] [--measurement 60]

Output: CSVs in results/calibration/ plus a stdout summary table.
"""

from __future__ import annotations

import argparse
import csv
import logging
import statistics
import sys
from pathlib import Path

# Make `scripts` importable when run from repo root as a module (-m).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts import multi_vm_runner
from scripts.run_ablation import PIPELINE_MAP, STRATEGY_CONFIG, COMPOSE_FILE

logger = logging.getLogger(__name__)


def _run_rate(
    *,
    rate: float,
    pipeline_slug: str,
    strategy: str,
    seed: int,
    warmup_s: int,
    measurement_s: int,
    dry_run: bool,
) -> str:
    """Run one calibration point. Returns the path to the result CSV."""
    strat = STRATEGY_CONFIG[strategy]
    pipeline_internal = PIPELINE_MAP[pipeline_slug]
    run_id = f"calib-{rate:g}pps_{strategy}_{pipeline_slug}_seed-{seed}"
    result_file = f"results/calibration/{run_id}.csv"

    logger.info("=== Calibrating at %g pps (%s, %s) ===", rate, strategy, pipeline_slug)
    multi_vm_runner.run_single(
        config=f"calib-{rate:g}pps_{strategy}",
        run_id=run_id,
        seed=seed,
        placement_mode=strat["placement_mode"],
        governance_config=strat["governance_config"],
        broker_module=strat.get("broker_module"),
        placement=strat.get("placement"),
        compose_file=COMPOSE_FILE,
        workload_env={
            "PIPELINE_TYPE": pipeline_internal,
            "ARRIVAL_RATE": str(rate),
        },
        results_subdir="calibration",
        warmup_s=warmup_s,
        measurement_s=measurement_s,
        wan_emulation=True,
        oracle_mode=strat.get("oracle_mode", False),
        dry_run=dry_run,
    )
    return result_file


def _summarize(csv_path: Path) -> dict:
    """Return summary stats for one calibration CSV.

    Excludes pipelines submitted during warmup. Computes completion rate
    and p95 / p99 / median latency over successful pipelines.
    """
    stats = {
        "n_total": 0,
        "n_warmup": 0,
        "n_post_warmup": 0,
        "n_success": 0,
        "cr": 0.0,
        "median_ms": 0.0,
        "p95_ms": 0.0,
        "p99_ms": 0.0,
    }
    if not csv_path.exists():
        return stats
    latencies: list[float] = []
    with open(csv_path) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            stats["n_total"] += 1
            is_warmup = row.get("warmup", "").lower() == "true"
            if is_warmup:
                stats["n_warmup"] += 1
                continue
            stats["n_post_warmup"] += 1
            if row.get("success", "").lower() == "true":
                stats["n_success"] += 1
                try:
                    lat = float(row.get("e2e_latency_ms") or 0)
                    if lat > 0:
                        latencies.append(lat)
                except ValueError:
                    pass
    if stats["n_post_warmup"] > 0:
        stats["cr"] = stats["n_success"] / stats["n_post_warmup"]
    if latencies:
        latencies.sort()
        stats["median_ms"] = statistics.median(latencies)
        k95 = int(round(0.95 * (len(latencies) - 1)))
        k99 = int(round(0.99 * (len(latencies) - 1)))
        stats["p95_ms"] = latencies[k95]
        stats["p99_ms"] = latencies[k99]
    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--topology", default="distributed",
                        choices=["distributed", "local"])
    parser.add_argument("--rates", default="5,10,15,20,30,50",
                        help="Comma-separated arrival rates in pps.")
    parser.add_argument("--pipeline", default="cqi-chain",
                        choices=list(PIPELINE_MAP.keys()))
    parser.add_argument("--strategy", default="oracle-global",
                        choices=list(STRATEGY_CONFIG.keys()))
    parser.add_argument("--seed", type=int, default=99)
    parser.add_argument("--warmup", type=int, default=30,
                        dest="warmup_s")
    parser.add_argument("--measurement", type=int, default=60,
                        dest="measurement_s")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.topology != "distributed":
        logger.error("Calibration requires --topology distributed (4-VM cluster).")
        return 1

    rates = [float(r) for r in args.rates.split(",")]
    results: list[tuple[float, dict]] = []
    for rate in rates:
        csv_file = _run_rate(
            rate=rate,
            pipeline_slug=args.pipeline,
            strategy=args.strategy,
            seed=args.seed,
            warmup_s=args.warmup_s,
            measurement_s=args.measurement_s,
            dry_run=args.dry_run,
        )
        summary = {} if args.dry_run else _summarize(
            Path(multi_vm_runner.REMOTE_PROJECT_DIR).expanduser() / csv_file
            if not Path(csv_file).is_absolute() else Path(csv_file)
        )
        # Path resolution: if running on VM1, the working dir is the project
        # root; fall back to relative path.
        if not summary:
            summary = _summarize(Path(csv_file))
        results.append((rate, summary))

    if args.dry_run:
        return 0

    print()
    print("=== Saturation calibration results ===")
    print(f"strategy={args.strategy}  pipeline={args.pipeline}  seed={args.seed}  "
          f"warmup={args.warmup_s}s  measurement={args.measurement_s}s")
    print()
    header = ("rate_pps", "n_post_warmup", "CR", "median_ms", "p95_ms", "p99_ms")
    print(f"{header[0]:>9}  {header[1]:>14}  {header[2]:>6}  "
          f"{header[3]:>10}  {header[4]:>8}  {header[5]:>8}")
    print("-" * 68)
    for rate, s in results:
        print(f"{rate:>9.1f}  {s['n_post_warmup']:>14d}  "
              f"{s['cr']*100:>5.1f}%  {s['median_ms']:>10.0f}  "
              f"{s['p95_ms']:>8.0f}  {s['p99_ms']:>8.0f}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
