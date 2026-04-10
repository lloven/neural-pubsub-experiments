#!/usr/bin/env python3
"""Re-mark phantom-done entries in a phase's .progress.json as queued.

A "phantom done" entry is a record whose status is `done` but whose
corresponding result CSV does not exist on disk. This happens when:

  1. A previous run was marked done by the runner but crashed before
     writing the CSV.
  2. The CSV was renamed to `.csv.old` after a bug fix invalidated it
     (the user did this manually for the oracle anomaly-sp runs after
     the DP colocation fix).
  3. The runner's `_discover_completed_runs` re-marked entries as done
     from a stale progress file even though the CSVs were gone.

With `--resume` enabled, the runner skips these phantom entries forever,
so the affected runs are never re-executed. This utility detects them
and re-marks them as `queued` so a subsequent `--resume` will pick them
up.

The script is dry-run by default. Pass `--apply` to actually mutate the
progress file (a `.json.bak` backup is created next to the original).

Usage::

    # Inspect what would be fixed (no mutation):
    python -m scripts.fix_phantom_done results/market/.progress.json

    # Apply the fix:
    python -m scripts.fix_phantom_done results/market/.progress.json --apply

    # Restrict to a specific run-id pattern (substring match):
    python -m scripts.fix_phantom_done results/market/.progress.json \\
        --filter oracle-global --apply
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def find_phantom_done(progress_path: Path) -> list[str]:
    """Return run_ids whose status is done but whose .csv is missing.

    The .csv is expected to live next to the progress file with the
    name ``<run_id>.csv`` (matching the convention used by
    ``run_market.py``, ``run_ablation.py``, and other phase runners).

    Args:
        progress_path: Path to the .progress.json file.

    Returns:
        Sorted list of phantom run_ids (entries that need re-queuing).
    """
    progress_path = Path(progress_path)
    if not progress_path.exists():
        raise FileNotFoundError(f"Progress file does not exist: {progress_path}")

    data = json.loads(progress_path.read_text())
    results_dir = progress_path.parent

    phantoms: list[str] = []
    for run_id, entry in data.items():
        if entry.get("status") != "done":
            continue
        csv_path = results_dir / f"{run_id}.csv"
        if not csv_path.exists():
            phantoms.append(run_id)

    return sorted(phantoms)


def fix_phantom_done(
    progress_path: Path,
    *,
    apply: bool = False,
    filter_substr: str | None = None,
) -> list[str]:
    """Detect and (optionally) re-mark phantom-done entries as queued.

    Args:
        progress_path: Path to the .progress.json file.
        apply: When True, mutate the file (creates a .json.bak backup).
            When False (default), only return the list of phantoms
            without changing the file.
        filter_substr: When set, restrict the fix to run_ids containing
            this substring. Useful for scoping a re-run to a specific
            strategy / pipeline / scenario.

    Returns:
        List of phantom run_ids found (and re-queued, if apply=True).
    """
    progress_path = Path(progress_path)
    phantoms = find_phantom_done(progress_path)

    if filter_substr:
        phantoms = [p for p in phantoms if filter_substr in p]

    if not phantoms or not apply:
        return phantoms

    # Backup before mutation
    backup = progress_path.with_suffix(".json.bak")
    backup.write_text(progress_path.read_text())

    data = json.loads(progress_path.read_text())
    for run_id in phantoms:
        # Preserve other fields; only flip status to queued.
        data[run_id]["status"] = "queued"

    progress_path.write_text(json.dumps(data, indent=2))
    logger.info(
        "Re-marked %d phantom-done entries as queued in %s (backup: %s)",
        len(phantoms), progress_path, backup,
    )
    return phantoms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "progress_path",
        type=Path,
        help="Path to the .progress.json file (e.g. results/market/.progress.json).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually mutate the file. Without this flag, the script "
             "is dry-run and only prints the affected entries.",
    )
    parser.add_argument(
        "--filter",
        dest="filter_substr",
        default=None,
        help="Restrict the fix to run_ids containing this substring "
             "(e.g. 'oracle-global').",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(name)s: %(message)s",
    )

    phantoms = fix_phantom_done(
        args.progress_path,
        apply=args.apply,
        filter_substr=args.filter_substr,
    )

    if not phantoms:
        print(f"No phantom-done entries found in {args.progress_path}")
        return 0

    action = "Re-queued" if args.apply else "Would re-queue"
    print(f"{action} {len(phantoms)} phantom-done entries:")
    for run_id in phantoms:
        print(f"  - {run_id}")

    if not args.apply:
        print()
        print("Re-run with --apply to actually modify the file.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
