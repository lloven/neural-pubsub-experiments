#!/usr/bin/env python3
"""Live progress monitor for Neural Pub/Sub experiment runs.

Displays:
  - Per-run progress with pipeline completion counts and ETA
  - Docker container health status
  - Overall phase progress with time estimates
  - Recent partial CSV stats (from crash-resilient snapshots)

Usage:
    python scripts/monitor.py                          # monitor results/phase_a
    python scripts/monitor.py results/phase_b          # monitor specific phase
    python scripts/monitor.py --interval 2             # refresh every 2s
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def format_time(seconds: float) -> str:
    if seconds < 0:
        return "--:--"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"


def progress_bar(fraction: float, width: int = 30) -> str:
    filled = int(width * fraction)
    bar = "\u2588" * filled + "\u2591" * (width - filled)
    return f"[{bar}] {fraction * 100:5.1f}%"


def mini_bar(fraction: float, width: int = 15) -> str:
    filled = int(width * fraction)
    bar = "\u2588" * filled + "\u2591" * (width - filled)
    return f"[{bar}]"


def load_progress(results_dir: Path) -> dict:
    pf = results_dir / ".progress.json"
    if pf.exists():
        try:
            return json.loads(pf.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def count_csv_rows(csv_path: Path) -> int:
    """Count data rows in a CSV file (excluding header)."""
    if not csv_path.exists():
        return 0
    try:
        with open(csv_path) as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            return sum(1 for _ in reader)
    except Exception:
        return 0


def get_docker_containers() -> list[dict]:
    """Get running Docker containers with status."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=5,
        )
        containers = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) == 2:
                containers.append({"name": parts[0], "status": parts[1]})
        return containers
    except Exception:
        return []


def get_run_pipelines(results_dir: Path, run_id: str) -> tuple[int, int]:
    """Get pipeline count from partial CSV and final CSV.

    Returns (partial_count, final_count).
    """
    # Check for partial CSV (written every 60s by broker)
    partial_files = list(results_dir.glob(f"{run_id}*.partial.csv"))
    partial_count = 0
    for pf in partial_files:
        partial_count = max(partial_count, count_csv_rows(pf))

    # Check for final CSV
    final_files = list(results_dir.glob(f"{run_id}*.csv"))
    final_files = [f for f in final_files if ".partial" not in f.name]
    final_count = 0
    for ff in final_files:
        final_count = max(final_count, count_csv_rows(ff))

    return partial_count, final_count


def render(results_dir: Path, progress: dict):
    """Render the dashboard."""
    now = time.time()
    containers = get_docker_containers()

    # Categorize runs
    runs_queued = []
    runs_running = []
    runs_done = []
    runs_failed = []

    for run_id, info in sorted(progress.items()):
        status = info.get("status", "unknown")
        if status == "queued":
            runs_queued.append((run_id, info))
        elif status == "running":
            runs_running.append((run_id, info))
        elif status == "done":
            runs_done.append((run_id, info))
        else:
            runs_failed.append((run_id, info))

    total = len(progress)
    n_done = len(runs_done)
    n_running = len(runs_running)
    n_failed = len(runs_failed)
    n_queued = len(runs_queued)
    fraction = n_done / total if total > 0 else 0

    # ETA estimation from completed runs
    completed_times = []
    for run_id, info in runs_done:
        ts = info.get("timestamp", "")
        try:
            end = datetime.fromisoformat(ts).timestamp()
        except (ValueError, TypeError):
            continue
        completed_times.append(end)

    # Find earliest start
    all_timestamps = []
    for info in progress.values():
        ts = info.get("timestamp", "")
        try:
            all_timestamps.append(datetime.fromisoformat(ts).timestamp())
        except (ValueError, TypeError):
            pass

    phase_start = min(all_timestamps) if all_timestamps else now
    elapsed = now - phase_start

    # Estimate remaining time
    if n_done > 0 and completed_times:
        avg_time_per_run = elapsed / n_done
        remaining_runs = n_queued + n_running
        eta = avg_time_per_run * remaining_runs
    else:
        eta = -1

    # Clear and render
    sys.stdout.write("\033[2J\033[H")

    print("=" * 72)
    print("  Neural Pub/Sub Experiment Monitor")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}    Dir: {results_dir}")
    print("=" * 72)
    print()
    print(f"  Overall:  {progress_bar(fraction, 35)}  ({n_done}/{total} runs)")
    print(f"  Elapsed:  {format_time(elapsed)}    ETA: {format_time(eta)}")
    print()

    # Running runs (detailed)
    if runs_running:
        print(f"  \U0001F504 Running ({n_running}):")
        for run_id, info in runs_running:
            ts = info.get("timestamp", "")
            try:
                start = datetime.fromisoformat(ts).timestamp()
                run_elapsed = now - start
            except (ValueError, TypeError):
                run_elapsed = 0

            partial, final = get_run_pipelines(results_dir, run_id)
            pipe_count = max(partial, final)

            # Find matching containers
            run_containers = [c for c in containers if run_id.lower().replace("_", "-") in c["name"].lower()]
            healthy = sum(1 for c in run_containers if "healthy" in c["status"].lower())
            total_c = len(run_containers)

            pipe_str = f"{pipe_count} pipelines" if pipe_count > 0 else "starting..."
            container_str = f"{healthy}/{total_c} healthy" if total_c > 0 else "no containers"

            print(f"    {run_id}")
            print(f"      Elapsed: {format_time(run_elapsed)}  |  {pipe_str}  |  {container_str}")
        print()

    # Completed runs (compact)
    if runs_done:
        print(f"  \u2705 Completed ({n_done}):")
        for run_id, info in runs_done[-5:]:  # show last 5
            detail = info.get("detail", "")
            if detail:
                # Count rows in final CSV
                final_path = Path(detail)
                rows = count_csv_rows(final_path)
                print(f"    {run_id}: {rows} pipelines")
            else:
                print(f"    {run_id}")
        if n_done > 5:
            print(f"    ... and {n_done - 5} more")
        print()

    # Failed runs
    if runs_failed:
        print(f"  \u274C Failed ({n_failed}):")
        for run_id, info in runs_failed:
            detail = info.get("detail", "")[:60]
            print(f"    {run_id}: {detail}")
        print()

    # Queued
    if runs_queued:
        print(f"  \u23F3 Queued: {', '.join(r[0] for r in runs_queued)}")
        print()

    # Docker containers
    if containers:
        npubsub_containers = [c for c in containers if "npubsub" in c["name"].lower()]
        if npubsub_containers:
            print(f"  Docker: {len(npubsub_containers)} containers")
            brokers = [c for c in npubsub_containers if "broker" in c["name"]]
            workers = [c for c in npubsub_containers if "worker" in c["name"]]
            workload = [c for c in npubsub_containers if "workload" in c["name"]]
            if brokers:
                print(f"    Brokers:  {len(brokers)} ({sum(1 for b in brokers if 'healthy' in b['status'].lower())} healthy)")
            if workers:
                print(f"    Workers:  {len(workers)}")
            if workload:
                print(f"    Workload: {len(workload)}")
    else:
        print("  Docker: no containers running")

    # Result files
    csv_files = sorted(results_dir.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
    csv_files = [f for f in csv_files if not f.name.startswith(".") and "summary" not in f.name]
    if csv_files:
        print()
        print("  Recent result files:")
        for f in csv_files[:5]:
            size = f.stat().st_size
            rows = count_csv_rows(f)
            size_str = f"{size / 1024:.0f}KB" if size > 1024 else f"{size}B"
            print(f"    {f.name}  ({rows} rows, {size_str})")

    print()
    print("  Ctrl+C to stop monitoring (experiment continues in background)")
    print()
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Monitor Neural Pub/Sub experiments")
    parser.add_argument("results_dir", nargs="?", default="results/phase_a",
                        help="Results directory to monitor (default: results/phase_a)")
    parser.add_argument("--interval", type=int, default=5, help="Refresh interval in seconds")
    args = parser.parse_args()

    results_dir = PROJECT_ROOT / args.results_dir

    if not results_dir.exists():
        print(f"Results directory not found: {results_dir}")
        print("Waiting for experiments to start...")
        while not results_dir.exists():
            time.sleep(2)

    try:
        while True:
            progress = load_progress(results_dir)
            if progress:
                render(results_dir, progress)
            else:
                sys.stdout.write("\033[2J\033[H")
                print(f"  Waiting for experiments to start in {results_dir}...")
                print(f"  No .progress.json found yet.")

            # Check if all done
            if progress and all(
                info.get("status") in ("done", "failed", "no_output")
                for info in progress.values()
            ):
                render(results_dir, progress)
                print("  \u2728 All runs complete!")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n  Monitor stopped. Experiment continues in background.")


if __name__ == "__main__":
    main()
