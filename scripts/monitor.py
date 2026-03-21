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


def load_progress(results_dir: Path, remote_host: str | None = None) -> dict:
    if remote_host:
        pf = str(results_dir / ".progress.json")
        try:
            result = subprocess.run(
                ["ssh", remote_host, "cat", pf],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout)
        except (json.JSONDecodeError, OSError, subprocess.TimeoutExpired):
            pass
        return {}

    pf = results_dir / ".progress.json"
    if pf.exists():
        try:
            return json.loads(pf.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def count_csv_rows(csv_path: Path, remote_host: str | None = None) -> int:
    """Count data rows in a CSV file (excluding header)."""
    if remote_host:
        try:
            result = subprocess.run(
                ["ssh", remote_host, "wc", "-l", str(csv_path)],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                # wc -l returns "<count> <path>"; subtract 1 for header
                count = int(result.stdout.strip().split()[0])
                return max(0, count - 1)
        except Exception:
            pass
        return 0

    if not csv_path.exists():
        return 0
    try:
        with open(csv_path) as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            return sum(1 for _ in reader)
    except Exception:
        return 0


def get_docker_containers(remote_host: str | None = None) -> list[dict]:
    """Get running Docker containers with status.

    Args:
        remote_host: If set, run ``docker ps`` on this SSH host instead of
            locally.
    """
    try:
        if remote_host:
            cmd = [
                "ssh", remote_host,
                "docker", "ps", "--format", "{{.Names}}\t{{.Status}}",
            ]
        else:
            cmd = ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
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


def render(results_dir: Path, progress: dict, remote_host: str | None = None):
    """Render the dashboard."""
    now = time.time()
    containers = get_docker_containers(remote_host=remote_host)

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
    elif n_running > 0 or n_queued > 0:
        # Fallback: assume 2400s per run (serial execution)
        remaining_runs = n_queued + n_running
        eta = remaining_runs * 2400
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
            if total_c > 0:
                container_str = f"{total_c} up" + (f", {healthy} health-OK" if healthy > 0 else "")
            else:
                container_str = "no containers"

            # Time-based progress bar (each run = 2400s total)
            run_duration = 2400  # warmup + measurement
            pct = min(run_elapsed / run_duration, 1.0) if run_elapsed > 0 else 0.0
            bar_len = 20
            filled = int(bar_len * pct)
            bar = "█" * filled + "░" * (bar_len - filled)
            eta_run = max(0, run_duration - run_elapsed)
            warmup_indicator = " (warmup)" if run_elapsed < 600 else ""

            print(f"    {run_id}")
            print(f"      [{bar}] {pct*100:4.1f}%  {format_time(run_elapsed)}/{format_time(run_duration)}{warmup_indicator}")
            print(f"      {pipe_str}  |  {container_str}  |  ETA: {format_time(eta_run)}")
        print()

    # Completed runs (compact)
    if runs_done:
        print(f"  \u2705 Completed ({n_done}):")
        for run_id, info in runs_done[-5:]:  # show last 5
            detail = info.get("detail", "")
            if detail:
                # Count rows in final CSV
                final_path = Path(detail)
                rows = count_csv_rows(final_path, remote_host=remote_host)
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

    # Queued (compact summary by config×rate)
    if runs_queued:
        from collections import Counter
        buckets = Counter()
        for run_id, _ in runs_queued:
            # Parse "A1_rate-low_stages-3_seed-42" → "A1 low"
            parts = run_id.split("_")
            config = parts[0] if parts else "?"
            rate = parts[1].replace("rate-", "") if len(parts) > 1 else "?"
            buckets[f"{config}/{rate}"] += 1
        summary = ", ".join(f"{k}×{v}" for k, v in sorted(buckets.items()))
        print(f"  ⏳ Queued ({len(runs_queued)}): {summary}")
        print()

    # Docker containers
    if containers:
        npubsub_containers = [c for c in containers if "npubsub" in c["name"].lower()]
        if npubsub_containers:
            print(f"  Docker: {len(npubsub_containers)} containers")
            brokers = [c for c in npubsub_containers if "broker" in c["name"]]
            workers = [c for c in npubsub_containers if "worker" in c["name"]]
            workload = [c for c in npubsub_containers if "workload" in c["name"]]
            kafka = [c for c in npubsub_containers if "kafka" in c["name"]]
            other = [c for c in npubsub_containers
                     if c not in brokers and c not in workers and c not in workload and c not in kafka]
            if brokers:
                print(f"    Brokers:  {len(brokers)} ({sum(1 for b in brokers if 'healthy' in b['status'].lower())} healthy)")
            if workers:
                print(f"    Workers:  {len(workers)}")
            if kafka:
                healthy = sum(1 for k in kafka if "healthy" in k["status"].lower())
                print(f"    Kafka:    {len(kafka)} ({healthy} healthy)")
            if workload:
                print(f"    Workload: {len(workload)}")
            if other:
                print(f"    Other:    {len(other)}")
    else:
        print("  Docker: no containers running")

    # Result files
    if remote_host:
        try:
            result = subprocess.run(
                ["ssh", remote_host, f"ls -t {results_dir}/*.csv 2>/dev/null | head -5"],
                capture_output=True, text=True, timeout=10,
            )
            csv_names = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
            if csv_names:
                print()
                print("  Recent result files:")
                for name in csv_names[:5]:
                    rows = count_csv_rows(Path(name), remote_host=remote_host)
                    print(f"    {Path(name).name}  ({rows} rows)")
        except Exception:
            pass
    else:
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
    parser.add_argument("--remote", metavar="HOST", default=None,
                        help="Monitor experiments running on a remote SSH host.")
    parser.add_argument("--remote-dir", metavar="DIR", default=None,
                        help="Repo directory on the remote host (default: reads HOST_D1_DIR from .env.local).")
    args = parser.parse_args()

    remote_host = args.remote
    remote_dir = args.remote_dir

    if remote_host and not remote_dir:
        # Try to read from .env.local
        env_local = PROJECT_ROOT / ".env.local"
        if env_local.exists():
            for line in env_local.read_text().splitlines():
                line = line.strip()
                if line.startswith("HOST_D1_DIR="):
                    remote_dir = line.split("=", 1)[1].strip()
                    break

    if remote_host:
        # Prepend remote repo dir so SSH commands resolve relative paths
        if remote_dir:
            results_dir = Path(remote_dir) / args.results_dir
        else:
            results_dir = Path(args.results_dir)
    else:
        results_dir = PROJECT_ROOT / args.results_dir

    if not remote_host:
        if not results_dir.exists():
            print(f"Results directory not found: {results_dir}")
            print("Waiting for experiments to start...")
            while not results_dir.exists():
                time.sleep(2)

    try:
        while True:
            progress = load_progress(results_dir, remote_host=remote_host)
            if progress:
                render(results_dir, progress, remote_host=remote_host)
            else:
                sys.stdout.write("\033[2J\033[H")
                host_label = f" on {remote_host}" if remote_host else ""
                print(f"  Waiting for experiments to start in {results_dir}{host_label}...")
                print(f"  No .progress.json found yet.")

            # Check if all done
            if progress and all(
                info.get("status") in ("done", "failed", "no_output")
                for info in progress.values()
            ):
                render(results_dir, progress, remote_host=remote_host)
                print("  \u2728 All runs complete!")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n  Monitor stopped. Experiment continues in background.")


if __name__ == "__main__":
    main()
