#!/usr/bin/env python3
"""Live progress monitor for Neural Pub/Sub experiment runs.

Displays:
  - Per-run progress with pipeline completion counts and ETA
  - Docker container health status
  - Overall phase progress with time estimates
  - Recent partial CSV stats (from crash-resilient snapshots)

Usage:
    python scripts/monitor.py                          # monitor results/baseline
    python scripts/monitor.py results/slicing          # monitor specific phase
    python scripts/monitor.py --interval 2             # refresh every 2s
    python scripts/monitor.py --all                    # monitor all phases
    python scripts/monitor.py --all --remote HOST      # all phases on remote
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

# Ensure the project root is on sys.path so that ``from scripts._common``
# works regardless of invocation method (``python scripts/monitor.py``,
# ``python -m scripts.monitor``, or ``PYTHONPATH=. python scripts/monitor.py``).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Directories to skip when discovering phase subdirectories in results/.
_SKIP_DIRS = {
    "analysis", "local", "test_cleanup", "test_signal",
    # Legacy phase names (pre-rename). Their data is archived under the
    # canonical names (federation, resilience, etc.) or in _archive/.
    "phase_a", "phase_b", "phase_c", "phase_d", "phase_e",
}


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


def format_run_eta(elapsed_s: float, expected_s: float) -> str:
    """Format per-run ETA with OVERDUE indicator for stalled runs."""
    if elapsed_s > expected_s * 1.5:
        excess = elapsed_s - expected_s
        return f"OVERDUE +{format_time(excess)}"
    remaining = max(0, expected_s - elapsed_s)
    return format_time(remaining)


def _discover_progress_from_csvs(
    results_dir: Path, remote_host: str | None = None
) -> dict:
    """Build a synthetic progress dict from CSV/FAILED files when no .progress.json exists.

    This supports the bash run-experiments.sh runner which doesn't write progress files.
    """
    progress = {}
    try:
        if remote_host:
            result = subprocess.run(
                ["ssh", remote_host, f"ls {results_dir}/*.csv {results_dir}/*.FAILED 2>/dev/null"],
                capture_output=True, text=True, timeout=10,
            )
            files = result.stdout.strip().split("\n") if result.stdout.strip() else []
        else:
            files = [str(f) for f in results_dir.glob("*.csv")] + \
                    [str(f) for f in results_dir.glob("*.FAILED")]
    except Exception:
        return {}

    for f in files:
        name = Path(f).stem
        if name.startswith("smoke") or name.startswith(".") or "summary" in name:
            continue
        if f.endswith(".FAILED"):
            progress[name] = {"status": "failed", "timestamp": ""}
        else:
            progress[name] = {"status": "done", "timestamp": ""}
    return progress


def _discover_distributed_runs(
    results_dir: Path, remote_host: str | None = None,
) -> dict:
    """Detect completed distributed runs by finding subdirs with vm*/ children.

    A distributed run is complete iff it has BOTH a vm*/ subdir AND a
    sibling .csv at ``{results_dir}/{run_id}.csv``. A directory with
    vm*/ children but no .csv is a phantom from an aborted run (partial
    rsync) and must NOT be counted. See L50/L51 in Tasks/lessons.md.
    """
    progress = {}
    if remote_host:
        try:
            # Find dirs with vm*/ children, then filter to those with sibling .csv
            result = subprocess.run(
                ["ssh", remote_host,
                 f"for d in {results_dir}/*/vm1/; do "
                 f"  rid=$(basename $(dirname \"$d\")); "
                 f"  [ -f \"{results_dir}/$rid.csv\" ] && echo \"$rid\"; "
                 f"done 2>/dev/null"],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.strip().splitlines():
                run_id = line.strip()
                if run_id:
                    progress[run_id] = {"status": "done", "timestamp": ""}
        except Exception:
            pass
        return progress

    if not results_dir.is_dir():
        return {}

    # Collect .csv basenames first (same approach as _common.py fix)
    csv_basenames: set[str] = set()
    for f in results_dir.iterdir():
        if f.is_file() and f.suffix == ".csv" and ".partial" not in f.name:
            if not f.name.startswith(".") and "summary" not in f.name:
                csv_basenames.add(f.stem)

    for entry in results_dir.iterdir():
        if not entry.is_dir() or entry.name.startswith(".") or entry.name.startswith("_"):
            continue
        # Check if this dir has vm*/ children (distributed result layout)
        vm_children = [c for c in entry.iterdir() if c.is_dir() and c.name.startswith("vm")]
        if not vm_children:
            continue
        # Require sibling .csv to exist (phantom dirs don't have one)
        if entry.name not in csv_basenames:
            continue
        progress[entry.name] = {"status": "done", "timestamp": ""}
    return progress


def merge_progress(*sources: dict) -> dict:
    """Merge multiple progress dicts. First source wins per run_id."""
    merged = {}
    for source in sources:
        for run_id, info in source.items():
            if run_id not in merged:
                merged[run_id] = info
    return merged


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
            # Send as single command string so SSH preserves \t and {{}} format
            cmd = [
                "ssh", remote_host,
                "docker ps --format '{{.Names}}\t{{.Status}}'",
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


def get_distributed_containers() -> list[dict]:
    """Get Docker containers from all VMs in the distributed cluster.

    Returns a flat list of container dicts, each augmented with a 'vm' key.
    """
    try:
        from scripts.multi_vm_runner import VMS
    except ImportError:
        return []

    all_containers = []
    for vm in VMS:
        containers = get_docker_containers(remote_host=vm.ssh_host)
        for c in containers:
            c["vm"] = vm.name
        all_containers.extend(containers)
    return all_containers


def get_run_pipelines(
    results_dir: Path, run_id: str, remote_host: str | None = None,
) -> tuple[int, int]:
    """Get pipeline count from partial CSV and final CSV.

    Returns (partial_count, final_count).

    Args:
        results_dir: Directory containing result CSV files.
        run_id: Run identifier used as filename prefix.
        remote_host: If set, check files on this SSH host instead of locally.
    """
    if remote_host:
        # Use SSH ls + wc to count rows remotely
        try:
            result = subprocess.run(
                ["ssh", remote_host,
                 f"wc -l {results_dir}/{run_id}*.csv 2>/dev/null | tail -1"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split()
                if parts and parts[0].isdigit():
                    total_lines = int(parts[0])
                    # Subtract 1 per file for header (rough estimate)
                    return 0, max(0, total_lines - 1)
        except Exception:
            pass
        return 0, 0

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


def render(results_dir: Path, progress: dict, remote_host: str | None = None,
           distributed: bool = False):
    """Render the dashboard."""
    now = time.time()
    if distributed:
        containers = get_distributed_containers()
    else:
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
        from scripts._common import DEFAULT_RUN_DURATION_S
        remaining_runs = n_queued + n_running
        eta = remaining_runs * DEFAULT_RUN_DURATION_S
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

            partial, final = get_run_pipelines(results_dir, run_id, remote_host=remote_host)
            pipe_count = max(partial, final)

            # Find matching containers
            run_containers = [c for c in containers if run_id.lower().replace("_", "-") in c["name"].lower()]
            healthy = sum(1 for c in run_containers if "healthy" in c["status"].lower())
            total_c = len(run_containers)

            if pipe_count > 0:
                pipe_str = f"{pipe_count} pipelines"
            elif total_c > 0:
                pipe_str = "in progress (no CSV yet)"
            else:
                pipe_str = "starting..."
            if total_c > 0:
                container_str = f"{total_c} up" + (f", {healthy} health-OK" if healthy > 0 else "")
            else:
                container_str = "no containers"

            from scripts._common import DEFAULT_RUN_DURATION_S
            run_duration = DEFAULT_RUN_DURATION_S
            pct = min(run_elapsed / run_duration, 1.0) if run_elapsed > 0 else 0.0
            bar_len = 20
            filled = int(bar_len * pct)
            bar = "█" * filled + "░" * (bar_len - filled)
            eta_run = max(0, run_duration - run_elapsed)
            from scripts._common import DEFAULT_WARMUP_S
            warmup_indicator = " (warmup)" if run_elapsed < DEFAULT_WARMUP_S else ""

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


# ---------------------------------------------------------------------------
# Multi-directory (--all) helpers
# ---------------------------------------------------------------------------


def discover_phases(
    results_root: Path, remote_host: str | None = None,
) -> list[str]:
    """Return sorted list of phase directory names under *results_root*.

    Skips hidden directories (starting with ``_``), ``analysis``, and other
    non-phase directories defined in ``_SKIP_DIRS``.

    When *remote_host* is set, lists directories via SSH instead of local
    filesystem (the local path may not exist for remote experiments).
    """
    if remote_host:
        try:
            out = subprocess.check_output(
                ["ssh", remote_host, f"ls -d {results_root}/*/"],
                text=True, stderr=subprocess.DEVNULL, timeout=10,
            )
            phases = []
            for line in out.strip().splitlines():
                name = line.rstrip("/").rsplit("/", 1)[-1]
                if name.startswith("_") or name.startswith("."):
                    continue
                if name in _SKIP_DIRS:
                    continue
                phases.append(name)
            return sorted(phases)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return []

    if not results_root.is_dir():
        return []
    phases = []
    for entry in sorted(results_root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("_") or entry.name.startswith("."):
            continue
        if entry.name in _SKIP_DIRS:
            continue
        phases.append(entry.name)
    return phases


def phase_summary(
    phase_dir: Path, remote_host: str | None = None,
) -> dict:
    """Compute a summary dict for a single phase directory.

    Returns a dict with keys: phase, total, done, running, failed, queued,
    and progress (the raw progress dict).
    """
    phase_name = phase_dir.name
    # Merge all progress sources: .progress.json takes precedence,
    # then CSV files on disk, then distributed run directories.
    progress_json = load_progress(phase_dir, remote_host=remote_host)
    csv_progress = _discover_progress_from_csvs(phase_dir, remote_host=remote_host)
    dist_progress = _discover_distributed_runs(phase_dir, remote_host=remote_host)
    progress = merge_progress(progress_json, csv_progress, dist_progress)

    n_done = sum(1 for v in progress.values() if v.get("status") == "done")
    n_running = sum(1 for v in progress.values() if v.get("status") == "running")
    n_failed = sum(1 for v in progress.values() if v.get("status") == "failed")
    n_queued = sum(1 for v in progress.values() if v.get("status") == "queued")
    total = len(progress)

    from scripts._common import DEFAULT_RUN_DURATION_S
    now = time.time()

    # Collect all timestamps, filtering out stale ones from old crashes.
    # Only use timestamps within a reasonable window of the most recent one.
    all_ts = []
    for info in progress.values():
        ts = info.get("timestamp", "")
        try:
            t = datetime.fromisoformat(ts).timestamp()
            all_ts.append(t)
        except (ValueError, TypeError):
            pass

    # T7: Filter stale timestamps — keep only those within 2x DEFAULT_RUN_DURATION_S
    # of the most recent timestamp. This prevents old crash artifacts from
    # skewing elapsed time and ETA.
    if all_ts:
        newest = max(all_ts)
        staleness_threshold = 2 * DEFAULT_RUN_DURATION_S
        recent_ts = [t for t in all_ts if newest - t < staleness_threshold]
        if not recent_ts:
            recent_ts = [newest]  # at least keep the newest
    else:
        recent_ts = []

    # Compute fraction with partial credit for running runs.
    # Use actual_run_duration from completed runs if available, else default.
    actual_run_duration = DEFAULT_RUN_DURATION_S
    if n_done >= 2 and recent_ts:
        # Estimate actual run duration from elapsed / done
        session_start = min(recent_ts)
        session_elapsed = now - session_start
        actual_run_duration = session_elapsed / n_done

    partial_credit = 0.0
    for info in progress.values():
        if info.get("status") == "running":
            ts = info.get("timestamp", "")
            try:
                start = datetime.fromisoformat(ts).timestamp()
                run_elapsed = now - start
                partial_credit += min(run_elapsed / actual_run_duration, 0.99)
            except (ValueError, TypeError):
                pass

    effective_done = n_done + partial_credit
    fraction = effective_done / total if total > 0 else 0.0

    # Compute ETA using session-aware rate.
    if effective_done > 0 and recent_ts:
        session_start = min(recent_ts)
        elapsed = now - session_start
        remaining = total - effective_done
        rate = elapsed / effective_done
        eta_s = rate * remaining
    elif total > 0:
        eta_s = (total - effective_done) * DEFAULT_RUN_DURATION_S
    else:
        eta_s = -1

    return {
        "phase": phase_name,
        "total": total,
        "done": n_done,
        "running": n_running,
        "failed": n_failed,
        "queued": n_queued,
        "fraction": fraction,
        "eta_s": eta_s,
        "progress": progress,
    }


def render_all_phases(
    summaries: list[dict],
    results_root: Path,
    remote_host: str | None = None,
    distributed: bool = False,
) -> None:
    """Render a combined dashboard for all phases.

    Shows a per-phase summary table, then detailed info for any phase
    that has running experiments.
    """
    host_label = f"    Host: {remote_host}" if remote_host else ""

    # Clear screen
    sys.stdout.write("\033[2J\033[H")

    print("=" * 72)
    print("  Neural Pub/Sub Experiment Monitor (All Phases)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{host_label}")
    print("=" * 72)
    print()

    # Summary table with progress bars and ETAs
    header = f"  {'Phase':<15} {'Progress':<22} {'Done':>5}/{'Total':<5} {'Run':>3} {'Fail':>4}  {'ETA':>10}"
    print(header)
    print("  " + "\u2500" * 70)
    for s in summaries:
        frac = s.get("fraction", 0.0)
        eta = s.get("eta_s", -1)
        bar = mini_bar(frac, 15)
        eta_str = format_time(eta) if eta >= 0 else "--:--"
        fail_str = str(s["failed"]) if s["failed"] > 0 else " "
        run_str = str(s["running"]) if s["running"] > 0 else " "
        print(
            f"  {s['phase']:<15} {bar} {frac*100:5.1f}%  "
            f"{s['done']:>4}/{s['total']:<4}  {run_str:>3} {fail_str:>4}  {eta_str:>10}"
        )
    print()

    # Detail for phases with running experiments
    for s in summaries:
        if s["running"] > 0:
            running_runs = [
                (rid, info) for rid, info in sorted(s["progress"].items())
                if info.get("status") == "running"
            ]
            for run_id, info in running_runs:
                ts = info.get("timestamp", "")
                now = time.time()
                try:
                    start = datetime.fromisoformat(ts).timestamp()
                    run_elapsed = now - start
                except (ValueError, TypeError):
                    run_elapsed = 0

                from scripts._common import DEFAULT_RUN_DURATION_S
                run_duration = DEFAULT_RUN_DURATION_S
                pct = min(run_elapsed / run_duration, 1.0) if run_elapsed > 0 else 0.0
                bar_len = 20
                filled = int(bar_len * pct)
                bar = "\u2588" * filled + "\u2591" * (bar_len - filled)
                eta_run = max(0, run_duration - run_elapsed)

                print(f"  \U0001F504 Running: {s['phase']}/{run_id}")
                print(
                    f"      [{bar}] {pct * 100:4.1f}%  "
                    f"{format_time(run_elapsed)}/{format_time(run_duration)}"
                )
            print()

    # Distributed cluster status
    if distributed:
        containers = get_distributed_containers()
        if containers:
            from collections import Counter
            vm_counts = Counter(c["vm"] for c in containers)
            brokers = sum(1 for c in containers if "broker" in c["name"])
            workers = sum(1 for c in containers if "worker" in c["name"])
            print(f"  Cluster: {len(containers)} containers across {len(vm_counts)} VMs "
                  f"({brokers} brokers, {workers} workers)")
            for vm_name, count in sorted(vm_counts.items()):
                print(f"    {vm_name}: {count} containers")
            print()

    # Grand totals
    grand_total = sum(s["total"] for s in summaries)
    grand_done = sum(s["done"] for s in summaries)
    grand_running = sum(s["running"] for s in summaries)
    grand_failed = sum(s["failed"] for s in summaries)
    if grand_total > 0:
        fraction = grand_done / grand_total
        print(f"  Grand total: {progress_bar(fraction, 30)}  ({grand_done}/{grand_total} runs)")
        if grand_failed:
            print(f"  Failed: {grand_failed}")
    else:
        print("  No experiment results found yet.")

    print()
    print("=" * 72)
    print("  Ctrl+C to stop monitoring (experiment continues in background)")
    print()
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Argument parser (extracted for testability)
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for the monitor CLI."""
    parser = argparse.ArgumentParser(description="Monitor Neural Pub/Sub experiments")
    parser.add_argument(
        "results_dir", nargs="?", default="results/baseline",
        help="Results directory to monitor (default: results/baseline)",
    )
    parser.add_argument(
        "--all", dest="all_phases", action="store_true",
        help="Monitor all experiment phases under results/.",
    )
    parser.add_argument(
        "--interval", type=int, default=5,
        help="Refresh interval in seconds",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Print status once and exit (no refresh loop).",
    )
    parser.add_argument(
        "--remote", metavar="HOST", default=None,
        help="Monitor experiments running on a remote SSH host.",
    )
    parser.add_argument(
        "--remote-dir", metavar="DIR", default=None,
        help="Repo directory on the remote host (default: reads HOST_D1_DIR from .env.local).",
    )
    parser.add_argument(
        "--distributed", action="store_true",
        help="Monitor distributed 4-VM cluster (checks Docker on all VMs).",
    )
    return parser


def _resolve_remote_dir(remote_host: str | None, remote_dir: str | None) -> str | None:
    """Resolve the remote directory, reading from .env.local if needed."""
    if remote_host and not remote_dir:
        env_local = PROJECT_ROOT / ".env.local"
        if env_local.exists():
            for line in env_local.read_text().splitlines():
                line = line.strip()
                if line.startswith("HOST_D1_DIR="):
                    return line.split("=", 1)[1].strip()
    return remote_dir


def main():
    parser = _build_parser()
    args = parser.parse_args()

    remote_host = args.remote
    remote_dir = _resolve_remote_dir(remote_host, args.remote_dir)
    once_mode = args.once

    # Resolve the results root (used by --all) and single results_dir
    if remote_host:
        if remote_dir:
            results_root = Path(remote_dir) / "results"
            results_dir = Path(remote_dir) / args.results_dir
        else:
            results_root = Path("results")
            results_dir = Path(args.results_dir)
    else:
        results_root = PROJECT_ROOT / "results"
        results_dir = PROJECT_ROOT / args.results_dir

    # ── --all mode: monitor all phase directories ────────────────────
    if args.all_phases:
        try:
            while True:
                phases = discover_phases(results_root, remote_host=remote_host)
                summaries = [
                    phase_summary(results_root / p, remote_host=remote_host)
                    for p in phases
                ]
                render_all_phases(summaries, results_root=results_root,
                                  remote_host=remote_host, distributed=args.distributed)

                if once_mode:
                    break

                # Check if everything is finished — only auto-exit when
                # there are active .progress.json files with queued/running entries.
                # Without this guard, the monitor exits prematurely between phases
                # (e.g., baseline done, contention not yet started by orchestrator).
                grand_total = sum(s["total"] for s in summaries)
                grand_running = sum(s["running"] for s in summaries)
                grand_queued = sum(s["queued"] for s in summaries)
                has_active_progress = any(
                    load_progress(results_root / p) for p in
                    discover_phases(results_root, remote_host=remote_host)
                )
                if (grand_total > 0 and grand_running == 0 and grand_queued == 0
                        and has_active_progress):
                    print("  All experiments complete!")
                    break

                time.sleep(args.interval)

        except KeyboardInterrupt:
            print("\n  Monitor stopped. Experiments continue in background.")
        return

    # ── Single-directory mode (original behavior) ────────────────────
    if not remote_host and not once_mode:
        if not results_dir.exists():
            print(f"Results directory not found: {results_dir}")
            print("Waiting for experiments to start...")
            while not results_dir.exists():
                time.sleep(2)

    try:
        while True:
            progress = merge_progress(
                load_progress(results_dir, remote_host=remote_host),
                _discover_progress_from_csvs(results_dir, remote_host=remote_host),
                _discover_distributed_runs(results_dir, remote_host=remote_host),
            )

            if progress:
                render(results_dir, progress, remote_host=remote_host,
                       distributed=args.distributed)
            else:
                if once_mode:
                    print(f"  No results found in {results_dir}")
                else:
                    sys.stdout.write("\033[2J\033[H")
                    host_label = f" on {remote_host}" if remote_host else ""
                    print(f"  Waiting for experiments to start in {results_dir}{host_label}...")
                    print(f"  No results found yet.")

            # --once: print once and exit
            if once_mode:
                break

            # Check if all done
            if progress and all(
                info.get("status") in ("done", "failed", "no_output")
                for info in progress.values()
            ):
                render(results_dir, progress, remote_host=remote_host,
                       distributed=args.distributed)
                print("  \u2728 All runs complete!")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n  Monitor stopped. Experiment continues in background.")


if __name__ == "__main__":
    main()
