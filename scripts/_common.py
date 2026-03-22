"""Shared infrastructure for all phase runner scripts (A through E).

Provides:
  - Constants: PROJECT_ROOT, COMPOSE_FILE, DEFAULT_SEEDS.
  - Docker Compose orchestration: compose_up / compose_down for starting
    and tearing down experiment stacks.
  - Failure injection helpers: inject_compose_kill (container kill),
    inject_network_partition (Docker network disconnect),
    inject_scale_down (replica reduction).
  - Generic single-run executor (run_single) that wraps compose lifecycle
    and result collection.
  - Shared CLI main loop (phase_main) with argparse, config validation,
    combinatorial run matrix execution, and CSV summary generation.

All phase runners (run_phase_a.py through run_phase_d.py) import from this
module; phase-specific logic is limited to config definitions, run-matrix
construction, and environment variable mapping.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
import random as _random_module

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.local.yaml"
COMPOSE_KAFKA = PROJECT_ROOT / "docker-compose.kafka.yaml"
COMPOSE_FLAT = PROJECT_ROOT / "docker-compose.flat.yaml"
COMPOSE_FLAT_EQ = PROJECT_ROOT / "docker-compose.flat-equalized.yaml"
COMPOSE_GOVERNANCE = PROJECT_ROOT / "docker-compose.governance.yaml"
DEFAULT_SEEDS = [42, 123, 456, 789, 0]  # 5 seeds (Phases A/B/C); Phase D uses 10
EXTENDED_SEEDS = [42, 123, 456, 789, 0, 7, 2024, 31415, 271828, 1337]  # 10 seeds for Phase D

# Default run timing (seconds). Used by phase runners and monitor.
DEFAULT_WARMUP_S = 120
DEFAULT_MEASUREMENT_S = 600
DEFAULT_RUN_DURATION_S = DEFAULT_WARMUP_S + DEFAULT_MEASUREMENT_S

# Rate label → numeric arrival rate (events/second)
RATE_MAP: dict[str, float] = {
    "low": 2.0,
    "medium": 5.0,
    "high": 10.0,
}


# ---------------------------------------------------------------------------
# Config resolution (unified mapping from config name → compose + env)
# ---------------------------------------------------------------------------

@dataclass
class ResolvedConfig:
    """Result of resolving a config name to compose files and env vars."""
    compose_files: list[Path]
    env: dict[str, str]

    # Optional broker module override (informational; already encoded in env)
    broker_module: str | None = None


# Transport modes for the factorial experiment.
TRANSPORTS = ["http", "kafka"]

# Config name → (extra compose overlays, env overrides, broker_module).
# Renumbered in dual-transport refactor (2026-03-21): A1=round-robin,
# A2=random, A3=neural (old A1/Kafka-only dropped, old A2-A4 shifted).
# All baselines run under both http and kafka transport (factorial design).
_CONFIG_TABLE: dict[str, dict] = {
    # Phase A: single-site baselines (3 placements × 2 transports)
    "A1": {"overlays": [], "env": {"BROKER_MODULE": "src.broker.static_broker", "PLACEMENT": "round_robin"}, "broker": "src.broker.static_broker"},
    "A2": {"overlays": [], "env": {"BROKER_MODULE": "src.broker.static_broker", "PLACEMENT": "random"}, "broker": "src.broker.static_broker"},
    "A3": {"overlays": [], "env": {}, "broker": None},
    # Phase B: slice-aware placement (4 configs × 2 transports)
    "B1": {"overlays": [COMPOSE_FLAT], "env": {}, "broker": None},
    "B1eq": {"overlays": [COMPOSE_FLAT_EQ], "env": {}, "broker": None},
    "B2": {"overlays": [], "env": {}, "broker": None},
    "B3": {"overlays": [COMPOSE_GOVERNANCE], "env": {}, "broker": None},
    "B4": {"overlays": [COMPOSE_GOVERNANCE], "env": {}, "broker": None},
    # Phase C: cross-site federation (3 configs × 2 transports)
    "C1": {"overlays": [], "env": {}, "broker": None},
    "C2": {"overlays": [], "env": {}, "broker": None},
    "C3": {"overlays": [], "env": {}, "broker": None},
    # Phase D: failure resilience (4 configs × 2 transports)
    "D1": {"overlays": [], "env": {}, "broker": None},
    "D2": {"overlays": [], "env": {}, "broker": None},
    "D3": {"overlays": [], "env": {}, "broker": None},
    "D4": {"overlays": [], "env": {}, "broker": None},
}


def resolve_config(
    config_name: str,
    rate: str = "medium",
    stages: int = 3,
    seed: int = 42,
    transport: str = "http",
) -> ResolvedConfig:
    """Resolve a config name (e.g. 'A2', 'B3') to compose files and env vars.

    Args:
        config_name: Experiment configuration identifier (A2-A4, B1-B4, etc.).
        rate: Workload rate label ('low', 'medium', 'high').
        stages: Pipeline complexity (number of stages).
        seed: Random seed for the run.
        transport: Dispatch transport mode ('http' or 'kafka').

    Returns:
        A ResolvedConfig with compose_files list and env dict.

    Raises:
        ValueError: If config_name is not recognised.
    """
    if config_name not in _CONFIG_TABLE:
        raise ValueError(
            f"Unknown config: {config_name}. "
            f"Valid: {sorted(_CONFIG_TABLE.keys())}"
        )

    entry = _CONFIG_TABLE[config_name]
    compose_files = [COMPOSE_FILE] + list(entry["overlays"])

    # Kafka transport: add the Kafka overlay
    if transport == "kafka":
        compose_files.append(COMPOSE_KAFKA)

    if rate in RATE_MAP:
        arrival_rate = RATE_MAP[rate]
    else:
        arrival_rate = float(rate)

    env: dict[str, str] = {
        "ARRIVAL_RATE": str(arrival_rate),
        "SEED": str(seed),
        "PIPELINE_STAGES": str(stages),
        **entry["env"],
    }

    return ResolvedConfig(
        compose_files=compose_files,
        env=env,
        broker_module=entry["broker"],
    )


def shuffle_configs(configs: list, seed: int = 42) -> list:
    """Deterministically shuffle a list of run configs using *seed*.

    Returns a new list (does not mutate the input). Using a fixed seed
    ensures reproducibility across machines while eliminating ordering
    biases (e.g., thermal drift favouring later runs).

    Args:
        configs: List of run configurations to shuffle.
        seed: Random seed for deterministic shuffling.

    Returns:
        A new list with the same elements in shuffled order.
    """
    rng = _random_module.Random(seed)
    shuffled = list(configs)
    rng.shuffle(shuffled)
    return shuffled


# ---------------------------------------------------------------------------
# Cool-down between runs
# ---------------------------------------------------------------------------


def cooldown_between_runs(duration_s: int = 60, dry_run: bool = False) -> None:
    """Wait between experiment runs to allow thermal and OS state to settle.

    Args:
        duration_s: Cool-down duration in seconds (default 60).
        dry_run: If True, skip the actual sleep (for testing).
    """
    if dry_run:
        logger.info("[DRY RUN] Would cool down for %ds", duration_s)
        return
    logger.info("Cooling down for %ds between runs...", duration_s)
    time.sleep(duration_s)
    logger.info("Cool-down complete.")


# ---------------------------------------------------------------------------
# Docker Compose orchestration
# ---------------------------------------------------------------------------

def compose_up(
    project_name: str,
    compose_file: Path,
    env: dict[str, str],
    timeout_s: int,
    failure_fn: Callable[[], None] | None = None,
    compose_files: list[Path] | None = None,
) -> None:
    """Run ``docker compose up`` with optional failure injection thread.

    If *failure_fn* is provided, it is started in a daemon thread after the
    compose process begins.  The function should contain its own ``sleep()``
    before the actual failure injection.

    Args:
        project_name: Docker Compose project name (``-p`` flag).
        compose_file: Path to the docker-compose YAML file (used when
            *compose_files* is None).
        env: Environment variable overrides merged with ``os.environ``.
        timeout_s: Maximum wall-clock seconds before the process is killed.
        failure_fn: Optional zero-argument callable executed in a daemon
            thread.  Should sleep internally before injecting the failure.
        compose_files: Optional list of compose file paths (base + overlays).
            When provided, overrides *compose_file*.

    Returns:
        None.  Failures are logged; cleanup is left to the caller via
        :func:`compose_down`.
    """
    compose_env = {**os.environ, **env}
    files = compose_files or [compose_file]
    file_args = []
    for f in files:
        file_args.extend(["-f", str(f)])
    cmd = [
        "docker", "compose", *file_args,
        "-p", project_name,
        "up", "--build", "--abort-on-container-exit",
        "--timeout", "30",
    ]

    if failure_fn is None:
        # Simple blocking run (Phase A style)
        try:
            subprocess.run(cmd, env=compose_env, check=True, timeout=timeout_s)
        except subprocess.TimeoutExpired:
            logger.warning("Project %s timed out", project_name)
        except subprocess.CalledProcessError as e:
            logger.error("Project %s failed with exit code %d", project_name, e.returncode)
    else:
        # Popen + failure thread (Phase B/C/D style)
        proc = subprocess.Popen(cmd, env=compose_env)
        failure_thread = threading.Thread(target=failure_fn, daemon=True)
        try:
            failure_thread.start()
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            logger.warning("Project %s timed out, collecting partial results", project_name)
            proc.terminate()
        except Exception as e:
            logger.error("Project %s failed: %s", project_name, e)


def compose_down(
    project_name: str,
    compose_file: Path,
    env: dict[str, str],
    compose_files: list[Path] | None = None,
) -> None:
    """Run ``docker compose down`` to clean up a project.

    Removes containers, volumes, and orphan services. Never raises;
    failures are silently ignored (check=False) so that cleanup is
    best-effort even if the stack is already down.

    Args:
        project_name: Docker Compose project name.
        compose_file: Path to the docker-compose YAML file (used when
            *compose_files* is None).
        env: Environment variable overrides (same dict passed to compose_up).
        compose_files: Optional list of compose file paths (base + overlays).
    """
    compose_env = {**os.environ, **env}
    files = compose_files or [compose_file]
    file_args = []
    for f in files:
        file_args.extend(["-f", str(f)])
    subprocess.run(
        [
            "docker", "compose", *file_args,
            "-p", project_name,
            "down", "--volumes", "--remove-orphans",
        ],
        env=compose_env,
        check=False,
        timeout=60,
    )


# ---------------------------------------------------------------------------
# Failure injection helpers
# ---------------------------------------------------------------------------

def inject_compose_kill(
    project_name: str,
    compose_file: Path,
    env: dict[str, str],
    target: str,
    delay_s: int,
    label: str = "service",
) -> None:
    """Sleep *delay_s* seconds, then ``docker compose kill <target>``.

    Intended to be run inside a daemon thread started by :func:`compose_up`.

    Args:
        project_name: Docker Compose project name.
        compose_file: Path to the docker-compose YAML file.
        env: Environment variable overrides.
        target: Compose service name to kill (e.g., ``"worker"``).
        delay_s: Seconds to wait before injecting the failure.
        label: Human-readable label for log messages (e.g., ``"broker"``).
    """
    logger.info(
        "Failure thread [%s]: waiting %ds before killing %s",
        label, delay_s, target,
    )
    time.sleep(delay_s)
    logger.info("Injecting %s failure: docker compose kill %s", label, target)
    compose_env = {**os.environ, **env}
    try:
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "-p", project_name,
                "kill", target,
            ],
            env=compose_env,
            check=True,
            timeout=30,
        )
        logger.info("%s failure injected: %s killed", label.capitalize(), target)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error("%s failure injection failed: %s", label.capitalize(), e)


def inject_network_partition(
    project_name: str,
    target: str,
    delay_s: int,
) -> None:
    """Sleep *delay_s*, then disconnect all containers from a Docker network.

    The network name is ``<project_name>_<target>``.

    Args:
        project_name: Docker Compose project name (used to derive the
            full Docker network name).
        target: Network suffix (e.g., ``"federation-net"``).  The actual
            Docker network disconnected is ``<project_name>_<target>``.
        delay_s: Seconds to wait before injecting the partition.
    """
    logger.info(
        "Failure thread [network]: waiting %ds before disconnecting %s",
        delay_s, target,
    )
    time.sleep(delay_s)
    network_name = f"{project_name}_{target}"
    logger.info("Injecting network partition: docker network disconnect %s", network_name)
    try:
        result = subprocess.run(
            ["docker", "network", "inspect", network_name, "--format",
             "{{range .Containers}}{{.Name}} {{end}}"],
            capture_output=True, text=True, timeout=30,
        )
        containers = result.stdout.strip().split()
        for container in containers:
            if not container:
                continue
            subprocess.run(
                ["docker", "network", "disconnect", "--force",
                 network_name, container],
                check=True, timeout=30,
            )
            logger.info("Disconnected %s from %s", container, network_name)
        logger.info("Network partition injected: %s isolated", network_name)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error("Network partition injection failed: %s", e)


def inject_scale_down(
    project_name: str,
    compose_file: Path,
    env: dict[str, str],
    target: str,
    delay_s: int,
    replicas: int = 1,
) -> None:
    """Sleep *delay_s*, then scale *target* down to *replicas*.

    Used for funnel partial-input failure simulation (Phase D, D4),
    where a subset of sensor workers are removed to test wait/proceed/abort
    semantics.

    Args:
        project_name: Docker Compose project name.
        compose_file: Path to the docker-compose YAML file.
        env: Environment variable overrides.
        target: Compose service name to scale (e.g., ``"sensor-worker"``).
        delay_s: Seconds to wait before scaling down.
        replicas: Target replica count after scale-down (default 1).
    """
    logger.info(
        "Failure thread [funnel]: waiting %ds before scaling %s to %d",
        delay_s, target, replicas,
    )
    time.sleep(delay_s)
    logger.info("Injecting funnel failure: scaling %s to %d replica(s)", target, replicas)
    compose_env = {**os.environ, **env}
    try:
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "-p", project_name,
                "up", "-d", "--scale", f"{target}={replicas}", "--no-recreate",
            ],
            env=compose_env,
            check=True,
            timeout=60,
        )
        logger.info("Funnel failure injected: %s scaled to %d", target, replicas)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error("Funnel failure injection failed: %s", e)


# ---------------------------------------------------------------------------
# Generic single-run executor
# ---------------------------------------------------------------------------

def run_single(
    run_id: str,
    env: dict[str, str],
    results_dir: Path,
    total_duration: int,
    dry_run: bool = False,
    failure_fn: Callable[[], None] | None = None,
    compose_files: list[Path] | None = None,
) -> dict[str, str]:
    """Execute one experiment run via Docker Compose and return a result dict.

    Parameters
    ----------
    run_id:
        Human-readable identifier (used in project name and output filename).
    env:
        Environment variable overrides passed to Docker Compose.
    results_dir:
        Directory where per-run CSV files land.
    total_duration:
        warmup_s + measurement_s (used for timeout calculation).
    dry_run:
        If True, skip execution and return a placeholder result.
    failure_fn:
        Optional zero-argument callable for failure injection (run in a thread).
    compose_files:
        Optional list of compose file paths (base + overlays). Defaults to
        ``[COMPOSE_FILE]`` when None.
    """
    result_file = results_dir / f"{run_id}.csv"
    project_name = f"npubsub-{run_id.lower().replace('_', '-')}"

    if dry_run:
        logger.info("  [DRY RUN] Would run for %ds, output to %s", total_duration, result_file)
        return {"run_id": run_id, "status": "dry_run", "result_file": str(result_file)}

    # Translate host path to container path (./results -> /app/results)
    try:
        container_result = Path("/app/results") / result_file.relative_to(PROJECT_ROOT / "results")
    except ValueError:
        container_result = result_file
    env["RESULT_FILE"] = str(container_result)

    try:
        compose_up(
            project_name=project_name,
            compose_file=COMPOSE_FILE,
            env=env,
            timeout_s=total_duration + 120,
            failure_fn=failure_fn,
            compose_files=compose_files,
        )
    finally:
        compose_down(project_name, COMPOSE_FILE, env, compose_files=compose_files)
        # Fix file ownership: Docker writes as root; make results readable
        # by the host user so the monitor and rsync can access them.
        _fix_result_permissions(results_dir)

    return {
        "run_id": run_id,
        "status": "completed" if result_file.exists() else "no_output",
        "result_file": str(result_file),
    }


def _fix_result_permissions(results_dir: Path) -> None:
    """Make Docker-written result files readable by the host user."""
    try:
        subprocess.run(
            ["docker", "run", "--rm",
             "-v", f"{results_dir.resolve()}:/data",
             "alpine", "chmod", "-R", "a+rw", "/data"],
            capture_output=True, timeout=30, check=False,
        )
    except Exception:
        pass  # best-effort


# ---------------------------------------------------------------------------
# Progress tracking and checkpointing
# ---------------------------------------------------------------------------

def _progress_file(results_dir: Path) -> Path:
    """Return the path to the progress JSON file (compatible with monitor.sh)."""
    return results_dir / ".progress.json"


def _load_progress(results_dir: Path) -> dict:
    """Load existing progress, or return empty dict."""
    pf = _progress_file(results_dir)
    if pf.exists():
        try:
            return json.loads(pf.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_progress(results_dir: Path, progress: dict) -> None:
    """Atomically write progress JSON."""
    pf = _progress_file(results_dir)
    tmp = pf.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress, indent=2))
    tmp.rename(pf)


def _update_progress(
    results_dir: Path, progress: dict,
    run_id: str, status: str, detail: str = "",
) -> None:
    """Update a single run's status and persist."""
    progress[run_id] = {
        "status": status,
        "detail": detail,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _save_progress(results_dir, progress)


# ---------------------------------------------------------------------------
# Shared main() loop
# ---------------------------------------------------------------------------

def phase_main(
    phase_name: str,
    description: str,
    configs: dict[str, Any],
    build_matrix_fn: Callable[[list[str], list[int]], list],
    run_fn: Callable[[Any, bool], dict],
    results_dir: Path,
    extra_args_fn: Callable[[argparse.ArgumentParser], None] | None = None,
    parse_extra_fn: Callable[[argparse.Namespace], dict] | None = None,
) -> None:
    """Shared entry point for all phase runner scripts.

    Parameters
    ----------
    phase_name:
        Short label such as "Phase A".
    description:
        One-line description for the argparse help text.
    configs:
        Dict of valid config names (e.g. ``{"A1": ..., "A2": ...}``).
    build_matrix_fn:
        ``(config_names, seeds, **extra) -> list[RunConfig]``.
        For phases that accept extra dimensions (rates, complexities), the
        caller should use *extra_args_fn* / *parse_extra_fn* to inject them.
    run_fn:
        ``(run_config, dry_run) -> result_dict``.
    results_dir:
        Where per-run and summary CSVs are written.
    extra_args_fn:
        Optional callback to add phase-specific CLI arguments.
    parse_extra_fn:
        Optional callback that returns extra kwargs for *build_matrix_fn*
        from the parsed Namespace.
    """
    default_configs = ",".join(configs.keys())

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--configs", default=default_configs,
        help=f"Comma-separated config names (default: {default_configs})",
    )
    parser.add_argument(
        "--seeds", default=",".join(str(s) for s in DEFAULT_SEEDS),
        help=f"Comma-separated seeds (default: {DEFAULT_SEEDS})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print runs without executing")
    parser.add_argument("--resume", action="store_true", help="Skip runs already completed (reads .progress.json)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])

    if extra_args_fn is not None:
        extra_args_fn(parser)

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    config_names = [c.strip() for c in args.configs.split(",")]
    seeds = [int(s.strip()) for s in args.seeds.split(",")]

    # Validate config names
    for c in config_names:
        if c not in configs:
            parser.error(f"Unknown config: {c}. Valid: {list(configs.keys())}")

    # Build extra kwargs from phase-specific args
    extra_kw: dict = {}
    if parse_extra_fn is not None:
        extra_kw = parse_extra_fn(args)

    runs = build_matrix_fn(config_names, seeds, **extra_kw)
    results_dir.mkdir(parents=True, exist_ok=True)

    # Load existing progress for checkpointing
    progress = _load_progress(results_dir) if args.resume else {}

    logger.info("%s: %d runs planned", phase_name, len(runs))
    if args.dry_run:
        logger.info("[DRY RUN MODE]")
    if args.resume:
        already_done = sum(1 for r in runs
                          if progress.get(
                              getattr(r, 'run_id', None) or getattr(r, 'config_name', ''), {}
                          ).get('status') == 'done')
        logger.info("[RESUME MODE] %d runs already completed, will skip them", already_done)

    # Initialize progress for all runs
    for run in runs:
        run_id = getattr(run, 'run_id', None) or getattr(run, 'config_name', None) or str(run)
        if run_id not in progress:
            _update_progress(results_dir, progress, run_id, "queued")

    results = []
    for i, run in enumerate(runs, 1):
        # Use run_id property if available (includes seed/rate), else config_name, else str
        run_id = getattr(run, 'run_id', None) or getattr(run, 'config_name', None) or str(run)

        # Checkpoint: skip completed runs on resume
        if args.resume and progress.get(run_id, {}).get("status") == "done":
            logger.info("--- Run %d/%d --- SKIP (already completed): %s", i, len(runs), run_id)
            results.append({"run_id": run_id, "status": "completed", "result_file": "(resumed)"})
            continue

        logger.info("--- Run %d/%d ---", i, len(runs))
        _update_progress(results_dir, progress, run_id, "running")

        try:
            result = run_fn(run, args.dry_run)
            status = "done" if result["status"] == "completed" else result["status"]
            _update_progress(results_dir, progress, run_id, status,
                           detail=result.get("result_file", ""))
        except Exception as e:
            logger.error("Run %s failed with exception: %s", run_id, e)
            result = {"run_id": run_id, "status": "failed", "result_file": str(e)}
            _update_progress(results_dir, progress, run_id, "failed", detail=str(e))

        results.append(result)

    completed = sum(1 for r in results if r["status"] == "completed")
    failed = sum(1 for r in results if r["status"] not in ("completed", "dry_run"))
    logger.info(
        "%s complete: %d/%d runs successful, %d failed",
        phase_name, completed, len(runs), failed,
    )

    summary_file = results_dir / f"{phase_name.lower().replace(' ', '_')}_summary.csv"
    with open(summary_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["run_id", "status", "result_file"])
        writer.writeheader()
        writer.writerows(results)
    logger.info("Summary written to %s", summary_file)
