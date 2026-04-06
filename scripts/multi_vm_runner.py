#!/usr/bin/env python3
"""Multi-VM experiment runner for 4-domain O-RAN deployment.

Orchestrates experiments across 4 VMs via SSH. Each VM runs a Docker
Compose stack with 1 broker + 12 workers. The runner:
1. Deploys the Docker image to all VMs
2. Starts compose stacks on all VMs
3. Waits for brokers to federate
4. Starts workload on the primary VM
5. Waits for completion
6. Collects results via rsync
7. Tears down compose stacks

Usage:
    python3 -m scripts.multi_vm_runner --config market-quad --seed 42
    python3 -m scripts.multi_vm_runner --config gov-edge-only --seed 42
    python3 -m scripts.multi_vm_runner --dry-run  # print commands only
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# VM topology
# ---------------------------------------------------------------------------

@dataclass
class VMConfig:
    """Configuration for a single VM in the cluster."""
    name: str
    ip: str
    ssh_host: str  # SSH alias or user@host
    env_file: str  # Path to .env file (relative to deploy/)
    site: str      # "edge" or "cloud"
    domain: str    # d1, d2, d3, d4

# Default placeholder VMS — override with real IPs in multi_vm_config_local.py
# (git-ignored). See deploy/vm*.env.example for env file templates.
VMS = [
    VMConfig("vm1", "10.0.0.1", "testbed-vm1", "vm1-edge-du.env",    "edge",  "d1"),
    VMConfig("vm2", "10.0.0.2", "testbed-vm2", "vm2-edge-ric.env",   "edge",  "d2"),
    VMConfig("vm3", "10.0.0.3", "testbed-vm3", "vm3-cloud-nrt.env",  "cloud", "d3"),
    VMConfig("vm4", "10.0.0.4", "testbed-vm4", "vm4-cloud-smo.env",  "cloud", "d4"),
]

try:
    from scripts.multi_vm_config_local import VMS  # noqa: F811
    logger.info("Loaded local VM config from multi_vm_config_local.py")
except ImportError:
    pass

DEPLOY_DIR = Path(__file__).parent.parent / "deploy"
RESULTS_BASE = Path(__file__).parent.parent / "results"
REMOTE_PROJECT_DIR = "~/neural-pubsub"

# Module-level override for local VM detection.
# Set via --local-vm CLI flag or auto-detected from hostname.
_LOCAL_VM_OVERRIDE: str | None = None

# ---------------------------------------------------------------------------
# Local VM detection
# ---------------------------------------------------------------------------

import socket


def is_local_vm(
    vm: VMConfig, local_vm_override: str | None = None,
) -> bool:
    """Check if *vm* is the local machine (no SSH needed).

    When the orchestrator runs on VM1, operations for VM1 can be
    executed locally (subprocess) instead of via SSH, making them
    immune to SSH connection drops.

    Detection priority:
    1. Explicit override (--local-vm CLI or module-level _LOCAL_VM_OVERRIDE).
    2. Hostname match against vm.name or vm.ssh_host.
    """
    override = local_vm_override or _LOCAL_VM_OVERRIDE
    if override:
        return vm.name == override or vm.ssh_host == override
    hostname = socket.gethostname()
    return hostname == vm.name or hostname == vm.ssh_host


def _local_run(
    cmd: str,
    dry_run: bool = False,
    timeout: int = 60,
    check: bool = False,
) -> str:
    """Execute a command locally (for VM1 operations).

    Same interface as _ssh but runs via subprocess.run(shell=True).
    """
    if dry_run:
        logger.info("[DRY RUN] LOCAL: %s", cmd)
        return ""
    logger.info("[LOCAL] %s", cmd)
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        logger.error("Local command failed (rc=%d): %s", result.returncode, result.stderr)
        if check:
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr,
            )
    return result.stdout


def _exec(
    vm: VMConfig,
    cmd: str,
    dry_run: bool = False,
    timeout: int = 60,
    check: bool = False,
    local_vm_override: str | None = None,
) -> str:
    """Execute a command on *vm*, locally or via SSH.

    Routes to _local_run when the VM is the local machine, _ssh with
    retry otherwise.
    """
    if is_local_vm(vm, local_vm_override):
        return _local_run(cmd, dry_run=dry_run, timeout=timeout, check=check)
    return _ssh(vm.ssh_host, cmd, dry_run=dry_run, timeout=timeout, check=check,
                retries=2)


# ---------------------------------------------------------------------------
# WAN emulation
# ---------------------------------------------------------------------------

WAN_DELAY_MS = 50
WAN_JITTER_MS = 5
WAN_INTERFACE = "enp1s0"  # Primary interface on 5GTNF VMs (not eth0)

def setup_wan_emulation(vm2: VMConfig, vm3: VMConfig, dry_run: bool = False) -> None:
    """Add tc qdisc netem delay on VM2's interface for traffic to VM3 (and vice versa).

    This emulates the edge-cloud WAN link (~50ms RTT).  Requires
    passwordless sudo for /sbin/tc on both VMs.  If sudo fails, logs
    a clear hint and continues (best-effort; experiments run without
    WAN shaping rather than crashing).
    """
    for src, dst in [(vm2, vm3), (vm3, vm2)]:
        cmds = [
            f"sudo tc qdisc del dev {WAN_INTERFACE} root 2>/dev/null || true",
            f"sudo tc qdisc add dev {WAN_INTERFACE} root handle 1: prio",
            f"sudo tc qdisc add dev {WAN_INTERFACE} parent 1:3 handle 30: netem delay {WAN_DELAY_MS}ms {WAN_JITTER_MS}ms",
            f"sudo tc filter add dev {WAN_INTERFACE} parent 1:0 protocol ip u32 match ip dst {dst.ip}/32 flowid 1:3",
        ]
        try:
            for cmd in cmds:
                _ssh(src.ssh_host, cmd, dry_run=dry_run)
        except subprocess.CalledProcessError as exc:
            logger.error(
                "tc setup failed on %s: %s. WAN emulation will not be active. "
                "To fix, run on each VM: "
                "echo 'lloven ALL=(ALL) NOPASSWD: /sbin/tc' | "
                "sudo tee /etc/sudoers.d/tc-netem && sudo chmod 440 /etc/sudoers.d/tc-netem",
                src.name,
                exc.stderr or exc,
            )
            return


def teardown_wan_emulation(vm2: VMConfig, vm3: VMConfig, dry_run: bool = False) -> None:
    """Remove tc qdisc rules."""
    for vm in [vm2, vm3]:
        _ssh(vm.ssh_host, f"sudo tc qdisc del dev {WAN_INTERFACE} root 2>/dev/null || true", dry_run=dry_run)

# ---------------------------------------------------------------------------
# Failure injection (SSH-based, no sudo needed)
# ---------------------------------------------------------------------------

def inject_remote_kill(
    vm: VMConfig,
    container: str,
    delay_s: int = 0,
    dry_run: bool = False,
) -> None:
    """Kill a container on a remote VM via SSH.

    Sleeps delay_s seconds, then runs docker kill. Designed to be called
    in a daemon thread from run_single().
    """
    if delay_s > 0 and not dry_run:
        time.sleep(delay_s)
    _ssh(vm.ssh_host, f"docker kill {container}", dry_run=dry_run)


def inject_remote_partition(
    vm_src: VMConfig,
    vm_dst: VMConfig,
    delay_s: int = 0,
    dry_run: bool = False,
) -> None:
    """Simulate a network partition by stopping the broker on vm_dst.

    Uses docker stop (no sudo needed) to make vm_dst's broker unreachable
    from vm_src's perspective. This is a coarse partition (full broker down)
    rather than a selective network drop.
    """
    if delay_s > 0 and not dry_run:
        time.sleep(delay_s)
    _ssh(vm_dst.ssh_host, "docker stop deploy-broker-1", dry_run=dry_run)


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

def _ssh(host: str, cmd: str, dry_run: bool = False, timeout: int = 60,
         check: bool = False, retries: int = 0, retry_delay: float = 2.0) -> str:
    """Execute a command on a remote host via SSH.

    Args:
        check: If True, raise subprocess.CalledProcessError on non-zero exit.
        retries: Number of retries on *connection* failures (TimeoutExpired,
            OSError). Command-level failures (non-zero exit) are NOT retried.
        retry_delay: Initial delay between retries (doubles each attempt).
    """
    full_cmd = ["ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10", host, cmd]
    if dry_run:
        logger.info("[DRY RUN] %s: %s", host, cmd)
        return ""
    for attempt in range(retries + 1):
        try:
            logger.info("[SSH] %s: %s", host, cmd)
            result = subprocess.run(
                full_cmd, capture_output=True, text=True, timeout=timeout,
            )
            if result.returncode != 0:
                logger.error("SSH failed on %s: %s", host, result.stderr)
                if check:
                    raise subprocess.CalledProcessError(
                        result.returncode, full_cmd, result.stdout, result.stderr,
                    )
            return result.stdout
        except (subprocess.TimeoutExpired, OSError) as exc:
            if attempt < retries:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    "SSH to %s failed (attempt %d/%d), retrying in %.0fs: %s",
                    host, attempt + 1, retries + 1, delay, exc,
                )
                time.sleep(delay)
            else:
                raise


def _rsync(src_host: str, src_path: str, dst_path: str, dry_run: bool = False) -> None:
    """Rsync results from a remote host."""
    full_cmd = ["rsync", "-az", "--ignore-errors", f"{src_host}:{src_path}", dst_path]
    if dry_run:
        logger.info("[DRY RUN] rsync %s:%s -> %s", src_host, src_path, dst_path)
        return
    logger.info("[RSYNC] %s:%s -> %s", src_host, src_path, dst_path)
    result = subprocess.run(full_cmd, capture_output=True, text=True)
    if result.returncode not in (0, 23):  # 23 = partial transfer (permission errors)
        logger.error("rsync failed: %s", result.stderr)
        raise subprocess.CalledProcessError(result.returncode, full_cmd)

# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------

_RSYNC_EXCLUDES = [
    ".git", "results", "__pycache__", ".pytest_cache",
    "*.pyc", ".mypy_cache", "logs", ".env.local",
]


def deploy_code(dry_run: bool = False) -> None:
    """Rsync the codebase from the local VM to all remote VMs.

    Skips the local VM (detected via is_local_vm).  Excludes .git,
    results, and caches to keep the transfer fast.
    """
    src = str(DEPLOY_DIR.parent) + "/"  # trailing slash = contents only
    for vm in VMS:
        if is_local_vm(vm):
            logger.info("Skipping code deploy to %s (local VM).", vm.name)
            continue
        # Use user@IP for rsync (not SSH alias) — the orchestrator VM
        # may not have ~/.ssh/config with alias definitions.
        user = vm.ssh_host.split("@")[0] if "@" in vm.ssh_host else "lloven"
        dst = f"{user}@{vm.ip}:{REMOTE_PROJECT_DIR}/"
        exclude_flags = []
        for pattern in _RSYNC_EXCLUDES:
            exclude_flags.extend(["--exclude", pattern])
        full_cmd = [
            "rsync", "-az", "--delete",
            "-e", "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10",
        ] + exclude_flags + [src, dst]
        if dry_run:
            logger.info("[DRY RUN] rsync code to %s", vm.name)
            continue
        logger.info("[RSYNC] code -> %s (%s)", vm.name, dst)
        result = subprocess.run(full_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("Code deploy to %s failed: %s", vm.name, result.stderr)
            raise subprocess.CalledProcessError(result.returncode, full_cmd)


def deploy_image(dry_run: bool = False) -> None:
    """Build and push the Docker image to all VMs."""
    logger.info("Building Docker image...")
    if not dry_run:
        subprocess.run(
            ["docker", "build", "-t", "neural-pubsub:latest", "."],
            cwd=DEPLOY_DIR.parent,
            check=True,
        )
        subprocess.run(
            ["docker", "save", "neural-pubsub:latest", "-o", "/tmp/npubsub.tar"],
            check=True,
        )
    for vm in VMS:
        if not dry_run:
            subprocess.run(["scp", "/tmp/npubsub.tar", f"{vm.ssh_host}:/tmp/"], check=True)
        _ssh(vm.ssh_host, "docker load -i /tmp/npubsub.tar", dry_run=dry_run)


def start_cluster(
    placement_mode: str = "market",
    governance_config: str = "all",
    broker_module: str | None = None,
    placement: str | None = None,
    extra_env: dict[str, str] | None = None,
    dry_run: bool = False,
) -> None:
    """Start compose stacks on all VMs."""
    for vm in VMS:
        gov_enabled = _governance_for_vm(vm, governance_config)

        env_parts = [
            f"PLACEMENT_MODE={placement_mode}",
            f"GOVERNANCE_ENABLED={gov_enabled}",
            f"VM_IP={vm.ip}",
        ]
        if broker_module:
            env_parts.append(f"BROKER_MODULE={broker_module}")
        if placement:
            env_parts.append(f"PLACEMENT={placement}")
        if extra_env:
            env_parts.extend(f"{k}={v}" for k, v in extra_env.items())

        env_overrides = " ".join(env_parts)
        cmd = (
            f"cd {REMOTE_PROJECT_DIR} && "
            f"{env_overrides} "
            f"docker compose --env-file deploy/{vm.env_file} "
            f"-f deploy/docker-compose.vm.yaml up -d"
        )
        _exec(vm, cmd, dry_run=dry_run)


def stop_cluster(dry_run: bool = False) -> None:
    """Stop and remove compose stacks on all VMs."""
    for vm in VMS:
        cmd = (
            f"cd {REMOTE_PROJECT_DIR} && "
            f"docker compose --env-file deploy/{vm.env_file} "
            f"-f deploy/docker-compose.vm.yaml down --remove-orphans"
        )
        _exec(vm, cmd, dry_run=dry_run)


def _governance_for_vm(vm: VMConfig, governance_config: str) -> str:
    """Determine whether governance is enabled for this VM."""
    if governance_config == "none":
        return "false"
    elif governance_config == "all":
        return "true"
    elif governance_config == "edge-only":
        return "true" if vm.site == "edge" else "false"
    elif governance_config == "cloud-only":
        return "true" if vm.site == "cloud" else "false"
    else:
        raise ValueError(f"Unknown governance config: {governance_config}")

# ---------------------------------------------------------------------------
# Experiment execution
# ---------------------------------------------------------------------------

def wait_for_federation(timeout_s: int = 120, dry_run: bool = False) -> bool:
    """Wait until all 4 brokers respond to health checks."""
    if dry_run:
        logger.info("[DRY RUN] Would wait for federation...")
        return True
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        all_ok = True
        for vm in VMS:
            try:
                result = _exec(vm, "curl -sf http://localhost:8080/health")
                if '"status"' not in result:
                    all_ok = False
                    break
                logger.debug("Health OK on %s: %s", vm.name, result.strip())
            except Exception:
                all_ok = False
                break
        if all_ok:
            logger.info("All 4 brokers healthy.")
            return True
        time.sleep(5)
    logger.error("Federation timeout after %ds", timeout_s)
    return False


def collect_results(run_id: str, results_subdir: str = "market", dry_run: bool = False) -> None:
    """Collect results from all VMs to the local results directory.

    For the local VM: copies results locally (no rsync needed).
    For remote VMs: rsyncs over SSH.
    """
    results_dir = RESULTS_BASE / results_subdir
    results_dir.mkdir(parents=True, exist_ok=True)
    for vm in VMS:
        dst = str(results_dir / run_id / vm.name) + "/"
        if is_local_vm(vm):
            # Local VM: the result CSV is already on disk at
            # results/{results_subdir}/{run_id}.csv. No copy needed
            # since collect_results destination is under the same tree.
            if dry_run:
                logger.info("[DRY RUN] local results already on disk for %s", vm.name)
            else:
                logger.info("Local results for %s already at results/%s/", vm.name, results_subdir)
        else:
            _rsync(
                vm.ssh_host,
                f"{REMOTE_PROJECT_DIR}/results/",
                dst,
                dry_run=dry_run,
            )


def run_single(
    config: str,
    seed: int,
    placement_mode: str,
    governance_config: str,
    warmup_s: int = 240,
    measurement_s: int = 600,
    broker_module: str | None = None,
    placement: str | None = None,
    extra_env: dict[str, str] | None = None,
    workload_env: dict[str, str] | None = None,
    results_subdir: str = "market",
    failure_fn: object | None = None,
    wan_emulation: bool = True,
    run_id: str | None = None,
    dry_run: bool = False,
) -> None:
    """Execute a single experiment run on the 4-VM cluster."""
    run_id = run_id or f"{config}_seed-{seed}"
    logger.info("=== Run: %s (placement=%s, governance=%s, broker=%s) ===",
                run_id, placement_mode, governance_config,
                broker_module or "neural_broker")

    # 1. Start cluster
    start_cluster(
        placement_mode, governance_config,
        broker_module=broker_module,
        placement=placement,
        extra_env=extra_env,
        dry_run=dry_run,
    )

    # 2. Setup WAN emulation (skip if no sudo or explicitly disabled)
    if wan_emulation:
        setup_wan_emulation(VMS[1], VMS[2], dry_run=dry_run)

    # 3. Wait for federation
    if not wait_for_federation(dry_run=dry_run):
        logger.error("Federation failed, skipping run %s", run_id)
        stop_cluster(dry_run=dry_run)
        return

    # 4. Start failure injection if configured (runs in background thread)
    if failure_fn and not dry_run:
        import threading
        t = threading.Thread(target=failure_fn, daemon=True)
        t.start()

    # 5. Start workload on VM1 via Docker (blocks until done)
    env_flags = ""
    if workload_env:
        env_flags = " ".join(f"-e {k}={v}" for k, v in workload_env.items()) + " "

    workload_cmd = (
        f"cd {REMOTE_PROJECT_DIR} && "
        f"mkdir -p results/{results_subdir} && "
        f"docker run --rm --network=host "
        f"--entrypoint python3 "
        f"{env_flags}"
        f"-v $PWD/results:/results "
        f"neural-pubsub:latest "
        f"-m src.workload.generator "
        f"--broker-url http://localhost:8080 "
        f"--seed {seed} "
        f"--warmup {warmup_s} "
        f"--duration {warmup_s + measurement_s} "
        f"--result-file /results/{results_subdir}/{run_id}.csv"
    )
    _exec(VMS[0], workload_cmd, dry_run=dry_run, timeout=warmup_s + measurement_s + 120)

    # 6. Collect results
    collect_results(run_id, results_subdir=results_subdir, dry_run=dry_run)

    # 7. Teardown
    if wan_emulation:
        teardown_wan_emulation(VMS[1], VMS[2], dry_run=dry_run)
    stop_cluster(dry_run=dry_run)

    logger.info("=== Completed: %s ===", run_id)

# ---------------------------------------------------------------------------
# Config → placement/governance mapping
# ---------------------------------------------------------------------------

CONFIG_MAP = {
    # --- Baseline strategies (S1/S2/S3) ---
    "round-robin":     {"placement": "neural", "governance": "none",
                        "broker_module": "src.broker.static_broker",
                        "static_placement": "round_robin"},
    "random":          {"placement": "neural", "governance": "none",
                        "broker_module": "src.broker.static_broker",
                        "static_placement": "random"},
    "neural":          {"placement": "neural", "governance": "none"},
    # --- Market/allocation strategies ---
    "oracle-global":   {"placement": "oracle",        "governance": "all"},
    "market-quad":     {"placement": "market",        "governance": "all"},
    "locality-only":   {"placement": "locality",      "governance": "all"},
    "latency-greedy":  {"placement": "latency_greedy", "governance": "all"},
    "spillover":       {"placement": "spillover",     "governance": "all"},
    # --- Governance composition ---
    "gov-none":        {"placement": "market", "governance": "none"},
    "gov-edge-only":   {"placement": "market", "governance": "edge-only"},
    "gov-cloud-only":  {"placement": "market", "governance": "cloud-only"},
    "gov-both":        {"placement": "market", "governance": "all"},
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-VM experiment runner")
    parser.add_argument("--stop", action="store_true",
                        help="Stop all containers on all VMs and exit")
    parser.add_argument("--config", default=None, choices=list(CONFIG_MAP.keys()))
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--deploy-image", action="store_true", help="Build and push Docker image first")
    parser.add_argument("--warmup", type=int, default=240)
    parser.add_argument("--measurement", type=int, default=600)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    if args.stop:
        logger.info("Stopping all containers on all VMs...")
        stop_cluster(dry_run=args.dry_run)
        return

    if not args.config or args.seed is None:
        parser.error("--config and --seed are required (unless --stop)")

    if args.deploy_image:
        deploy_image(dry_run=args.dry_run)

    cfg = CONFIG_MAP[args.config]
    run_single(
        config=args.config,
        seed=args.seed,
        placement_mode=cfg["placement"],
        governance_config=cfg["governance"],
        broker_module=cfg.get("broker_module"),
        placement=cfg.get("static_placement"),
        warmup_s=args.warmup,
        measurement_s=args.measurement,
        wan_emulation=False,  # requires sudo; enable when available
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
