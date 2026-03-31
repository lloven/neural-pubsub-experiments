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
RESULTS_DIR = Path(__file__).parent.parent / "results" / "market"
REMOTE_PROJECT_DIR = "~/neural-pubsub"

# ---------------------------------------------------------------------------
# WAN emulation
# ---------------------------------------------------------------------------

WAN_DELAY_MS = 50
WAN_JITTER_MS = 5

def setup_wan_emulation(vm2: VMConfig, vm3: VMConfig, dry_run: bool = False) -> None:
    """Add tc qdisc netem delay on VM2's interface for traffic to VM3 (and vice versa).

    This emulates the edge-cloud WAN link (~50ms RTT).
    """
    for src, dst in [(vm2, vm3), (vm3, vm2)]:
        cmds = [
            f"sudo tc qdisc del dev eth0 root 2>/dev/null || true",
            f"sudo tc qdisc add dev eth0 root handle 1: prio",
            f"sudo tc qdisc add dev eth0 parent 1:3 handle 30: netem delay {WAN_DELAY_MS}ms {WAN_JITTER_MS}ms",
            f"sudo tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst {dst.ip}/32 flowid 1:3",
        ]
        for cmd in cmds:
            _ssh(src.ssh_host, cmd, dry_run=dry_run)


def teardown_wan_emulation(vm2: VMConfig, vm3: VMConfig, dry_run: bool = False) -> None:
    """Remove tc qdisc rules."""
    for vm in [vm2, vm3]:
        _ssh(vm.ssh_host, "sudo tc qdisc del dev eth0 root 2>/dev/null || true", dry_run=dry_run)

# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

def _ssh(host: str, cmd: str, dry_run: bool = False, timeout: int = 60) -> str:
    """Execute a command on a remote host via SSH."""
    full_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd]
    if dry_run:
        logger.info("[DRY RUN] %s: %s", host, cmd)
        return ""
    logger.info("[SSH] %s: %s", host, cmd)
    result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        logger.error("SSH failed on %s: %s", host, result.stderr)
    return result.stdout


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
    dry_run: bool = False,
) -> None:
    """Start compose stacks on all VMs."""
    for vm in VMS:
        # Determine governance for this VM based on config
        gov_enabled = _governance_for_vm(vm, governance_config)

        env_overrides = (
            f"PLACEMENT_MODE={placement_mode} "
            f"GOVERNANCE_ENABLED={gov_enabled} "
            f"VM_IP={vm.ip}"
        )
        cmd = (
            f"cd {REMOTE_PROJECT_DIR} && "
            f"{env_overrides} "
            f"docker compose --env-file deploy/{vm.env_file} "
            f"-f deploy/docker-compose.vm.yaml up -d"
        )
        _ssh(vm.ssh_host, cmd, dry_run=dry_run)


def stop_cluster(dry_run: bool = False) -> None:
    """Stop and remove compose stacks on all VMs."""
    for vm in VMS:
        cmd = (
            f"cd {REMOTE_PROJECT_DIR} && "
            f"docker compose --env-file deploy/{vm.env_file} "
            f"-f deploy/docker-compose.vm.yaml down --remove-orphans"
        )
        _ssh(vm.ssh_host, cmd, dry_run=dry_run)


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
                result = _ssh(vm.ssh_host, "curl -sf http://localhost:8080/health")
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


def collect_results(run_id: str, dry_run: bool = False) -> None:
    """Rsync results from all VMs to local results directory."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for vm in VMS:
        _rsync(
            vm.ssh_host,
            f"{REMOTE_PROJECT_DIR}/results/",
            str(RESULTS_DIR / run_id / vm.name) + "/",
            dry_run=dry_run,
        )


def run_single(
    config: str,
    seed: int,
    placement_mode: str,
    governance_config: str,
    warmup_s: int = 240,
    measurement_s: int = 600,
    dry_run: bool = False,
) -> None:
    """Execute a single experiment run."""
    run_id = f"{config}_seed-{seed}"
    logger.info("=== Run: %s (placement=%s, governance=%s) ===", run_id, placement_mode, governance_config)

    # 1. Start cluster
    start_cluster(placement_mode, governance_config, dry_run=dry_run)

    # 2. Setup WAN emulation
    setup_wan_emulation(VMS[1], VMS[2], dry_run=dry_run)

    # 3. Wait for federation
    if not wait_for_federation(dry_run=dry_run):
        logger.error("Federation failed, skipping run %s", run_id)
        stop_cluster(dry_run=dry_run)
        return

    # 4. Start workload on VM1 via Docker (blocks until done)
    workload_cmd = (
        f"cd {REMOTE_PROJECT_DIR} && "
        f"mkdir -p results/market && "
        f"docker run --rm --network=host "
        f"--entrypoint python3 "
        f"-v $PWD/results/market:/results "
        f"neural-pubsub:latest "
        f"-m src.workload.generator "
        f"--broker-url http://localhost:8080 "
        f"--seed {seed} "
        f"--warmup {warmup_s} "
        f"--duration {warmup_s + measurement_s} "
        f"--result-file /results/{run_id}.csv"
    )
    _ssh(VMS[0].ssh_host, workload_cmd, dry_run=dry_run, timeout=warmup_s + measurement_s + 120)

    # 5. Collect results
    collect_results(run_id, dry_run=dry_run)

    # 6. Teardown
    teardown_wan_emulation(VMS[1], VMS[2], dry_run=dry_run)
    stop_cluster(dry_run=dry_run)

    logger.info("=== Completed: %s ===", run_id)

# ---------------------------------------------------------------------------
# Config → placement/governance mapping
# ---------------------------------------------------------------------------

CONFIG_MAP = {
    # Allocation strategies (all use gov-all by default)
    "oracle-global":   {"placement": "oracle",       "governance": "all"},
    "market-quad":     {"placement": "market",        "governance": "all"},
    "locality-only":   {"placement": "locality",      "governance": "all"},
    "latency-greedy":  {"placement": "latency_greedy", "governance": "all"},
    "spillover":       {"placement": "spillover",     "governance": "all"},
    # Governance composition (all use market placement)
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
    parser.add_argument("--config", required=True, choices=list(CONFIG_MAP.keys()))
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--deploy-image", action="store_true", help="Build and push Docker image first")
    parser.add_argument("--warmup", type=int, default=240)
    parser.add_argument("--measurement", type=int, default=600)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    if args.deploy_image:
        deploy_image(dry_run=args.dry_run)

    cfg = CONFIG_MAP[args.config]
    run_single(
        config=args.config,
        seed=args.seed,
        placement_mode=cfg["placement"],
        governance_config=cfg["governance"],
        warmup_s=args.warmup,
        measurement_s=args.measurement,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
