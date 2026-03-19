"""Testbed deployment tool for the Neural Pub/Sub experiment.

Deploys the Docker stack to remote testbed nodes via SSH.  Each node
runs one or more Compose services; the mapping from service to node is
defined in a YAML config file (see testbed-config.yaml).

Actions
-------
push    Docker-save the image locally, scp it to every node, then
        ssh docker load on each node.
start   ssh docker compose up -d on every node (using the testbed
        compose file with service filtering).
stop    ssh docker compose down on every node.
status  ssh docker ps on every node and print a summary table.
logs    ssh docker compose logs --tail=50 on a single node
        (specify with --node).

Usage examples
--------------
    # Push the image to all testbed nodes
    python scripts/deploy.py --config testbed-config.yaml --action push

    # Start services on all nodes
    python scripts/deploy.py --config testbed-config.yaml --action start

    # Check which containers are running
    python scripts/deploy.py --config testbed-config.yaml --action status

    # Tail logs from broker-d1's node
    python scripts/deploy.py --config testbed-config.yaml --action logs --node broker-d1

    # Stop everything
    python scripts/deploy.py --config testbed-config.yaml --action stop

Requirements
------------
- SSH key-based auth to every node (no password prompts).
- Docker and Docker Compose v2 installed on every node.
- The testbed compose file (docker-compose.testbed.yaml) must exist
  locally; it is scp'd to each node during 'push'.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

SSH_TIMEOUT = 30  # seconds
SCP_TIMEOUT = 300  # image transfer can be large


@dataclass
class NodeConfig:
    """SSH-accessible testbed node."""

    name: str
    host: str
    ssh_user: str
    ssh_key: str
    compose_service: str


@dataclass
class TestbedConfig:
    """Parsed testbed-config.yaml."""

    nodes: list[NodeConfig]
    image_name: str
    image_tag: str
    compose_file: str
    results_dir: str


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def load_config(path: str) -> TestbedConfig:
    """Load and validate testbed-config.yaml."""
    with open(path) as fh:
        raw = yaml.safe_load(fh)

    nodes = []
    for name, node_raw in raw["nodes"].items():
        nodes.append(
            NodeConfig(
                name=name,
                host=node_raw["host"],
                ssh_user=node_raw.get("ssh_user", "guest"),
                ssh_key=os.path.expanduser(node_raw.get("ssh_key", "~/.ssh/id_ed25519")),
                compose_service=node_raw.get("compose_service", name),
            )
        )

    return TestbedConfig(
        nodes=nodes,
        image_name=raw.get("image_name", "neural-pubsub"),
        image_tag=raw.get("image_tag", "latest"),
        compose_file=raw.get("compose_file", "docker-compose.testbed.yaml"),
        results_dir=raw.get("results_dir", "/tmp/neural-pubsub/results"),
    )


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------


def _ssh_cmd(node: NodeConfig) -> list[str]:
    """Base ssh command list with common options."""
    return [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", f"ConnectTimeout={SSH_TIMEOUT}",
        "-i", node.ssh_key,
        f"{node.ssh_user}@{node.host}",
    ]


def _run_ssh(node: NodeConfig, remote_cmd: str, timeout: int = SSH_TIMEOUT) -> subprocess.CompletedProcess:
    """Run a command on a remote node via SSH."""
    cmd = _ssh_cmd(node) + [remote_cmd]
    logger.debug("SSH %s: %s", node.name, remote_cmd)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _scp_to(node: NodeConfig, local_path: str, remote_path: str, timeout: int = SCP_TIMEOUT) -> subprocess.CompletedProcess:
    """Copy a local file to a remote node."""
    cmd = [
        "scp",
        "-o", "StrictHostKeyChecking=no",
        "-o", f"ConnectTimeout={SSH_TIMEOUT}",
        "-i", node.ssh_key,
        local_path,
        f"{node.ssh_user}@{node.host}:{remote_path}",
    ]
    logger.debug("SCP %s -> %s:%s", local_path, node.name, remote_path)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


def action_push(cfg: TestbedConfig) -> bool:
    """Save image locally, transfer to all nodes, and load."""
    image_ref = f"{cfg.image_name}:{cfg.image_tag}"
    logger.info("Saving image %s to tar archive ...", image_ref)

    with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp:
        tar_path = tmp.name

    try:
        result = subprocess.run(
            ["docker", "save", "-o", tar_path, image_ref],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            logger.error("docker save failed: %s", result.stderr.strip())
            return False

        tar_size_mb = os.path.getsize(tar_path) / (1024 * 1024)
        logger.info("Image archive: %.1f MB", tar_size_mb)

        ok = True
        for node in cfg.nodes:
            logger.info("Pushing to %s (%s) ...", node.name, node.host)
            remote_tar = f"/tmp/{cfg.image_name}.tar"

            # Transfer image
            r = _scp_to(node, tar_path, remote_tar)
            if r.returncode != 0:
                logger.error("SCP to %s failed: %s", node.name, r.stderr.strip())
                ok = False
                continue

            # Transfer compose file
            r = _scp_to(node, cfg.compose_file, f"/tmp/{os.path.basename(cfg.compose_file)}")
            if r.returncode != 0:
                logger.error("SCP compose file to %s failed: %s", node.name, r.stderr.strip())
                ok = False
                continue

            # Load image
            r = _run_ssh(node, f"docker load -i {remote_tar} && rm -f {remote_tar}", timeout=120)
            if r.returncode != 0:
                logger.error("docker load on %s failed: %s", node.name, r.stderr.strip())
                ok = False
            else:
                logger.info("Image loaded on %s.", node.name)

        return ok
    finally:
        os.unlink(tar_path)


def action_start(cfg: TestbedConfig) -> bool:
    """Start services on all nodes in parallel."""
    compose_basename = os.path.basename(cfg.compose_file)
    ok = True

    def start_node(node: NodeConfig) -> tuple[str, bool]:
        remote_compose = f"/tmp/{compose_basename}"
        cmd = (
            f"cd /tmp && "
            f"RESULTS_DIR={cfg.results_dir} "
            f"docker compose -f {remote_compose} up -d {node.compose_service}"
        )
        r = _run_ssh(node, cmd, timeout=60)
        if r.returncode != 0:
            return node.name, False
        return node.name, True

    with ThreadPoolExecutor(max_workers=len(cfg.nodes)) as pool:
        futures = {pool.submit(start_node, n): n for n in cfg.nodes}
        for future in as_completed(futures):
            name, success = future.result()
            if success:
                logger.info("Started %s.", name)
            else:
                logger.error("Failed to start %s.", name)
                ok = False
    return ok


def action_stop(cfg: TestbedConfig) -> bool:
    """Stop services on all nodes."""
    compose_basename = os.path.basename(cfg.compose_file)
    ok = True
    for node in cfg.nodes:
        remote_compose = f"/tmp/{compose_basename}"
        r = _run_ssh(node, f"cd /tmp && docker compose -f {remote_compose} down", timeout=60)
        if r.returncode != 0:
            logger.error("Stop failed on %s: %s", node.name, r.stderr.strip())
            ok = False
        else:
            logger.info("Stopped %s.", node.name)
    return ok


def action_status(cfg: TestbedConfig) -> bool:
    """Print running containers on each node."""
    for node in cfg.nodes:
        r = _run_ssh(node, "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'")
        header = f"--- {node.name} ({node.host}) ---"
        print(header)
        if r.returncode != 0:
            print(f"  ERROR: {r.stderr.strip()}")
        else:
            output = r.stdout.strip()
            print(output if output else "  (no containers)")
        print()
    return True


def action_logs(cfg: TestbedConfig, node_name: str, tail: int = 50) -> bool:
    """Fetch recent logs from a specific node."""
    node = next((n for n in cfg.nodes if n.name == node_name), None)
    if node is None:
        logger.error("Unknown node '%s'. Available: %s", node_name, [n.name for n in cfg.nodes])
        return False

    compose_basename = os.path.basename(cfg.compose_file)
    remote_compose = f"/tmp/{compose_basename}"
    r = _run_ssh(
        node,
        f"cd /tmp && docker compose -f {remote_compose} logs --tail={tail} {node.compose_service}",
        timeout=30,
    )
    if r.returncode != 0:
        logger.error("Logs failed on %s: %s", node.name, r.stderr.strip())
        return False
    print(r.stdout)
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy Neural Pub/Sub stack to testbed nodes via SSH.",
    )
    parser.add_argument(
        "--config", required=True, help="Path to testbed-config.yaml.",
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["push", "start", "stop", "status", "logs"],
        help="Deployment action.",
    )
    parser.add_argument(
        "--node",
        default=None,
        help="Node name (required for 'logs' action).",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=50,
        help="Number of log lines to fetch (default: 50).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    cfg = load_config(args.config)
    logger.info("Loaded config with %d nodes.", len(cfg.nodes))

    action_map = {
        "push": lambda: action_push(cfg),
        "start": lambda: action_start(cfg),
        "stop": lambda: action_stop(cfg),
        "status": lambda: action_status(cfg),
        "logs": lambda: action_logs(cfg, args.node, args.tail),
    }

    if args.action == "logs" and not args.node:
        logger.error("--node is required for the 'logs' action.")
        sys.exit(1)

    t0 = time.time()
    success = action_map[args.action]()
    elapsed = time.time() - t0

    if success:
        logger.info("Action '%s' completed in %.1f s.", args.action, elapsed)
    else:
        logger.error("Action '%s' failed after %.1f s.", args.action, elapsed)
        sys.exit(1)


if __name__ == "__main__":
    main()
