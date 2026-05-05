"""Sharded-oracle broker: 4-broker centralised orchestrator with a designated leader.

Architecture
------------

The sharded-oracle is a fair process-count comparator for the federated
market. It runs the same 4-broker process topology as ``market-quad`` but
makes globally-optimal placement decisions like ``oracle-global``:

- One broker (``IS_COORDINATOR=true``, e.g. on VM1) is the **coordinator**:
  on each pipeline arrival it pulls peer state via HTTP, merges with its
  local state into a single ``NetworkTopology``, and calls the existing
  global solver ``find_placement`` over the merged topology.
- The remaining brokers are **state-owners**: they expose
  ``/sharded-oracle/state`` returning a snapshot of their local worker
  registry, current loads, slice membership, and governance constraints.
  Pipeline submissions arriving at a state-owner are forwarded to the
  coordinator (write-leader pattern).

This module exports the pure-function building blocks (``merge_topologies``,
``merge_governance``, ``topology_to_snapshot``, ``snapshot_to_topology``,
``decide_globally``) plus the role-detection helper ``is_coordinator_role``.
The broker class itself (HTTP endpoints, state pulls) is layered on top.
"""
from __future__ import annotations

import os
from typing import Iterable

from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
    find_placement,
)
from src.pipeline.dag import PipelineDAG


# --------------------------------------------------------------------------
# Role detection
# --------------------------------------------------------------------------


def is_coordinator_role() -> bool:
    """True if this broker process is the coordinator.

    Reads the ``IS_COORDINATOR`` env var. Case-insensitive; only the
    literal string "true" (any case) selects the coordinator role.
    Anything else (unset, "false", empty, "1") returns False.
    """
    value = os.environ.get("IS_COORDINATOR", "")
    return value.strip().lower() == "true"


# --------------------------------------------------------------------------
# State merging (pure functions)
# --------------------------------------------------------------------------


def merge_topologies(
    topologies: Iterable[NetworkTopology],
    cross_latencies: dict[tuple[str, str], float],
) -> NetworkTopology:
    """Merge multiple per-shard topologies into a single global topology.

    Each input topology represents one shard's view of its own workers.
    The cross-shard latencies (e.g. WAN_max ≈ 50 ms between sites) are
    injected explicitly; intra-shard latencies are preserved verbatim.

    Args:
        topologies: One ``NetworkTopology`` per shard.
        cross_latencies: ``{(node_a, node_b): latency_ms}`` for pairs
            that span shards. Symmetric pairs may be supplied either way.

    Returns:
        A single ``NetworkTopology`` containing every node across all
        shards plus the union of intra- and cross-shard latencies. The
        ``current_load`` of each node is preserved so the global solver
        sees live queue depth, not just static capacity.

    Raises:
        ValueError: If two shards report the same ``node_id``.
    """
    merged_nodes: list[ExecutionUnit] = []
    seen_ids: set[str] = set()
    merged_lat: dict[tuple[str, str], float] = {}

    for topo in topologies:
        for node in topo.nodes:
            if node.node_id in seen_ids:
                raise ValueError(
                    f"node_id collision across shards: '{node.node_id}'"
                )
            seen_ids.add(node.node_id)
            merged_nodes.append(node)
        for pair, lat in topo.latency_matrix.items():
            merged_lat[pair] = lat

    for pair, lat in cross_latencies.items():
        merged_lat[pair] = lat

    return NetworkTopology(nodes=merged_nodes, latency_matrix=merged_lat)


def merge_governance(
    policies: Iterable[GovernancePolicy],
) -> GovernancePolicy:
    """Merge per-shard governance policies into a single global policy.

    Locality constraints (``local_stage_types``) are unioned: any shard
    that flags a stage type as locality-constrained binds the global
    policy. Trust levels are intersected via ``min``: when shards report
    different trust between the same domain pair, the more conservative
    (lower) value wins, ensuring partial enforcement at one shard does
    not loosen the global policy.
    """
    union_local: set[str] = set()
    min_trust: dict[tuple[str, str], float] = {}

    for pol in policies:
        union_local |= set(pol.local_stage_types)
        for pair, lvl in pol.trust_levels.items():
            existing = min_trust.get(pair)
            if existing is None or lvl < existing:
                min_trust[pair] = lvl

    return GovernancePolicy(
        local_stage_types=union_local,
        trust_levels=min_trust,
    )


# --------------------------------------------------------------------------
# Snapshot serialisation (over the wire)
# --------------------------------------------------------------------------


def topology_to_snapshot(
    topo: NetworkTopology,
    worker_urls: dict[str, str] | None = None,
) -> dict:
    """Serialise a ``NetworkTopology`` to a JSON-friendly dict.

    Used by the ``/sharded-oracle/state`` endpoint to publish a state
    snapshot that the coordinator can pull and merge.

    If ``worker_urls`` is supplied (state-owner role: a mapping from
    ``node_id`` to the dispatch HTTP base URL of that worker), the
    mapping is included in the snapshot so the coordinator can dispatch
    stages to peer workers directly without a synchronous federation
    round-trip per stage.
    """
    return {
        "nodes": [
            {
                "node_id": n.node_id,
                "domain_id": n.domain_id,
                "slice_id": n.slice_id,
                "capacity": n.capacity,
                "current_load": n.current_load,
                "compute_times": n.compute_times,
            }
            for n in topo.nodes
        ],
        "latency_matrix": [
            {"a": a, "b": b, "ms": ms}
            for (a, b), ms in topo.latency_matrix.items()
        ],
        "worker_urls": [
            {"node_id": nid, "url": url}
            for nid, url in (worker_urls or {}).items()
        ],
    }


def snapshot_to_worker_urls(snap: dict) -> dict[str, str]:
    """Extract the ``{node_id: url}`` mapping from a peer state snapshot.

    Returns an empty dict if the snapshot was published without URLs
    (e.g. by a state-owner that has no live worker registry yet).
    """
    return {
        entry["node_id"]: entry["url"]
        for entry in snap.get("worker_urls", [])
    }


def snapshot_to_topology(snap: dict) -> NetworkTopology:
    """Deserialise a state snapshot received from a peer state-owner."""
    nodes = [
        ExecutionUnit(
            node_id=n["node_id"],
            domain_id=n["domain_id"],
            slice_id=n["slice_id"],
            capacity=n["capacity"],
            current_load=n.get("current_load", 0.0),
            compute_times=n.get("compute_times"),
        )
        for n in snap.get("nodes", [])
    ]
    latency_matrix: dict[tuple[str, str], float] = {
        (entry["a"], entry["b"]): entry["ms"]
        for entry in snap.get("latency_matrix", [])
    }
    return NetworkTopology(nodes=nodes, latency_matrix=latency_matrix)


# --------------------------------------------------------------------------
# Global decide flow
# --------------------------------------------------------------------------


def decide_globally(
    *,
    dag: PipelineDAG,
    local_topology: NetworkTopology,
    peer_topologies: Iterable[NetworkTopology],
    cross_latencies: dict[tuple[str, str], float],
    governance: GovernancePolicy,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
) -> dict[str, str]:
    """Run the global placement solver over the merged shard topology.

    This is the coordinator's atomic operation: given a freshly-pulled
    snapshot of every shard's state, build the merged
    ``NetworkTopology`` and call the existing ``find_placement``. The
    resulting ``{stage_id: node_id}`` dict assigns stages to any of the
    shard-owned workers; the coordinator dispatches each stage to the
    broker that owns the target node.

    Args:
        dag: The pipeline DAG to place.
        local_topology: The coordinator's own worker view.
        peer_topologies: Iterable of peer state-owners' snapshots.
        cross_latencies: Cross-shard pairwise latencies (e.g. WAN).
        governance: Merged governance policy from ``merge_governance``.
        alpha, beta, gamma: Cost-function weights (default 1.0 per
            ``configs/domain_d{1,2}.yaml``).

    Returns:
        ``{stage_id: node_id}`` over the merged worker pool.
    """
    merged_topology = merge_topologies(
        [local_topology, *peer_topologies], cross_latencies,
    )
    return find_placement(
        dag=dag,
        topology=merged_topology,
        governance=governance,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
    )
