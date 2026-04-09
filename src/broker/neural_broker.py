"""Neural Broker: HTTP API for the Neural Pub/Sub distribution architecture.

Implements a single broker node in the federated architecture (Section 4).
Each broker manages:
- A local Neural Router instance for semantic matching
- A set of registered workers (execution units)
- A placement engine for pipeline stage assignment
- Federation with peer brokers via subscription summaries

The broker is the orchestrator: it receives pipeline requests (publish),
matches them to subscriptions, computes placements, dispatches stages
to workers, and tracks execution via the measurement harness.

Usage:
    python -m src.broker.neural_broker --domain d1 --config configs/domain_d1.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request, Response

from src.broker.base import _build_dag
from src.broker.models import (
    HealthResponse,
    PipelineState,
    PublishRequest,
    PublishResponse,
    RegisterRequest,
    RegisterResponse,
    StageResultRequest,
    WorkerInfo,
)
from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
    find_placement,
    market_mode_placement,
    locality_placement,
    latency_greedy_placement,
    spillover_placement,
)
from src.federation.propagation import SummaryPropagator
from src.federation.summary import (
    ClusterSummary,
    SubscriptionSummary,
    deserialize,
    serialize,
)
from src.measurement.harness import (
    AdaptationTracker,
    AggregateMetrics,
    FederationMonitor,
    MetricsCollector,
    TimestampRecord,
)
from src.pipeline.dag import PipelineDAG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class BrokerConfig:
    """Configuration for a NeuralBroker instance.

    Attributes:
        domain_id: Data-sovereignty domain this broker belongs to.
        broker_id: Unique identifier for this broker instance.
        host: Bind address for the HTTP server.
        port: Listen port for the HTTP server.
        alpha: Latency weight in the placement cost function (Eq. 10).
        beta: Load-balance weight in the placement cost function (Eq. 10).
        gamma: Domain-crossing weight in the placement cost function (Eq. 10).
        peer_urls: Base URLs of federation peer brokers.
        summary_interval_s: Interval (delta_prop) between summary propagation
            rounds in seconds.
        local_stage_types: Stage types that must not leave this domain.
        trust_levels: Mapping from peer domain_id to trust level in [0, 1].
    """

    domain_id: str
    broker_id: str
    host: str = "0.0.0.0"
    port: int = 8080
    # Placement weights (Eq. 10)
    alpha: float = 1.0
    beta: float = 1.0
    gamma: float = 1.0
    # Federation
    peer_urls: list[str] = field(default_factory=list)
    summary_interval_s: float = 10.0
    # Health monitoring
    health_check_interval_s: float = 5.0
    health_check_max_failures: int = 3
    # Governance
    governance_enabled: bool = False
    local_stage_types: list[str] = field(default_factory=list)
    trust_levels: dict[str, float] = field(default_factory=dict)
    # Placement mode
    placement_mode: str = "neural"  # "neural", "market", "locality", "latency", "spillover"
    # WAN cost for market-mode cross-domain pricing
    wan_cost_ms: float = 0.0
    # Transport (dual-transport factorial experiment)
    transport: str = "http"  # "http" or "kafka"
    kafka_bootstrap: str | None = None
    # Market mode load-aware worker selection (ablation only).
    # When True, market_mode_placement picks the least-loaded feasible
    # worker within a domain. Default False preserves the main campaign's
    # market behaviour for reproducibility.
    market_load_aware: bool = False


def load_config(path: str) -> BrokerConfig:
    """Load a BrokerConfig from a YAML file.

    The YAML file may contain any subset of BrokerConfig fields. Missing
    fields fall back to dataclass defaults. The ``domain_id`` and
    ``broker_id`` fields are required.

    Expected YAML structure::

        domain_id: d1
        broker_id: broker-d1-0
        host: "0.0.0.0"
        port: 8080
        alpha: 1.0
        beta: 1.0
        gamma: 1.0
        peer_urls:
          - http://broker-d2:8080
        summary_interval_s: 10.0
        local_stage_types:
          - collect
        trust_levels:
          d2: 0.8
          d3: 0.6

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A populated BrokerConfig.

    Raises:
        ValueError: If required fields (domain_id, broker_id) are absent.
        FileNotFoundError: If the file does not exist.
    """
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if "domain_id" not in raw:
        raise ValueError(f"Config '{path}' is missing required field 'domain_id'.")
    if "broker_id" not in raw:
        raise ValueError(f"Config '{path}' is missing required field 'broker_id'.")

    return BrokerConfig(
        domain_id=raw["domain_id"],
        broker_id=raw["broker_id"],
        host=raw.get("host", "0.0.0.0"),
        port=int(raw.get("port", 8080)),
        alpha=float(raw.get("alpha", 1.0)),
        beta=float(raw.get("beta", 1.0)),
        gamma=float(raw.get("gamma", 1.0)),
        peer_urls=list(raw.get("peer_urls", [])),
        summary_interval_s=float(raw.get("summary_interval_s", 10.0)),
        health_check_interval_s=float(raw.get("health_check_interval_s", 5.0)),
        health_check_max_failures=int(raw.get("health_check_max_failures", 3)),
        local_stage_types=list(raw.get("local_stage_types", [])),
        trust_levels={str(k): float(v) for k, v in raw.get("trust_levels", {}).items()},
        wan_cost_ms=float(raw.get("wan_cost_ms", 0.0)),
    )


# WorkerInfo, PipelineState imported from src.broker.models


# _PIPELINE_FACTORIES and _build_dag imported from src.broker.base


# Pydantic models imported from src.broker.models


# ---------------------------------------------------------------------------
# NeuralBroker
# ---------------------------------------------------------------------------


class NeuralBroker:
    """Central broker node for the Neural Pub/Sub federated architecture.

    Owns a local worker registry, placement engine, federation layer, and
    measurement harness. Exposes all functionality through a FastAPI HTTP
    application built by ``build_app()``.

    Lifecycle::

        broker = NeuralBroker(config)
        app = broker.build_app()
        # app is passed to uvicorn; startup/shutdown hooks handle background tasks

    Internal state is protected by ``asyncio.Lock`` where concurrent access
    is possible (worker registry, active pipelines).
    """

    def __init__(self, config: BrokerConfig) -> None:
        self.config = config

        # Worker registry: node_id -> WorkerInfo
        self._workers: dict[str, WorkerInfo] = {}
        self._workers_lock = asyncio.Lock()

        # Network topology (rebuilt on each worker register/deregister)
        self._topology: NetworkTopology = NetworkTopology(nodes=[], latency_matrix={})

        # Governance policy derived from config.  When governance is disabled
        # (the default), use an empty policy so placement ignores constraints.
        if config.governance_enabled:
            self._governance = GovernancePolicy(
                local_stage_types=set(config.local_stage_types),
                trust_levels={
                    (config.domain_id, peer): level
                    for peer, level in config.trust_levels.items()
                },
            )
        else:
            self._governance = GovernancePolicy(
                local_stage_types=set(),
                trust_levels={},
            )

        # Active pipelines: pipeline_id -> PipelineState
        self._active_pipelines: dict[str, PipelineState] = {}
        self._pipelines_lock = asyncio.Lock()

        # Measurement harness
        self._metrics = MetricsCollector()
        self._federation_monitor = FederationMonitor()
        self._metrics.set_federation_monitor(self._federation_monitor)
        self._adaptation_tracker = AdaptationTracker()

        # Health monitoring: consecutive failure count per worker
        self._worker_failures: dict[str, int] = {}
        self._health_check_task: asyncio.Task[None] | None = None

        # Long-lived HTTP client (created in startup, closed in shutdown)
        self._http_client: httpx.AsyncClient | None = None

        # Kafka producer (created in startup if transport='kafka')
        self._producer = None

        # Federation layer
        self._propagator = SummaryPropagator(
            domain_id=config.domain_id,
            peers=config.peer_urls,
            interval_seconds=config.summary_interval_s,
        )
        # Peer summaries cache: domain_id -> list[SubscriptionSummary]
        self._peer_summaries: dict[str, list[SubscriptionSummary]] = {}

    # ------------------------------------------------------------------
    # Dual-transport dispatch helper
    # ------------------------------------------------------------------

    async def _send_to_worker(
        self,
        worker_url: str,
        payload: dict,
        *,
        worker_id: str | None = None,
        topic: str | None = None,
    ) -> None:
        """Send a stage execution request to a worker via HTTP or Kafka.

        Args:
            worker_url: The worker's base URL (e.g. "http://worker:8081").
            payload: The stage execution payload.
            worker_id: Worker node ID (used in Kafka message for consumer routing).
            topic: Kafka topic (pipeline_type). Required if transport='kafka'.
        """
        if self.config.transport == "kafka":
            msg = {**payload, "target_url": worker_url, "target_worker": worker_id or ""}
            await self._producer.send_and_wait(topic or "default", value=msg)
        else:
            url = f"{worker_url.rstrip('/')}/execute"
            resp = await self._http_client.post(url, json=payload, timeout=30.0)
            resp.raise_for_status()

    # ------------------------------------------------------------------
    # Topology helpers
    # ------------------------------------------------------------------

    def _rebuild_topology(self) -> None:
        """Rebuild the NetworkTopology from the current worker registry.

        Assigns zero intra-domain latency and a fixed cross-domain latency
        (10 ms) when workers share a domain, and a larger default (50 ms)
        otherwise. In a real deployment this would be replaced with measured
        RTT values from a network monitor.

        This method is called every time a worker registers or deregisters.
        It is not async-safe by itself; callers must hold ``_workers_lock``
        or ensure single-threaded access.
        """
        nodes = [
            ExecutionUnit(
                node_id=w.node_id,
                domain_id=w.domain_id,
                slice_id=w.slice_id,
                capacity=w.capacity,
                current_load=w.current_load,
            )
            for w in self._workers.values()
        ]

        latency_matrix: dict[tuple[str, str], float] = {}
        node_list = list(self._workers.values())
        for i, a in enumerate(node_list):
            for j, b in enumerate(node_list):
                if i >= j:
                    continue
                # Same-domain -> low latency; cross-domain -> higher latency
                if a.domain_id == b.domain_id:
                    latency = 2.0
                else:
                    latency = 20.0
                latency_matrix[(a.node_id, b.node_id)] = latency

        self._topology = NetworkTopology(nodes=nodes, latency_matrix=latency_matrix)
        logger.debug(
            "Topology rebuilt: %d nodes, %d latency pairs.",
            len(nodes),
            len(latency_matrix),
        )
        # Update federation summary with new capacity information
        self._update_and_propagate_summary()

    def _build_capacity_summary(self) -> SubscriptionSummary:
        """Build a capacity-based SubscriptionSummary from registered workers.

        Groups workers by slice and creates one ClusterSummary per slice
        with a zero-vector centroid embedding (capacity-only summary, no
        semantic content). The available_capacity is the sum of spare
        capacity across all workers in that slice.

        This summary is used for federation: peer brokers can see how much
        capacity this domain has, enabling cross-domain pipeline forwarding.
        """
        import numpy as np

        # Group workers by slice_id
        slices: dict[str, float] = {}
        for w in self._workers.values():
            spare = max(0.0, w.capacity - w.current_load)
            slices[w.slice_id] = slices.get(w.slice_id, 0.0) + spare

        # Build one ClusterSummary per slice
        # Use a zero-vector centroid (capacity-only, no semantic matching)
        dim = 384  # all-MiniLM-L6-v2 embedding dimension
        clusters = [
            ClusterSummary(
                cluster_id=f"slice-{slice_id}",
                centroid_embedding=np.zeros(dim, dtype=np.float32),
                radius=1.0,  # broad radius: accept any semantic match
                available_capacity=capacity,
            )
            for slice_id, capacity in slices.items()
        ]

        return SubscriptionSummary(
            domain_id=self.config.domain_id,
            clusters=clusters,
            timestamp=time.time(),
        )

    def _update_and_propagate_summary(self) -> None:
        """Rebuild the capacity summary and push to the propagator.

        Called after any worker register/deregister or load change that
        affects the domain's available capacity.
        """
        summary = self._build_capacity_summary()
        self._propagator.update_local_summary(summary)
        logger.debug(
            "Updated local summary: %d clusters, total capacity %.2f.",
            len(summary.clusters),
            sum(c.available_capacity for c in summary.clusters),
        )

    def _update_worker_load(self, node_id: str, delta: float) -> None:
        """Adjust the current_load of a worker and resync the topology node.

        Args:
            node_id: Worker to update.
            delta: Load change (positive = more load, negative = released load).
        """
        worker = self._workers.get(node_id)
        if worker is None:
            return
        worker.current_load = max(0.0, worker.current_load + delta)
        # Update the corresponding ExecutionUnit in the topology
        for eu in self._topology.nodes:
            if eu.node_id == node_id:
                eu.current_load = worker.current_load
                break

    # ------------------------------------------------------------------
    # Placement dispatch (Task 4)
    # ------------------------------------------------------------------

    def _compute_clearing_prices_from(
        self, workers: dict[str, WorkerInfo],
    ) -> dict[str, dict[str, float]]:
        """Compute market clearing prices from a workers snapshot.

        Each worker bids on all known stage types with its bid_cost_ms.
        Demand per stage type is estimated as 1 (one stage execution per
        pipeline arrival). The clearing price = marginal cost at the
        demand quantity within each (domain, stage_type) group.

        Args:
            workers: Snapshot of the worker registry to compute bids from.

        Returns:
            {domain_id: {stage_type: clearing_price}}
        """
        from src.broker.market import WorkerBid, compute_clearing_prices

        known_stage_types = self._known_stage_types()

        bids = []
        for w in workers.values():
            for st in known_stage_types:
                bid = WorkerBid(
                    worker_id=w.node_id,
                    domain_id=w.domain_id,
                    stage_type=st,
                    compute_ms=w.bid_cost_ms,
                    cost_per_stage=w.bid_cost_ms,
                )
                bids.append(bid)

        demand = {st: 1 for st in known_stage_types}
        return compute_clearing_prices(bids, demand)

    def _compute_clearing_prices(self) -> dict[str, dict[str, float]]:
        """Compute market clearing prices from the live worker registry.

        Convenience wrapper around _compute_clearing_prices_from that
        reads self._workers. Callers that already hold a workers snapshot
        should use _compute_clearing_prices_from directly.
        """
        return self._compute_clearing_prices_from(self._workers)

    def _known_stage_types(self) -> set[str]:
        """Return the set of stage types from registered pipeline templates.

        Falls back to a default set if no pipelines have been submitted yet.
        """
        stage_types: set[str] = set()
        pipelines = getattr(self, "_pipelines", {})
        for ps in pipelines.values():
            if hasattr(ps, "dag") and ps.dag is not None:
                for stage in ps.dag.stages.values():
                    stage_types.add(stage.stage_type)
        if not stage_types:
            # Default stage types from the 3 O-RAN pipeline templates
            stage_types = {
                "data_collect", "preprocess", "feature_extract",
                "predict", "detect", "fuse", "aggregate", "report",
            }
        return stage_types

    def _dispatch_placement_on(
        self,
        dag,
        topology: NetworkTopology,
        governance: GovernancePolicy,
        workers: dict[str, WorkerInfo],
    ) -> dict[str, str] | None:
        """Route placement to the correct algorithm using snapshot state.

        All placement algorithms receive the snapshotted topology,
        governance, and workers rather than reading self._* directly.
        This allows the caller to release _workers_lock before calling
        this method, eliminating lock contention on the publish hot path.

        Args:
            dag: The pipeline DAG to place.
            topology: Snapshotted network topology.
            governance: Snapshotted governance policy.
            workers: Snapshotted worker registry (for market clearing prices).

        Returns:
            Placement dict {stage_id: node_id} or None if rejected (market mode).

        Raises:
            RuntimeError: If no feasible placement exists (neural/heuristic modes).
        """
        mode = self.config.placement_mode

        if mode == "market":
            prices = self._compute_clearing_prices_from(workers)
            return market_mode_placement(
                dag,
                topology,
                governance,
                prices,
                self.config.wan_cost_ms,
                self.config.domain_id,
                load_aware=self.config.market_load_aware,
            )
        elif mode == "locality":
            return locality_placement(
                dag, topology, governance, self.config.domain_id,
            )
        elif mode == "latency":
            return latency_greedy_placement(
                dag, topology, governance,
            )
        elif mode == "spillover":
            return spillover_placement(
                dag, topology, governance, self.config.domain_id,
            )
        else:
            return find_placement(
                dag=dag,
                topology=topology,
                governance=governance,
                alpha=self.config.alpha,
                beta=self.config.beta,
                gamma=self.config.gamma,
            )

    def _dispatch_placement(self, dag) -> dict[str, str] | None:
        """Route placement using live state (convenience wrapper).

        Delegates to _dispatch_placement_on with self._topology,
        self._governance, and self._workers. Used by failure-recovery
        paths that already hold _workers_lock.
        """
        return self._dispatch_placement_on(
            dag, self._topology, self._governance, dict(self._workers),
        )

    # ------------------------------------------------------------------
    # Health monitoring
    # ------------------------------------------------------------------

    async def _health_check_loop(self) -> None:
        """Periodically ping every registered worker's GET /health endpoint.

        Workers that fail ``health_check_max_failures`` consecutive health
        checks are considered dead and removed from the registry. In-flight
        pipelines with stages on the dead worker are re-placed onto surviving
        workers when possible.
        """
        interval = self.config.health_check_interval_s
        max_failures = self.config.health_check_max_failures

        while True:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

            # Snapshot current workers under lock
            async with self._workers_lock:
                workers_snapshot = dict(self._workers)

            dead_workers: list[str] = []

            async def _probe(nid: str, w: WorkerInfo) -> tuple[str, bool]:
                """Probe a single worker; return (node_id, healthy)."""
                try:
                    resp = await self._http_client.get(
                        f"{w.url.rstrip('/')}/health", timeout=2.0
                    )
                    resp.raise_for_status()
                    return nid, True
                except Exception:
                    return nid, False

            # --- Concurrent health probes ---
            # All registered workers are probed in parallel via
            # asyncio.gather.  This keeps the total probe time bounded by
            # the slowest single response (plus the 2 s timeout) rather
            # than growing linearly with the number of workers.
            results = await asyncio.gather(
                *(_probe(nid, w) for nid, w in workers_snapshot.items())
            )

            # --- Consecutive-failure tracking ---
            # A successful probe resets the failure counter to zero.  Each
            # failed probe increments the counter.  Once a worker reaches
            # `max_failures` consecutive failures it is declared dead.
            # Using a consecutive (not cumulative) counter avoids false
            # positives from transient network blips.
            for node_id, healthy in results:
                if healthy:
                    self._worker_failures.pop(node_id, None)
                else:
                    count = self._worker_failures.get(node_id, 0) + 1
                    self._worker_failures[node_id] = count
                    logger.warning(
                        "Health check failed for worker '%s' (%d/%d).",
                        node_id,
                        count,
                        max_failures,
                    )
                    if count >= max_failures:
                        dead_workers.append(node_id)

            # Remove dead workers and trigger re-placement for any
            # in-flight pipelines that had stages on those workers.
            for node_id in dead_workers:
                await self._remove_dead_worker(node_id)

    async def _remove_dead_worker(self, node_id: str) -> None:
        """Remove a worker that has failed health checks and re-place its stages.

        Args:
            node_id: The dead worker's node_id.
        """
        logger.error(
            "Worker '%s' declared dead after %d consecutive health check failures.",
            node_id,
            self.config.health_check_max_failures,
        )
        self._worker_failures.pop(node_id, None)

        # Record a failure event so the AdaptationTracker can later compute
        # the time delta between failure detection and successful recovery.
        self._adaptation_tracker.record_failure("worker_health", node_id, time.time())

        # Detection is now complete; record this boundary so that
        # detection time and re-placement time can be measured separately.
        self._adaptation_tracker.record_detection_complete(
            "worker_health", node_id, time.time()
        )

        # Remove the dead worker from the registry and rebuild the
        # NetworkTopology so subsequent placement calls exclude it.
        async with self._workers_lock:
            removed = self._workers.pop(node_id, None)
            if removed is not None:
                self._rebuild_topology()

        # Scan active pipelines for stages assigned to the dead worker and
        # attempt to re-place them onto surviving workers.
        await self._replace_failed_stages(node_id)

    async def _replace_failed_stages(self, dead_node_id: str) -> None:
        """Re-place pipeline stages that were assigned to a dead worker.

        For each active (non-completed, non-failed) pipeline with stages on
        the dead worker, attempts to re-compute placement for the incomplete
        stages using the remaining workers. If re-placement succeeds, updates
        the placement and dispatches the re-placed stages. If it fails, marks
        the pipeline as failed.

        When FUNNEL_BYPASS_REPLACE is active, stages that are predecessors of
        fan-in (funnel) stages are NOT re-placed. This lets the dead worker
        remain in the placement map so that _find_ready_stages can detect it
        and invoke apply_funnel_policy with the actual funnel mode.

        Args:
            dead_node_id: The node_id of the dead worker.
        """
        from src.broker.funnel_resilience import (
            find_funnel_predecessor_stages,
            get_funnel_bypass_replace,
        )

        bypass = get_funnel_bypass_replace()

        # Collect affected pipelines under lock
        async with self._pipelines_lock:
            affected: list[PipelineState] = [
                ps
                for ps in self._active_pipelines.values()
                if not ps.failed
                and dead_node_id in ps.placement.values()
            ]

        for ps in affected:
            # Identify stages on the dead worker that have not yet completed.
            # Already-completed stages do not need re-placement because their
            # results have already been collected.
            dead_stages = [
                sid
                for sid, nid in ps.placement.items()
                if nid == dead_node_id and sid not in ps.completed_stages
            ]
            if not dead_stages:
                continue

            # When bypass is active, exclude stages that are predecessors of
            # fan-in stages from re-placement.
            if bypass:
                funnel_preds = find_funnel_predecessor_stages(ps.dag)
                stages_to_replace = [
                    sid for sid in dead_stages if sid not in funnel_preds
                ]
            else:
                stages_to_replace = dead_stages

            # --- Re-placement strategy ---
            # Invoke the full DAG placement solver (find_placement) on the
            # *complete* DAG, not just the failed stages.  This is necessary
            # because the solver requires the full graph topology (edges,
            # latency bounds, sovereignty labels) to produce a feasible
            # assignment.  Only the assignments for `stages_to_replace` are
            # extracted from the result; all other stages keep their current
            # placement.  Passing a partial sub-graph would require
            # constraint-preserving extraction (future work).
            replaced = False
            if stages_to_replace:
                async with self._workers_lock:
                    if self._topology.nodes:
                        try:
                            new_placement = find_placement(
                                dag=ps.dag,
                                topology=self._topology,
                                governance=self._governance,
                                alpha=self.config.alpha,
                                beta=self.config.beta,
                                gamma=self.config.gamma,
                            )
                            # Cherry-pick only the stages to replace from the new solution.
                            for sid in stages_to_replace:
                                ps.placement[sid] = new_placement[sid]
                            replaced = True
                        except RuntimeError as exc:
                            logger.warning(
                                "Re-placement failed for pipeline '%s': %s",
                                ps.pipeline_id,
                                exc,
                            )
            else:
                # All dead stages are funnel predecessors and bypass is active;
                # nothing to re-place, but this is intentional (not a failure).
                replaced = True

            if replaced:
                logger.info(
                    "Re-placed stages %s of pipeline '%s' (dead worker: '%s').",
                    dead_stages,
                    ps.pipeline_id,
                    dead_node_id,
                )
                # Record recovery so the AdaptationTracker can compute the
                # failure-to-recovery time delta.
                self._adaptation_tracker.record_recovery(
                    "worker_health", dead_node_id, time.time()
                )
                # Dispatch the re-placed stages whose predecessors are done.
                await self._dispatch_ready_stages(ps)
            else:
                # --- Pipeline failure path ---
                # When re-placement fails (e.g., no surviving node satisfies
                # the constraint set), the pipeline is marked as permanently
                # failed and removed from the active set.  The metrics
                # collector records a failed completion for aggregate stats.
                async with self._pipelines_lock:
                    ps.failed = True
                    ps.error = (
                        f"Worker '{dead_node_id}' died; re-placement failed "
                        f"for stages {dead_stages}."
                    )
                    self._active_pipelines.pop(ps.pipeline_id, None)
                await self._metrics.complete_pipeline(
                    ps.pipeline_id, success=False, error=ps.error
                )
                logger.error(
                    "Pipeline '%s' failed: no re-placement for stages %s.",
                    ps.pipeline_id,
                    dead_stages,
                )

    # ------------------------------------------------------------------
    # Federation forwarding
    # ------------------------------------------------------------------

    async def _try_federation_forward(
        self, req: "PublishRequest"
    ) -> Optional["PublishResponse"]:
        """Attempt to forward a pipeline request to a federation peer.

        Tries each peer broker in order. Returns a ``PublishResponse`` if a
        peer accepts the pipeline, or ``None`` if all peers fail.

        The request is tagged with ``__forwarded_from`` to prevent infinite
        forwarding loops between peers.
        """
        for peer_url in self._propagator.peers:
            try:
                fwd_config = dict(req.config)
                fwd_config["__forwarded_from"] = self.config.domain_id
                resp = await self._http_client.post(
                    f"{peer_url.rstrip('/')}/publish",
                    json={
                        "pipeline_type": req.pipeline_type,
                        "config": fwd_config,
                    },
                    timeout=30.0,
                )
                if resp.status_code == 200:
                    result = resp.json()
                    self._federation_monitor.record_summary_sent(
                        size_bytes=len(resp.content),
                        from_domain=self.config.domain_id,
                        to_domain=result.get("placement", {}).get(
                            "__domain__", peer_url
                        ),
                    )
                    logger.info(
                        "Pipeline forwarded to peer '%s': pipeline_id=%s",
                        peer_url,
                        result.get("pipeline_id"),
                    )
                    return PublishResponse(
                        pipeline_id=result["pipeline_id"],
                        placement=result.get("placement", {}),
                        status="forwarded",
                    )
                else:
                    logger.debug(
                        "Peer '%s' rejected forwarded pipeline: HTTP %d %s",
                        peer_url,
                        resp.status_code,
                        resp.text[:200],
                    )
            except Exception as exc:
                logger.warning(
                    "Federation forward to '%s' failed: %r",
                    peer_url,
                    exc,
                )
        return None

    # ------------------------------------------------------------------
    # Stage dispatch
    # ------------------------------------------------------------------

    async def _dispatch_stage(
        self,
        pipeline_state: PipelineState,
        stage_id: str,
        http_client: httpx.AsyncClient,
    ) -> None:
        """Send a stage execution request to the assigned worker.

        Records a 'dispatched' timestamp in the MetricsCollector and fires a
        POST /execute to the worker. Failures are logged and mark the pipeline
        as failed; they do not raise so that other concurrent dispatches can
        proceed.

        Args:
            pipeline_state: The owning pipeline's state.
            stage_id: The stage to dispatch.
            http_client: Shared async HTTP client for this dispatch round.
        """
        node_id = pipeline_state.placement[stage_id]
        worker = self._workers.get(node_id)
        if worker is None:
            logger.error(
                "Cannot dispatch stage '%s': worker '%s' not found.",
                stage_id,
                node_id,
            )
            async with self._pipelines_lock:
                pipeline_state.failed = True
                pipeline_state.error = f"Worker '{node_id}' not registered."
                self._active_pipelines.pop(pipeline_state.pipeline_id, None)
            return

        stage = pipeline_state.dag.get_stage(stage_id)

        # Record dispatch timestamp
        await self._metrics.record(
            TimestampRecord(
                pipeline_id=pipeline_state.pipeline_id,
                stage_id=stage_id,
                event="dispatched",
                timestamp=time.time(),
                node_id=node_id,
                metadata={"pipeline_type": pipeline_state.pipeline_type},
            )
        )

        payload = {
            "pipeline_id": pipeline_state.pipeline_id,
            "stage_id": stage_id,
            "stage_type": stage.stage_type,
            "computational_demand": stage.computational_demand,
            "input_data": "",
            "metadata": {
                "broker_id": self.config.broker_id,
                "pipeline_type": pipeline_state.pipeline_type,
            },
        }

        # Reserve load at dispatch time (not placement time).
        # Released in stage_result handler via _update_worker_load(-demand).
        async with self._workers_lock:
            self._update_worker_load(node_id, stage.computational_demand)

        try:
            await self._send_to_worker(
                worker.url, payload,
                worker_id=node_id, topic=pipeline_state.pipeline_type,
            )
            logger.debug(
                "Dispatched stage '%s' (pipeline=%s) to worker '%s'.",
                stage_id,
                pipeline_state.pipeline_id,
                node_id,
            )
        except Exception as exc:
            # --- Dispatch failure: single-stage retry path ---
            # When a POST /execute fails (network error, worker crash, etc.),
            # the broker treats the worker as unreachable: it removes the
            # worker from the registry, rebuilds the topology, and attempts
            # a single re-placement + re-dispatch cycle for this stage.
            logger.warning(
                "Dispatch of stage '%s' to worker '%s' failed: %s. "
                "Attempting re-placement.",
                stage_id,
                node_id,
                exc,
            )
            # Do NOT evict the worker on a single dispatch failure.
            # Transient errors (network blips, connection pool contention,
            # slow responses) should not permanently remove workers.
            # The health check loop (with consecutive-failure threshold)
            # handles actual worker death.  Instead, increment the failure
            # counter so the health check is aware of the issue.
            async with self._workers_lock:
                count = self._worker_failures.get(node_id, 0) + 1
                self._worker_failures[node_id] = count

            self._adaptation_tracker.record_failure(
                "dispatch_fail", node_id, time.time()
            )
            self._adaptation_tracker.record_detection_complete(
                "dispatch_fail", node_id, time.time()
            )

            # Re-place using the full DAG (see _replace_failed_stages for
            # the rationale).  Only this single stage's assignment is
            # extracted from the new solution.
            re_placed = False
            async with self._workers_lock:
                if self._topology.nodes:
                    try:
                        new_placement = find_placement(
                            dag=pipeline_state.dag,
                            topology=self._topology,
                            governance=self._governance,
                            alpha=self.config.alpha,
                            beta=self.config.beta,
                            gamma=self.config.gamma,
                        )
                        new_node = new_placement[stage_id]
                        pipeline_state.placement[stage_id] = new_node
                        re_placed = True
                    except RuntimeError:
                        pass

            if re_placed:
                # Attempt a single re-dispatch to the replacement worker.
                # If this also fails, the pipeline is marked as failed below
                # (no further retry attempts).
                new_node_id = pipeline_state.placement[stage_id]
                new_worker = self._workers.get(new_node_id)
                if new_worker is not None:
                    logger.info(
                        "Re-placed stage '%s' to worker '%s'.", stage_id, new_node_id
                    )
                    self._adaptation_tracker.record_recovery(
                        "dispatch_fail", node_id, time.time()
                    )
                    try:
                        await self._send_to_worker(
                            new_worker.url, payload,
                            worker_id=new_node_id, topic=pipeline_state.pipeline_type,
                        )
                        return
                    except Exception as exc2:
                        logger.error(
                            "Re-dispatch of stage '%s' to '%s' also failed: %s",
                            stage_id,
                            new_node_id,
                            exc2,
                        )

            # Release load for this dispatched stage (reserved at dispatch time).
            async with self._workers_lock:
                self._update_worker_load(node_id, -stage.computational_demand)

            # Mark pipeline as permanently failed.
            async with self._pipelines_lock:
                pipeline_state.failed = True
                pipeline_state.error = (
                    f"Dispatch of stage '{stage_id}' to '{node_id}' failed "
                    f"and re-placement was unsuccessful: {exc}"
                )
                self._active_pipelines.pop(pipeline_state.pipeline_id, None)

    async def _dispatch_ready_stages(
        self,
        pipeline_state: PipelineState,
    ) -> None:
        """Dispatch all stages whose predecessors have completed.

        Uses the shared ``BaseBroker._find_ready_stages`` helper to consult
        the funnel resilience policy for fan-in stages with dead predecessors.

        Args:
            pipeline_state: The pipeline to advance.
        """
        from src.broker.base import BaseBroker

        if pipeline_state.failed:
            return

        # Determine dead workers (workers in placement but not in registry)
        dead_workers: set[str] = set()
        async with self._workers_lock:
            live_workers = set(self._workers.keys())
        placement_workers = set(pipeline_state.placement.values())
        dead_workers = placement_workers - live_workers

        ready, funnel_result = BaseBroker._find_ready_stages(
            pipeline_state, dead_workers=dead_workers,
        )

        # Handle funnel policy results
        if funnel_result is not None and funnel_result.pipeline_failed:
            error_msg = (
                f"funnel_{funnel_result.action}: pipeline failed due to "
                f"funnel resilience policy ({funnel_result.action} mode)"
            )
            async with self._pipelines_lock:
                pipeline_state.failed = True
                pipeline_state.error = error_msg
                self._active_pipelines.pop(pipeline_state.pipeline_id, None)
            await self._metrics.complete_pipeline(
                pipeline_state.pipeline_id, success=False, error=error_msg,
            )
            return

        if not ready:
            return

        # Track dispatched stages BEFORE dispatch (prevents re-dispatch
        # if a result callback arrives while dispatch is in-flight).
        async with self._pipelines_lock:
            pipeline_state.dispatched_stages.update(ready)

        tasks = [
            self._dispatch_stage(pipeline_state, sid, self._http_client)
            for sid in ready
        ]
        await asyncio.gather(*tasks)

    # ------------------------------------------------------------------
    # FastAPI application
    # ------------------------------------------------------------------

    def build_app(self) -> FastAPI:
        """Build and return the FastAPI application.

        Registers startup/shutdown lifecycle hooks that start and stop the
        ``SummaryPropagator`` background task.

        Returns:
            A fully configured FastAPI application.
        """
        app = FastAPI(
            title=f"NeuralBroker [{self.config.broker_id}]",
            description=(
                "Neural Pub/Sub broker: pipeline placement, worker dispatch, "
                "and federation layer."
            ),
            version="0.1.0",
        )

        # ------------------------------------------------------------------
        # Lifecycle hooks
        # ------------------------------------------------------------------

        @app.on_event("startup")
        async def _startup() -> None:
            # Connection pool sized for large worker pools (e.g., 48 workers
            # in oracle mode).  Default limits (20 keepalive) cause connection
            # pool exhaustion when concurrent stage dispatches and health
            # checks compete for connections.
            self._http_client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=100,
                ),
            )
            if self.config.transport == "kafka" and self.config.kafka_bootstrap:
                import json as _json
                from aiokafka import AIOKafkaProducer
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self.config.kafka_bootstrap,
                    value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
                )
                await self._producer.start()
                logger.info("Kafka producer started (bootstrap=%s).", self.config.kafka_bootstrap)
            await self._propagator.start()
            self._health_check_task = asyncio.create_task(
                self._health_check_loop()
            )
            logger.info(
                "NeuralBroker '%s' (domain=%s, transport=%s) started on %s:%d.",
                self.config.broker_id,
                self.config.domain_id,
                self.config.transport,
                self.config.host,
                self.config.port,
            )

        @app.on_event("shutdown")
        async def _shutdown() -> None:
            if self._health_check_task is not None:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass
                self._health_check_task = None
            await self._propagator.stop()
            if self._producer is not None:
                await self._producer.stop()
                self._producer = None
            if self._http_client is not None:
                await self._http_client.aclose()
                self._http_client = None
            logger.info("NeuralBroker '%s' shut down.", self.config.broker_id)

        # ------------------------------------------------------------------
        # POST /publish
        # ------------------------------------------------------------------

        @app.post("/publish", response_model=PublishResponse)
        async def publish(req: PublishRequest) -> PublishResponse:
            """Receive a pipeline request, compute placement, and dispatch.

            Steps:
                1. Build a PipelineDAG from the requested template.
                2. Compute placement via find_placement() (Eq. 10).
                3. Record a 'created' timestamp.
                4. Dispatch source stages to their assigned workers.

            Returns a pipeline_id, the computed placement, and status
            "dispatched" on success, or raises HTTP 422/503 on error.
            """
            # Step 1: Build DAG
            try:
                dag = _build_dag(req.pipeline_type, req.config)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

            # Resolve __local__ sovereignty domain to this broker's domain_id
            for stage in dag.stages.values():
                if stage.data_sovereignty_domain == "__local__":
                    stage.data_sovereignty_domain = self.config.domain_id

            # Step 2: Compute placement (local first, then federation)
            #
            # Snapshot-and-release: hold _workers_lock only long enough to
            # capture the topology, governance, and workers state. Release
            # the lock before computing placement, which may take O(|V|*|N|^2)
            # for the DP solver.  This eliminates lock contention at high
            # arrival rates where concurrent publishes would otherwise
            # serialize on the lock.
            placement = None
            local_error = None

            async with self._workers_lock:
                topo_snapshot = self._topology
                gov_snapshot = self._governance
                workers_snapshot = dict(self._workers)
                has_nodes = bool(topo_snapshot.nodes)

            if has_nodes:
                try:
                    placement = self._dispatch_placement_on(
                        dag, topo_snapshot, gov_snapshot, workers_snapshot,
                    )
                except RuntimeError as exc:
                    local_error = str(exc)
            else:
                local_error = "No workers registered; cannot compute placement."

            # Note: load reservation is done per-stage at dispatch time
            # (in _dispatch_stage), not per-pipeline at placement time.
            # Reserving all stages upfront exhausts capacity after ~25
            # concurrent pipelines (48 capacity / 1.9 demand per pipeline).
            # Per-stage reservation only blocks capacity for stages
            # actually being executed, allowing higher concurrency.

            # Step 2b: If local placement failed, try federation forwarding
            # Only forward if this is not already a forwarded request (prevent loops)
            is_forwarded = req.config.get("__forwarded_from") is not None
            if placement is None and self._propagator.peers and not is_forwarded:
                forwarded = await self._try_federation_forward(req)
                if forwarded is not None:
                    return forwarded

                # All peers failed or had no capacity
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"Local placement failed ({local_error}); "
                        f"federation forwarding also failed."
                    ),
                )

            if placement is None:
                raise HTTPException(status_code=503, detail=local_error)

            pipeline_id = str(uuid.uuid4())

            pipeline_state = PipelineState(
                pipeline_id=pipeline_id,
                pipeline_type=req.pipeline_type,
                dag=dag,
                placement=placement,
            )

            async with self._pipelines_lock:
                self._active_pipelines[pipeline_id] = pipeline_state

            # Step 3: Record creation timestamp
            await self._metrics.record(
                TimestampRecord(
                    pipeline_id=pipeline_id,
                    stage_id="__pipeline__",
                    event="created",
                    timestamp=time.time(),
                    node_id=self.config.broker_id,
                    metadata={"pipeline_type": req.pipeline_type},
                )
            )

            # Step 3b: Record placement completion timestamp
            await self._metrics.record(
                TimestampRecord(
                    pipeline_id=pipeline_id,
                    stage_id="__pipeline__",
                    event="placement_complete",
                    timestamp=time.time(),
                    node_id=self.config.broker_id,
                    metadata={"pipeline_type": req.pipeline_type},
                )
            )

            # Step 4a: Post-placement governance feasibility check
            # Use the same topology/governance snapshots from Step 2
            # (consistent with the state used for placement).
            from src.broker.placement import check_feasibility

            is_feasible, violations = check_feasibility(
                placement, dag, topo_snapshot, gov_snapshot
            )
            if not is_feasible:
                for v in violations:
                    self._metrics.record_governance_violation(
                        pipeline_id, "__placement__", v
                    )
                logger.warning(
                    "Pipeline '%s' has %d governance violation(s): %s",
                    pipeline_id,
                    len(violations),
                    violations,
                )

            # Step 4b: Log routing accuracy (deterministic routing = F1 1.0)
            self._metrics.record_routing_accuracy(pipeline_id, 1.0)

            # Step 5: Dispatch source stages (no predecessors)
            await _dispatch_ready_stages_for(pipeline_state)

            logger.info(
                "Pipeline '%s' (type=%s) dispatched with placement %s.",
                pipeline_id,
                req.pipeline_type,
                placement,
            )
            return PublishResponse(
                pipeline_id=pipeline_id,
                placement=placement,
                status="dispatched",
            )

        # Helper: thin wrapper so inner coroutines can call this
        async def _dispatch_ready_stages_for(ps: PipelineState) -> None:
            await self._dispatch_ready_stages(ps)

        # ------------------------------------------------------------------
        # POST /register
        # ------------------------------------------------------------------

        @app.get("/workers")
        async def workers() -> dict:
            """Return registered workers with URLs (for kafka-consumer sidecar)."""
            async with self._workers_lock:
                return {
                    nid: {"url": w.url, "domain_id": w.domain_id, "slice_id": w.slice_id}
                    for nid, w in self._workers.items()
                }

        @app.post("/register", response_model=RegisterResponse)
        async def register(req: RegisterRequest, request: Request) -> RegisterResponse:
            """Register a worker with this broker.

            Stores the worker in the registry and rebuilds the NetworkTopology.
            If the worker's ``url`` field is empty, the broker constructs a
            fallback URL from the request's client host and a default port
            (8081).
            """
            worker_url = req.url
            if not worker_url:
                client_host = (
                    request.client.host if request.client else "127.0.0.1"
                )
                worker_url = f"http://{client_host}:8081"

            async with self._workers_lock:
                if req.node_id in self._workers:
                    logger.debug("Re-registering existing worker '%s'.", req.node_id)
                self._workers[req.node_id] = WorkerInfo(
                    node_id=req.node_id,
                    domain_id=req.domain_id,
                    slice_id=req.slice_id,
                    capacity=req.capacity,
                    current_load=0.0,
                    url=worker_url,
                    bid_cost_ms=req.bid_cost_ms,
                )
                self._rebuild_topology()

            logger.info(
                "Worker '%s' registered (domain=%s, slice=%s, capacity=%.2f, url=%s).",
                req.node_id,
                req.domain_id,
                req.slice_id,
                req.capacity,
                worker_url,
            )
            return RegisterResponse(status="registered", node_id=req.node_id)

        # ------------------------------------------------------------------
        # DELETE /register/{node_id}
        # ------------------------------------------------------------------

        @app.delete("/register/{node_id}")
        async def deregister(node_id: str) -> dict:
            """Deregister a worker and rebuild the topology.

            Pipelines that had stages assigned to the removed node are marked
            as failed so that the caller can detect and react to the loss.
            """
            async with self._workers_lock:
                if node_id not in self._workers:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Worker '{node_id}' not registered.",
                    )
                del self._workers[node_id]
                self._rebuild_topology()

            # Mark affected active pipelines as failed
            async with self._pipelines_lock:
                for ps in self._active_pipelines.values():
                    if node_id in ps.placement.values() and not ps.failed:
                        ps.failed = True
                        ps.error = (
                            f"Worker '{node_id}' deregistered while pipeline "
                            f"'{ps.pipeline_id}' was in flight."
                        )
                        logger.warning(
                            "Pipeline '%s' failed: worker '%s' deregistered.",
                            ps.pipeline_id,
                            node_id,
                        )

            logger.info("Worker '%s' deregistered.", node_id)
            return {"status": "deregistered", "node_id": node_id}

        # ------------------------------------------------------------------
        # POST /result
        # ------------------------------------------------------------------

        @app.post("/result")
        async def result(req: StageResultRequest) -> dict:
            """Handle a stage completion report from a worker.

            Records timing information in the MetricsCollector, updates
            worker load, and dispatches the next ready stages. If all stages
            have completed the pipeline is finalised.

            Returns a brief acknowledgement dict.
            """
            # Record the moment the broker receives the stage result
            await self._metrics.record(
                TimestampRecord(
                    pipeline_id=req.pipeline_id,
                    stage_id=req.stage_id,
                    event="stage_result_received",
                    timestamp=time.time(),
                    node_id=req.node_id,
                    metadata={"pipeline_type": "__pending__"},
                )
            )

            async with self._pipelines_lock:
                ps = self._active_pipelines.get(req.pipeline_id)
                if ps is None:
                    logger.warning(
                        "Received result for unknown pipeline '%s'.", req.pipeline_id
                    )
                    return {"status": "unknown_pipeline", "pipeline_id": req.pipeline_id}

                if req.success:
                    ps.completed_stages.add(req.stage_id)
                else:
                    ps.failed = True
                    ps.error = req.error or f"Stage '{req.stage_id}' failed."
                    logger.error(
                        "Stage '%s' of pipeline '%s' failed: %s",
                        req.stage_id,
                        req.pipeline_id,
                        req.error,
                    )

            # Record stage timing
            await self._metrics.record(
                TimestampRecord(
                    pipeline_id=req.pipeline_id,
                    stage_id=req.stage_id,
                    event="stage_start",
                    timestamp=req.start_time,
                    node_id=req.node_id,
                    metadata={"pipeline_type": ps.pipeline_type},
                )
            )
            await self._metrics.record(
                TimestampRecord(
                    pipeline_id=req.pipeline_id,
                    stage_id=req.stage_id,
                    event="stage_end",
                    timestamp=req.end_time,
                    node_id=req.node_id,
                    metadata={"pipeline_type": ps.pipeline_type},
                )
            )

            # Release worker load
            async with self._workers_lock:
                stage = ps.dag.get_stage(req.stage_id)
                self._update_worker_load(req.node_id, -stage.computational_demand)

            # Check pipeline completion or advance
            if ps.failed:
                await self._metrics.complete_pipeline(
                    req.pipeline_id, success=False, error=ps.error
                )
                async with self._pipelines_lock:
                    self._active_pipelines.pop(req.pipeline_id, None)
                return {"status": "pipeline_failed", "pipeline_id": req.pipeline_id}

            if ps.completed_stages == ps.all_stages:
                # All stages done: record delivery and finalise
                await self._metrics.record(
                    TimestampRecord(
                        pipeline_id=req.pipeline_id,
                        stage_id=req.stage_id,
                        event="delivered",
                        timestamp=time.time(),
                        node_id=req.node_id,
                        metadata={"pipeline_type": ps.pipeline_type},
                    )
                )
                await self._metrics.complete_pipeline(req.pipeline_id, success=True)
                async with self._pipelines_lock:
                    self._active_pipelines.pop(req.pipeline_id, None)
                logger.info(
                    "Pipeline '%s' completed successfully.", req.pipeline_id
                )
                return {"status": "pipeline_complete", "pipeline_id": req.pipeline_id}

            # Advance: dispatch newly ready stages
            await _dispatch_ready_stages_for(ps)
            return {
                "status": "stage_recorded",
                "pipeline_id": req.pipeline_id,
                "stage_id": req.stage_id,
            }

        # ------------------------------------------------------------------
        # GET /metrics
        # ------------------------------------------------------------------

        @app.get("/metrics")
        async def metrics() -> dict:
            """Return current aggregate metrics as a JSON dict.

            Includes pipeline counts, latency percentiles, throughput, and
            federation bandwidth as collected by the MetricsCollector and
            FederationMonitor.
            """
            agg: AggregateMetrics = await self._metrics.compute_aggregate()
            fed_bytes = self._federation_monitor.total_bytes()
            return {
                "total_pipelines": agg.total_pipelines,
                "completed": agg.completed,
                "failed": agg.failed,
                "throughput_per_sec": agg.throughput_per_sec,
                "latency_mean_ms": agg.latency_mean_ms,
                "latency_p50_ms": agg.latency_p50_ms,
                "latency_p95_ms": agg.latency_p95_ms,
                "latency_p99_ms": agg.latency_p99_ms,
                "per_stage_latency_mean": agg.per_stage_latency_mean,
                "federation_bandwidth_bytes": fed_bytes,
                "adaptation_events": agg.adaptation_events,
                "adaptation_time_mean_ms": agg.adaptation_time_mean_ms,
                "domain_crossings_mean": agg.domain_crossings_mean,
            }

        # ------------------------------------------------------------------
        # POST /metrics/export  (write CSV to disk)
        # ------------------------------------------------------------------

        @app.post("/metrics/export")
        async def metrics_export(request: Request) -> dict:
            """Export per-pipeline metrics to a CSV file.

            Body: {"path": "/path/to/output.csv"}
            If no path is provided, writes to results/local/metrics.csv.
            """
            body = await request.json()
            path = body.get("path", "results/local/metrics.csv")
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            await self._metrics.export_csv(path)
            logger.info("Metrics exported to %s", path)
            return {"status": "exported", "path": path}

        # ------------------------------------------------------------------
        # POST /federation/summary  (receive from peer)
        # ------------------------------------------------------------------

        @app.post("/federation/summary")
        async def federation_summary_receive(request: Request) -> dict:
            """Accept a msgpack-encoded SubscriptionSummary from a peer broker.

            The ``SummaryPropagator`` in each peer pushes its local summary
            here (Content-Type: application/x-msgpack). The summary is stored
            in the peer cache and forwarded to the local propagator so it is
            available for federated routing.

            Returns a brief acknowledgement dict.
            """
            body = await request.body()
            if not body:
                raise HTTPException(status_code=400, detail="Empty body.")
            try:
                summary = deserialize(body)
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to deserialise summary: {exc}",
                ) from exc

            await self._propagator.receive_summary(summary)

            # Update local peer cache (list per domain for multi-summary support)
            domain = summary.domain_id
            self._peer_summaries.setdefault(domain, [])
            # Replace existing summary from the same domain
            self._peer_summaries[domain] = [summary]

            # Track federation bandwidth
            self._federation_monitor.record_summary_received(
                size_bytes=len(body),
                from_domain=summary.domain_id,
                to_domain=self.config.domain_id,
            )

            logger.debug(
                "Received federation summary from domain '%s' (%d bytes, %d clusters).",
                summary.domain_id,
                len(body),
                len(summary.clusters),
            )
            return {
                "status": "received",
                "from_domain": summary.domain_id,
                "clusters": len(summary.clusters),
            }

        # ------------------------------------------------------------------
        # GET /federation/summary  (serve to peers)
        # ------------------------------------------------------------------

        @app.get("/federation/summary")
        async def federation_summary_serve() -> Response:
            """Return this broker's local SubscriptionSummary as msgpack bytes.

            Peers may poll this endpoint to pull summaries rather than
            waiting for push propagation. Returns the most recently cached
            local summary set by ``SummaryPropagator.update_local_summary``.

            Returns HTTP 204 if no local summary has been computed yet.
            """
            local = self._propagator.local_summary
            if local is None:
                return Response(status_code=204)

            data = serialize(local)
            self._federation_monitor.record_summary_sent(
                size_bytes=len(data),
                from_domain=self.config.domain_id,
                to_domain="__pull__",
            )
            return Response(
                content=data,
                media_type="application/x-msgpack",
            )

        # ------------------------------------------------------------------
        # GET /health
        # ------------------------------------------------------------------

        @app.get("/health", response_model=HealthResponse)
        async def health() -> HealthResponse:
            """Return a compact health summary for monitoring / liveness probes."""
            async with self._workers_lock:
                n_workers = len(self._workers)
            async with self._pipelines_lock:
                n_active = len(self._active_pipelines)
            return HealthResponse(
                broker_id=self.config.broker_id,
                domain_id=self.config.domain_id,
                workers=n_workers,
                active_pipelines=n_active,
                status="ok",
            )

        return app


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Neural Pub/Sub broker node.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--domain",
        required=True,
        dest="domain_id",
        help="Domain ID for this broker.",
    )
    parser.add_argument(
        "--config",
        required=False,
        default=None,
        help="Path to a YAML config file. CLI flags override file values.",
    )
    parser.add_argument(
        "--broker-id",
        dest="broker_id",
        default=None,
        help="Unique broker identifier. Defaults to 'broker-<domain>-0'.",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host for the HTTP server.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Listen port for the HTTP server.",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=1.0,
        help="Latency weight for placement cost (Eq. 10).",
    )
    parser.add_argument(
        "--beta",
        type=float,
        default=1.0,
        help="Load-balance weight for placement cost (Eq. 10).",
    )
    parser.add_argument(
        "--gamma",
        type=float,
        default=1.0,
        help="Domain-crossing weight for placement cost (Eq. 10).",
    )
    parser.add_argument(
        "--peers",
        nargs="*",
        default=[],
        metavar="URL",
        help="Base URLs of federation peer brokers.",
    )
    parser.add_argument(
        "--summary-interval",
        type=float,
        default=float(os.environ.get("FEDERATION_INTERVAL_S", "10.0")),
        dest="summary_interval_s",
        help="Summary propagation interval in seconds (delta_prop).",
    )
    parser.add_argument(
        "--governance-enabled",
        action="store_true",
        default=os.environ.get("GOVERNANCE_ENABLED", "").lower() == "true",
        help="Enable governance constraints on placement.",
    )
    parser.add_argument(
        "--transport",
        default=os.environ.get("TRANSPORT", "http"),
        choices=["http", "kafka"],
        help="Dispatch transport: 'http' (direct) or 'kafka'.",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        help="Kafka bootstrap servers (only used with --transport kafka).",
    )
    parser.add_argument(
        "--placement-mode",
        default=os.environ.get("PLACEMENT_MODE", "neural"),
        choices=["neural", "market", "locality", "latency", "spillover"],
        help="Placement strategy: neural (S3), market (price-based), "
        "locality (local-only), latency (greedy), spillover (local+overflow).",
    )
    parser.add_argument(
        "--wan-cost",
        type=float,
        default=float(os.environ.get("WAN_COST_MS", "0.0")),
        dest="wan_cost_ms",
        help="WAN cost in ms for market-mode cross-domain pricing (default 0.0).",
    )
    parser.add_argument(
        "--market-load-aware",
        action="store_true",
        dest="market_load_aware",
        default=os.environ.get("MARKET_LOAD_AWARE", "false").lower() == "true",
        help="Enable load-aware worker selection in market mode (ablation only).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def _make_config_from_args(args: argparse.Namespace) -> BrokerConfig:
    """Merge YAML file (if provided) with CLI arguments.

    CLI flags take precedence over file values. Fields not specified in
    either source fall back to BrokerConfig defaults.
    """
    if args.config:
        cfg = load_config(args.config)
    else:
        broker_id = args.broker_id or f"broker-{args.domain_id}-0"
        cfg = BrokerConfig(domain_id=args.domain_id, broker_id=broker_id)

    # CLI overrides
    cfg.domain_id = args.domain_id
    if args.broker_id:
        cfg.broker_id = args.broker_id
    cfg.host = args.host
    cfg.port = args.port
    cfg.alpha = args.alpha
    cfg.beta = args.beta
    cfg.gamma = args.gamma
    if args.peers:
        cfg.peer_urls = args.peers
    cfg.summary_interval_s = args.summary_interval_s
    cfg.governance_enabled = args.governance_enabled
    cfg.placement_mode = args.placement_mode
    cfg.wan_cost_ms = args.wan_cost_ms
    cfg.market_load_aware = args.market_load_aware
    cfg.transport = args.transport
    cfg.kafka_bootstrap = args.kafka_bootstrap if args.transport == "kafka" else None

    return cfg


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = _make_config_from_args(args)
    broker = NeuralBroker(config)
    app = broker.build_app()

    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=args.log_level.lower(),
    )
