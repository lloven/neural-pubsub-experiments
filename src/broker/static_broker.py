"""Static Broker: round-robin and random baseline placement.

A fair baseline broker that uses the same infrastructure as NeuralBroker
(health checks, dispatch-time recovery, federation) but replaces the neural
placement engine with simple round-robin or random assignment.

After the fairness fix, the ONLY difference between S1/S2 (StaticBroker)
and S3 (NeuralBroker) is the placement algorithm.

Usage:
    python -m src.broker.static_broker --domain d1 --port 8080
    PLACEMENT=random python -m src.broker.static_broker --domain d1 --port 8080
"""

from __future__ import annotations

import argparse
import asyncio
import enum
import itertools
import logging
import os
import random
import time

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response

from src.broker.base import BaseBroker
from src.broker.models import PipelineState, WorkerInfo
from src.broker.placement import slice_matches
from src.federation.propagation import SummaryPropagator
from src.federation.summary import (
    ClusterSummary,
    SubscriptionSummary,
    deserialize,
    serialize,
)
from src.measurement.harness import TimestampRecord
from src.pipeline.dag import PipelineDAG

logger = logging.getLogger(__name__)


class PlacementStrategy(enum.Enum):
    """Supported static placement strategies."""

    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


class StaticBroker(BaseBroker):
    """Baseline broker with round-robin or random stage placement.

    Provides the same infrastructure as NeuralBroker for fair H6 comparison:
    - Health check loop (periodic ping, dead worker removal)
    - Dispatch-time recovery (evict dead worker, re-place on survivor)
    - Federation (SummaryPropagator, /federation/summary endpoints)

    The ONLY difference from NeuralBroker is the placement algorithm.
    """

    def __init__(
        self,
        domain_id: str,
        broker_id: str,
        placement: str | PlacementStrategy = PlacementStrategy.ROUND_ROBIN,
        *,
        transport: str = "http",
        kafka_bootstrap: str | None = None,
        peer_urls: list[str] | None = None,
        health_check_interval_s: float = 5.0,
        health_check_max_failures: int = 3,
        summary_interval_s: float = 10.0,
    ) -> None:
        super().__init__(domain_id, broker_id, transport=transport, kafka_bootstrap=kafka_bootstrap)
        if isinstance(placement, str):
            placement = PlacementStrategy(placement)
        self.placement = placement
        # Per-slice round-robin cycles: maps slice_requirement (or None) to
        # an itertools.cycle over eligible worker IDs.
        self._slice_cycles: dict[str | None, itertools.cycle] = {}
        # Legacy attribute kept for backward compat in case anything references it
        self._worker_cycle: itertools.cycle | None = None

        # Health monitoring (same mechanism as NeuralBroker)
        self._health_check_interval_s = health_check_interval_s
        self._health_check_max_failures = health_check_max_failures
        self._worker_failures: dict[str, int] = {}
        self._health_check_task: asyncio.Task[None] | None = None

        # Federation (same mechanism as NeuralBroker)
        self._propagator = SummaryPropagator(
            domain_id=domain_id,
            peers=peer_urls or [],
            interval_seconds=summary_interval_s,
        )
        self._peer_summaries: dict[str, list[SubscriptionSummary]] = {}

    # ------------------------------------------------------------------
    # Worker-change hook
    # ------------------------------------------------------------------

    def _on_worker_change(self) -> None:
        self._rebuild_cycle()

    def _rebuild_cycle(self) -> None:
        """Rebuild per-slice round-robin iterators from current workers.

        Each unique slice_requirement gets its own cycle containing only
        workers eligible for that slice (via slice_matches). The None key
        covers stages with no slice requirement (all workers eligible).
        """
        # Collect all distinct slice requirements we might encounter.
        # Always include None (no requirement) plus every worker's slice_id.
        slice_keys: set[str | None] = {None}
        for w in self._workers.values():
            if w.slice_id != "flat":
                slice_keys.add(w.slice_id)

        self._slice_cycles = {}
        for req in slice_keys:
            eligible = sorted(
                wid for wid, w in self._workers.items()
                if slice_matches(w.slice_id, req)
            )
            if eligible:
                self._slice_cycles[req] = itertools.cycle(eligible)

        # Legacy global cycle for backward compat
        all_ids = sorted(self._workers.keys())
        self._worker_cycle = itertools.cycle(all_ids) if all_ids else None

    # ------------------------------------------------------------------
    # Placement
    # ------------------------------------------------------------------

    def _eligible_workers(self, slice_requirement: str | None) -> list[str]:
        """Return sorted list of worker IDs eligible for the given slice."""
        return sorted(
            wid for wid, w in self._workers.items()
            if slice_matches(w.slice_id, slice_requirement)
        )

    def _pick_worker(self, slice_requirement: str | None = None) -> str:
        """Pick a worker for a stage with the given slice requirement.

        For round-robin: uses per-slice cycles.
        For random: picks uniformly from eligible workers.

        Raises RuntimeError if no eligible workers exist for the slice.
        """
        if not self._workers:
            raise RuntimeError("No workers registered.")

        if self.placement is PlacementStrategy.RANDOM:
            eligible = self._eligible_workers(slice_requirement)
            if not eligible:
                raise RuntimeError(
                    f"No eligible workers for slice '{slice_requirement}'."
                )
            return random.choice(eligible)

        # Round-robin: use per-slice cycle
        cycle = self._slice_cycles.get(slice_requirement)
        if cycle is None:
            # Cycle not pre-built for this requirement; build on demand
            eligible = self._eligible_workers(slice_requirement)
            if not eligible:
                raise RuntimeError(
                    f"No eligible workers for slice '{slice_requirement}'."
                )
            cycle = itertools.cycle(eligible)
            self._slice_cycles[slice_requirement] = cycle

        return next(cycle)

    def _compute_placement(self, dag: PipelineDAG) -> dict[str, str]:
        order = dag.topological_sort()
        placement: dict[str, str] = {}
        for stage_id in order:
            stage = dag.get_stage(stage_id)
            placement[stage_id] = self._pick_worker(stage.slice_requirement)
        return placement

    # ------------------------------------------------------------------
    # Health monitoring (same algorithm as NeuralBroker)
    # ------------------------------------------------------------------

    async def _health_check_loop(self) -> None:
        """Periodically ping every registered worker's GET /health endpoint.

        Workers that fail health_check_max_failures consecutive health checks
        are considered dead and removed from the registry. Same algorithm as
        NeuralBroker._health_check_loop for fair comparison.
        """
        interval = self._health_check_interval_s
        max_failures = self._health_check_max_failures

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

            # Concurrent health probes (same as NeuralBroker)
            results = await asyncio.gather(
                *(_probe(nid, w) for nid, w in workers_snapshot.items())
            )

            # Consecutive-failure tracking (same as NeuralBroker)
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

            # Remove dead workers
            for node_id in dead_workers:
                await self._remove_dead_worker(node_id)

    async def _remove_dead_worker(self, node_id: str) -> None:
        """Remove a worker that has failed health checks and re-place its stages.

        Same mechanism as NeuralBroker._remove_dead_worker.
        """
        logger.error(
            "Worker '%s' declared dead after %d consecutive health check failures.",
            node_id,
            self._health_check_max_failures,
        )
        self._worker_failures.pop(node_id, None)

        # Remove from registry
        async with self._workers_lock:
            removed = self._workers.pop(node_id, None)
            if removed is not None:
                self._on_worker_change()

        # Re-place affected pipeline stages
        await self._replace_failed_stages(node_id)

    async def _replace_failed_stages(self, dead_node_id: str) -> None:
        """Re-place pipeline stages assigned to a dead worker using static placement.

        Same recovery mechanism as NeuralBroker, but uses round-robin/random
        for re-placement instead of the neural placement solver.

        When FUNNEL_BYPASS_REPLACE is active, stages that are predecessors of
        fan-in (funnel) stages are NOT re-placed. This lets the dead worker
        remain in the placement map so that _find_ready_stages can detect it
        and invoke apply_funnel_policy with the actual funnel mode.
        """
        from src.broker.funnel_resilience import (
            find_funnel_predecessor_stages,
            get_funnel_bypass_replace,
        )

        bypass = get_funnel_bypass_replace()

        async with self._pipelines_lock:
            affected: list[PipelineState] = [
                ps
                for ps in self._active_pipelines.values()
                if not ps.failed
                and dead_node_id in ps.placement.values()
            ]

        for ps in affected:
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

            replaced = False
            if stages_to_replace:
                async with self._workers_lock:
                    if self._workers:
                        try:
                            for sid in stages_to_replace:
                                stage = ps.dag.get_stage(sid)
                                ps.placement[sid] = self._pick_worker(
                                    stage.slice_requirement
                                )
                            replaced = True
                        except RuntimeError:
                            pass
            else:
                # All dead stages are funnel predecessors and bypass is active;
                # nothing to re-place, but this is intentional (not a failure).
                replaced = True  # Don't trigger the failure path

            if replaced:
                logger.info(
                    "Re-placed stages %s of pipeline '%s' (dead worker: '%s').",
                    dead_stages,
                    ps.pipeline_id,
                    dead_node_id,
                )
                await self._dispatch_ready_stages(ps)
            else:
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

    # ------------------------------------------------------------------
    # Dispatch with recovery (overrides BaseBroker._dispatch_stage)
    # ------------------------------------------------------------------

    async def _dispatch_stage(self, ps: PipelineState, stage_id: str) -> None:
        """Dispatch a stage with dispatch-time recovery.

        Same recovery mechanism as NeuralBroker._dispatch_stage: if the
        dispatch fails, evict the dead worker, re-place the stage on a
        surviving worker using round-robin/random, and re-dispatch.
        """
        node_id = ps.placement[stage_id]
        worker = self._workers.get(node_id)
        if worker is None:
            async with self._pipelines_lock:
                ps.failed = True
                ps.error = f"Worker '{node_id}' not registered."
                self._active_pipelines.pop(ps.pipeline_id, None)
            await self._metrics.complete_pipeline(
                ps.pipeline_id, success=False, error=ps.error
            )
            return

        stage = ps.dag.get_stage(stage_id)

        await self._metrics.record(
            TimestampRecord(
                pipeline_id=ps.pipeline_id,
                stage_id=stage_id,
                event="dispatched",
                timestamp=time.time(),
                node_id=node_id,
                metadata={"pipeline_type": ps.pipeline_type},
            )
        )

        payload = {
            "pipeline_id": ps.pipeline_id,
            "stage_id": stage_id,
            "stage_type": stage.stage_type,
            "computational_demand": stage.computational_demand,
            "input_data": "",
            "metadata": {"broker_id": self.broker_id, "pipeline_type": ps.pipeline_type},
        }

        if self.transport == "kafka":
            payload["target_worker"] = node_id
            payload["target_url"] = worker.url
            topic = ps.pipeline_type
            try:
                await self._producer.send_and_wait(topic, value=payload)
                return
            except Exception as exc:
                logger.warning(
                    "Kafka dispatch of stage '%s' to '%s' failed: %s. "
                    "Attempting re-placement.",
                    stage_id, node_id, exc,
                )
        else:
            url = f"{worker.url.rstrip('/')}/execute"
            try:
                resp = await self._http_client.post(url, json=payload, timeout=30.0)
                resp.raise_for_status()
                return
            except Exception as exc:
                logger.warning(
                    "Dispatch of stage '%s' to worker '%s' failed: %s. "
                    "Attempting re-placement.",
                    stage_id, node_id, exc,
                )

        # --- Dispatch failed: evict and re-place (same as NeuralBroker) ---
        async with self._workers_lock:
            if node_id in self._workers:
                del self._workers[node_id]
                self._on_worker_change()
                self._worker_failures.pop(node_id, None)

        # Re-place using static placement (round-robin/random)
        re_placed = False
        stage = ps.dag.get_stage(stage_id)
        async with self._workers_lock:
            if self._workers:
                try:
                    new_node = self._pick_worker(stage.slice_requirement)
                    ps.placement[stage_id] = new_node
                    re_placed = True
                except RuntimeError:
                    pass

        if re_placed:
            new_node_id = ps.placement[stage_id]
            new_worker = self._workers.get(new_node_id)
            if new_worker is not None:
                logger.info(
                    "Re-placed stage '%s' to worker '%s'.", stage_id, new_node_id
                )
                try:
                    if self.transport == "kafka":
                        payload["target_worker"] = new_node_id
                        payload["target_url"] = new_worker.url
                        await self._producer.send_and_wait(
                            ps.pipeline_type, value=payload
                        )
                    else:
                        url = f"{new_worker.url.rstrip('/')}/execute"
                        resp = await self._http_client.post(
                            url, json=payload, timeout=30.0
                        )
                        resp.raise_for_status()
                    return
                except Exception as exc2:
                    logger.error(
                        "Re-dispatch of stage '%s' to '%s' also failed: %s",
                        stage_id, new_node_id, exc2,
                    )

        # All recovery attempts exhausted
        async with self._pipelines_lock:
            ps.failed = True
            ps.error = (
                f"Dispatch of stage '{stage_id}' to '{node_id}' failed "
                f"and re-placement was unsuccessful."
            )
            self._active_pipelines.pop(ps.pipeline_id, None)

    # ------------------------------------------------------------------
    # Federation helpers
    # ------------------------------------------------------------------

    def _build_capacity_summary(self) -> SubscriptionSummary:
        """Build a capacity-based SubscriptionSummary (same as NeuralBroker)."""
        import numpy as np

        slices: dict[str, float] = {}
        for w in self._workers.values():
            spare = max(0.0, w.capacity - w.current_load)
            slices[w.slice_id] = slices.get(w.slice_id, 0.0) + spare

        dim = 384
        clusters = [
            ClusterSummary(
                cluster_id=f"slice-{slice_id}",
                centroid_embedding=np.zeros(dim, dtype=np.float32),
                radius=1.0,
                available_capacity=capacity,
            )
            for slice_id, capacity in slices.items()
        ]

        return SubscriptionSummary(
            domain_id=self.domain_id,
            clusters=clusters,
            timestamp=time.time(),
        )

    async def _try_federation_forward(self, req) -> dict | None:
        """Attempt to forward a pipeline request to a federation peer.

        Same mechanism as NeuralBroker._try_federation_forward.
        """
        from src.broker.models import PublishResponse

        for peer_url in self._propagator.peers:
            try:
                fwd_config = dict(req.config)
                fwd_config["__forwarded_from"] = self.domain_id
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
                    logger.info(
                        "Pipeline forwarded to peer '%s': pipeline_id=%s",
                        peer_url,
                        result.get("pipeline_id"),
                    )
                    return result
                else:
                    logger.debug(
                        "Peer '%s' rejected forwarded pipeline: HTTP %d",
                        peer_url,
                        resp.status_code,
                    )
            except Exception as exc:
                logger.warning(
                    "Federation forward to '%s' failed: %r",
                    peer_url,
                    exc,
                )
        return None

    # ------------------------------------------------------------------
    # FastAPI app builder (extends BaseBroker with federation endpoints)
    # ------------------------------------------------------------------

    def build_app(self) -> FastAPI:
        """Build the FastAPI app with federation and health check lifecycle hooks."""
        app = super().build_app()
        broker = self

        # --- Override startup to add health checks and federation ---
        @app.on_event("startup")
        async def _fairness_startup() -> None:
            await broker._propagator.start()
            broker._health_check_task = asyncio.create_task(
                broker._health_check_loop()
            )
            logger.info(
                "StaticBroker '%s' fairness infrastructure started "
                "(health checks, federation).",
                broker.broker_id,
            )

        @app.on_event("shutdown")
        async def _fairness_shutdown() -> None:
            if broker._health_check_task is not None:
                broker._health_check_task.cancel()
                try:
                    await broker._health_check_task
                except asyncio.CancelledError:
                    pass
                broker._health_check_task = None
            await broker._propagator.stop()

        # --- Federation endpoints (same as NeuralBroker) ---

        @app.post("/federation/summary")
        async def federation_summary_receive(request: Request) -> dict:
            """Accept a msgpack-encoded SubscriptionSummary from a peer broker."""
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

            await broker._propagator.receive_summary(summary)
            domain = summary.domain_id
            broker._peer_summaries[domain] = [summary]

            logger.debug(
                "Received federation summary from domain '%s' (%d bytes).",
                summary.domain_id,
                len(body),
            )
            return {
                "status": "received",
                "from_domain": summary.domain_id,
                "clusters": len(summary.clusters),
            }

        @app.get("/federation/summary")
        async def federation_summary_serve() -> Response:
            """Return this broker's local SubscriptionSummary as msgpack bytes."""
            local = broker._propagator.local_summary
            if local is None:
                return Response(status_code=204)
            data = serialize(local)
            return Response(
                content=data,
                media_type="application/x-msgpack",
            )

        return app


# ---------------------------------------------------------------------------
# Module-level app (for uvicorn / Docker CMD)
# ---------------------------------------------------------------------------

_domain = os.environ.get("DOMAIN", "d1")
_broker_id = os.environ.get("BROKER_ID", f"static-{_domain}")
_placement = os.environ.get("PLACEMENT", "round_robin")
_transport = os.environ.get("TRANSPORT", "http")
_kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
_peer_urls_raw = os.environ.get("PEERS", "")
_peer_urls = [u.strip() for u in _peer_urls_raw.split(",") if u.strip()]

_broker = StaticBroker(
    domain_id=_domain, broker_id=_broker_id, placement=_placement,
    transport=_transport,
    kafka_bootstrap=_kafka_bootstrap if _transport == "kafka" else None,
    peer_urls=_peer_urls,
)
app = _broker.build_app()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args():
    parser = argparse.ArgumentParser(description="Static baseline broker.")
    parser.add_argument("--domain", default="d1", help="Domain ID.")
    parser.add_argument("--port", type=int, default=8080, help="Listen port.")
    parser.add_argument("--broker-id", default=None, help="Broker ID.")
    parser.add_argument(
        "--placement", default=os.environ.get("PLACEMENT", "round_robin"),
        choices=[s.value for s in PlacementStrategy],
        help="Placement strategy (default: round_robin, or PLACEMENT env var).",
    )
    parser.add_argument(
        "--peers", nargs="*", default=[],
        metavar="URL",
        help="Base URLs of federation peer brokers.",
    )
    parser.add_argument(
        "--health-check-interval", type=float, default=5.0,
        dest="health_check_interval_s",
        help="Health check interval in seconds.",
    )
    parser.add_argument(
        "--health-check-max-failures", type=int, default=3,
        dest="health_check_max_failures",
        help="Max consecutive health check failures before worker eviction.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    broker_id = args.broker_id or f"static-{args.domain}"
    broker = StaticBroker(
        domain_id=args.domain,
        broker_id=broker_id,
        placement=args.placement,
        peer_urls=args.peers if args.peers else [],
        health_check_interval_s=args.health_check_interval_s,
        health_check_max_failures=args.health_check_max_failures,
    )
    local_app = broker.build_app()
    uvicorn.run(local_app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
