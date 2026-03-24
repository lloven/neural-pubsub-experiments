"""BaseBroker: shared HTTP API skeleton for baseline brokers.

Provides the common FastAPI application structure (endpoints for /register,
/deregister, /publish, /result, /health, /metrics/export) and DAG walking
logic. Subclasses override ``_compute_placement`` and ``_dispatch_stage``
to implement their specific distribution strategy.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import os
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request

from src.broker.funnel_resilience import (
    FunnelMode,
    FunnelPolicyResult,
    apply_funnel_policy,
    get_funnel_mode,
    get_funnel_timeout,
)
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
from src.measurement.harness import MetricsCollector, TimestampRecord
from src.pipeline.dag import PipelineDAG
from src.pipeline.patterns import (
    anomaly_detection_pipeline,
    cqi_prediction_pipeline,
    funnel_pipeline,
    map_pipeline,
    sensor_fusion_pipeline,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline factory (single definition shared by all brokers)
# ---------------------------------------------------------------------------

# Maps pipeline_type string to a factory function (or callable).
# The factory receives a dict of config overrides and returns a PipelineDAG.
# Uses the neural_broker's complete version with all parameters.
_PIPELINE_FACTORIES: dict[str, Any] = {
    "cqi_prediction": lambda cfg: cqi_prediction_pipeline(),
    "anomaly_detection": lambda cfg: anomaly_detection_pipeline(),
    "sensor_fusion": lambda cfg: sensor_fusion_pipeline(
        n_sensors=int(cfg.get("n_sensors", 3))
    ),
    "map": lambda cfg: map_pipeline(
        stage_type=cfg.get("stage_type", "transform"),
        n_stages=int(cfg.get("n_stages", 3)),
        computational_demand=float(cfg.get("computational_demand", 0.5)),
        output_data_rate=float(cfg.get("output_data_rate", 5.0)),
        latency_bound=float(cfg.get("latency_bound", 10.0)),
        slice_requirement=cfg.get("slice_requirement"),
    ),
    "funnel": lambda cfg: funnel_pipeline(
        n_inputs=int(cfg.get("n_inputs", 3)),
        input_type=cfg.get("input_type", "ingest"),
        latency_bound_in=float(cfg.get("latency_bound_in", 10.0)),
        latency_bound_out=float(cfg.get("latency_bound_out", 5.0)),
    ),
}


def _build_dag(pipeline_type: str, config: dict) -> PipelineDAG:
    """Instantiate a PipelineDAG from a registered factory.

    Args:
        pipeline_type: Key into the factory registry.
        config: Override dict forwarded to the factory.

    Returns:
        A fully constructed PipelineDAG.

    Raises:
        ValueError: If the pipeline_type is not registered.
    """
    factory = _PIPELINE_FACTORIES.get(pipeline_type)
    if factory is None:
        raise ValueError(
            f"Unknown pipeline_type '{pipeline_type}'. "
            f"Registered types: {sorted(_PIPELINE_FACTORIES.keys())}."
        )
    return factory(config)


# ---------------------------------------------------------------------------
# BaseBroker
# ---------------------------------------------------------------------------


class BaseBroker(abc.ABC):
    """Abstract base for baseline brokers (StaticBroker, NeuralBroker).

    Provides the shared FastAPI application skeleton, worker registry,
    pipeline tracking, metrics collection, DAG-walking dispatch logic,
    and dual-transport stage dispatch (HTTP or Kafka).

    Subclasses must implement ``_compute_placement``. The ``_dispatch_stage``
    method is concrete and switches between HTTP and Kafka based on the
    ``transport`` parameter.
    """

    def __init__(
        self,
        domain_id: str,
        broker_id: str,
        *,
        transport: str = "http",
        kafka_bootstrap: str | None = None,
    ) -> None:
        self.domain_id = domain_id
        self.broker_id = broker_id
        self.transport = transport
        self.kafka_bootstrap = kafka_bootstrap

        self._workers: dict[str, WorkerInfo] = {}
        self._workers_lock = asyncio.Lock()

        self._active_pipelines: dict[str, PipelineState] = {}
        self._pipelines_lock = asyncio.Lock()

        self._metrics = MetricsCollector()
        self._http_client: httpx.AsyncClient | None = None
        self._producer = None  # AIOKafkaProducer (created at startup if transport=kafka)

    # ------------------------------------------------------------------
    # Abstract methods
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def _compute_placement(self, dag: PipelineDAG) -> dict[str, str]:
        """Assign each DAG stage to a worker/target.

        Args:
            dag: The pipeline DAG to place.

        Returns:
            Mapping from stage_id to target identifier.
        """

    # ------------------------------------------------------------------
    # Dual-transport dispatch (concrete)
    # ------------------------------------------------------------------

    async def _dispatch_stage(self, ps: PipelineState, stage_id: str) -> None:
        """Dispatch a single stage to its assigned target.

        Uses HTTP or Kafka transport depending on ``self.transport``.
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
            # Kafka transport: publish with placement embedded
            payload["target_worker"] = node_id
            payload["target_url"] = worker.url
            topic = ps.pipeline_type
            try:
                await self._producer.send_and_wait(topic, value=payload)
            except Exception as exc:
                logger.error("Kafka publish failed for stage '%s': %s", stage_id, exc)
                async with self._pipelines_lock:
                    ps.failed = True
                    ps.error = f"Kafka publish failed for stage '{stage_id}': {exc}"
                    self._active_pipelines.pop(ps.pipeline_id, None)
                await self._metrics.complete_pipeline(
                    ps.pipeline_id, success=False, error=ps.error
                )
        else:
            # HTTP transport: direct POST to worker
            url = f"{worker.url.rstrip('/')}/execute"
            try:
                resp = await self._http_client.post(url, json=payload, timeout=30.0)
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("Dispatch of stage '%s' to '%s' failed: %s", stage_id, node_id, exc)
                async with self._pipelines_lock:
                    ps.failed = True
                    ps.error = f"Dispatch failed for stage '{stage_id}': {exc}"
                    self._active_pipelines.pop(ps.pipeline_id, None)
                await self._metrics.complete_pipeline(
                    ps.pipeline_id, success=False, error=ps.error
                )

    # ------------------------------------------------------------------
    # DAG walking (shared)
    # ------------------------------------------------------------------

    @staticmethod
    def _find_ready_stages(
        ps: PipelineState,
        *,
        dead_workers: set[str] | None = None,
    ) -> tuple[list[str], FunnelPolicyResult | None]:
        """Determine which stages are ready to dispatch, consulting funnel policy.

        For fan-in stages (multiple predecessors) where some predecessors are
        assigned to dead workers and will never complete, the funnel resilience
        policy (Section 4.4.3) decides whether to wait, proceed with partial
        inputs, or abort.

        Args:
            ps: The pipeline state to inspect.
            dead_workers: Set of worker IDs known to be dead/unreachable.

        Returns:
            A tuple of (ready_stage_ids, funnel_policy_result). The funnel
            result is None when no funnel intervention was needed.
        """
        if dead_workers is None:
            dead_workers = set()

        dag = ps.dag
        mode = get_funnel_mode()
        timeout_s = get_funnel_timeout()
        ready: list[str] = []
        funnel_result: FunnelPolicyResult | None = None

        for stage_id in dag.stages:
            if stage_id in ps.completed_stages:
                continue
            if stage_id in ps.dispatched_stages:
                continue
            if stage_id in ps.skipped_stages:
                continue

            preds = dag.predecessors(stage_id)

            # Check if all predecessors are complete
            if all(p in ps.completed_stages for p in preds):
                ready.append(stage_id)
                continue

            # Check if this is a fan-in stage with dead predecessors
            if len(preds) <= 1:
                # Not a fan-in stage; skip funnel logic
                continue

            # Identify predecessors that are on dead workers and incomplete
            dead_preds = {
                p for p in preds
                if p not in ps.completed_stages
                and ps.placement.get(p) in dead_workers
            }

            if not dead_preds:
                # Missing predecessors are on live workers; just wait normally
                continue

            # Fan-in stage with dead predecessors: consult funnel policy
            # Track when we started waiting for this funnel stage
            if ps.funnel_wait_start is None:
                ps.funnel_wait_start = time.time()

            timeout_reached = (time.time() - ps.funnel_wait_start) >= timeout_s

            result = apply_funnel_policy(
                mode=mode,
                expected_inputs=set(preds),
                received_inputs=ps.completed_stages & set(preds),
                timeout_reached=timeout_reached,
            )
            funnel_result = result

            if result.action == "proceed":
                # Mark dead predecessors as skipped and proceed
                ps.skipped_stages |= dead_preds
                ps.partial = True
                ready.append(stage_id)

            elif result.action == "abort":
                # Do not add to ready; caller should fail the pipeline
                pass

            elif result.action == "wait":
                # Do not add to ready; keep waiting
                pass

            elif result.action == "fail":
                # Timeout reached in wait mode
                pass

        return ready, funnel_result

    async def _dispatch_ready_stages(self, ps: PipelineState) -> None:
        """Dispatch all stages whose predecessors have completed.

        Acquires _pipelines_lock to read completed_stages safely.
        Consults the funnel resilience policy for fan-in stages with
        dead predecessors.
        """
        if ps.failed:
            return

        # Determine dead workers (workers in placement but not in registry)
        dead_workers: set[str] = set()
        async with self._workers_lock:
            live_workers = set(self._workers.keys())
        placement_workers = set(ps.placement.values())
        dead_workers = placement_workers - live_workers

        async with self._pipelines_lock:
            ready, funnel_result = self._find_ready_stages(
                ps, dead_workers=dead_workers,
            )

        # Handle funnel policy results
        if funnel_result is not None and funnel_result.pipeline_failed:
            error_msg = (
                f"funnel_{funnel_result.action}: pipeline failed due to "
                f"funnel resilience policy ({funnel_result.action} mode)"
            )
            async with self._pipelines_lock:
                ps.failed = True
                ps.error = error_msg
                self._active_pipelines.pop(ps.pipeline_id, None)
            await self._metrics.complete_pipeline(
                ps.pipeline_id, success=False, error=error_msg,
            )
            return

        if not ready:
            return

        async with self._pipelines_lock:
            ps.dispatched_stages.update(ready)

        tasks = [self._dispatch_stage(ps, sid) for sid in ready]
        await asyncio.gather(*tasks)

    # ------------------------------------------------------------------
    # Result handling (shared)
    # ------------------------------------------------------------------

    async def _handle_result(self, req: StageResultRequest) -> dict:
        """Process a stage completion report from a worker.

        Records timing metrics, updates pipeline state, and dispatches
        the next ready stages or finalises the pipeline.
        """
        # Record the moment the broker receives the stage result
        await self._metrics.record(
            TimestampRecord(
                pipeline_id=req.pipeline_id,
                stage_id=req.stage_id,
                event="stage_result_received",
                timestamp=time.time(),
                node_id=req.node_id,
            )
        )

        async with self._pipelines_lock:
            ps = self._active_pipelines.get(req.pipeline_id)
            if ps is None:
                return {"status": "unknown_pipeline", "pipeline_id": req.pipeline_id}
            if req.success:
                ps.completed_stages.add(req.stage_id)
            else:
                ps.failed = True
                ps.error = req.error or f"Stage '{req.stage_id}' failed."

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

        if ps.failed:
            await self._metrics.complete_pipeline(
                req.pipeline_id, success=False, error=ps.error
            )
            async with self._pipelines_lock:
                self._active_pipelines.pop(req.pipeline_id, None)
            return {"status": "pipeline_failed", "pipeline_id": req.pipeline_id}

        if ps.completed_stages == ps.all_stages:
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
            return {"status": "pipeline_complete", "pipeline_id": req.pipeline_id}

        await self._dispatch_ready_stages(ps)
        return {
            "status": "stage_recorded",
            "pipeline_id": req.pipeline_id,
            "stage_id": req.stage_id,
        }

    # ------------------------------------------------------------------
    # FastAPI app builder
    # ------------------------------------------------------------------

    def build_app(self) -> FastAPI:
        """Build the shared FastAPI application.

        Registers endpoints for /register, /deregister, /publish, /result,
        /health, and /metrics/export. Subclasses may override to add
        lifecycle hooks (e.g. Kafka producer start/stop).
        """
        app = FastAPI(title=f"{type(self).__name__} [{self.broker_id}]")
        broker = self

        @app.on_event("startup")
        async def _startup() -> None:
            broker._http_client = httpx.AsyncClient()
            if broker.transport == "kafka" and broker.kafka_bootstrap:
                import json as _json
                from aiokafka import AIOKafkaProducer
                broker._producer = AIOKafkaProducer(
                    bootstrap_servers=broker.kafka_bootstrap,
                    value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
                )
                await broker._producer.start()
                logger.info("Kafka producer started (bootstrap=%s).", broker.kafka_bootstrap)
            # Periodic partial CSV export for crash resilience
            broker._snapshot_task = asyncio.create_task(broker._periodic_snapshot())

        @app.on_event("shutdown")
        async def _shutdown() -> None:
            if hasattr(broker, '_snapshot_task'):
                broker._snapshot_task.cancel()
            if broker._producer is not None:
                await broker._producer.stop()
                broker._producer = None
            if broker._http_client is not None:
                await broker._http_client.aclose()

        @app.get("/workers")
        async def workers() -> dict:
            """Return registered workers with URLs (for kafka-consumer sidecar)."""
            async with broker._workers_lock:
                return {
                    nid: {"url": w.url, "domain_id": w.domain_id, "slice_id": w.slice_id}
                    for nid, w in broker._workers.items()
                }

        @app.post("/register", response_model=RegisterResponse)
        async def register(req: RegisterRequest, request: Request) -> RegisterResponse:
            worker_url = req.url
            if not worker_url:
                client_host = request.client.host if request.client else "127.0.0.1"
                worker_url = f"http://{client_host}:8081"
            async with broker._workers_lock:
                broker._workers[req.node_id] = WorkerInfo(
                    node_id=req.node_id,
                    domain_id=req.domain_id,
                    slice_id=req.slice_id,
                    capacity=req.capacity,
                    url=worker_url,
                )
                broker._on_worker_change()
            return RegisterResponse(status="registered", node_id=req.node_id)

        @app.delete("/register/{node_id}")
        async def deregister(node_id: str) -> dict:
            async with broker._workers_lock:
                if node_id not in broker._workers:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Worker '{node_id}' not registered.",
                    )
                del broker._workers[node_id]
                broker._on_worker_change()
            return {"status": "deregistered", "node_id": node_id}

        @app.post("/publish", response_model=PublishResponse)
        async def publish(req: PublishRequest) -> PublishResponse:
            try:
                dag = _build_dag(req.pipeline_type, req.config)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

            async with broker._workers_lock:
                if not broker._workers:
                    raise HTTPException(
                        status_code=503, detail="No workers registered."
                    )
                placement = broker._compute_placement(dag)

            pipeline_id = str(uuid.uuid4())
            ps = PipelineState(
                pipeline_id=pipeline_id,
                pipeline_type=req.pipeline_type,
                dag=dag,
                placement=placement,
            )
            async with broker._pipelines_lock:
                broker._active_pipelines[pipeline_id] = ps

            await broker._metrics.record(
                TimestampRecord(
                    pipeline_id=pipeline_id,
                    stage_id="__pipeline__",
                    event="created",
                    timestamp=time.time(),
                    node_id=broker.broker_id,
                    metadata={"pipeline_type": req.pipeline_type},
                )
            )

            await broker._metrics.record(
                TimestampRecord(
                    pipeline_id=pipeline_id,
                    stage_id="__pipeline__",
                    event="placement_complete",
                    timestamp=time.time(),
                    node_id=broker.broker_id,
                    metadata={"pipeline_type": req.pipeline_type},
                )
            )

            await broker._dispatch_ready_stages(ps)
            return PublishResponse(
                pipeline_id=pipeline_id,
                placement=placement,
                status="dispatched",
            )

        @app.post("/result")
        async def result(req: StageResultRequest) -> dict:
            return await broker._handle_result(req)

        @app.get("/health", response_model=HealthResponse)
        async def health() -> HealthResponse:
            async with broker._workers_lock:
                n_workers = len(broker._workers)
            async with broker._pipelines_lock:
                n_active = len(broker._active_pipelines)
            return HealthResponse(
                broker_id=broker.broker_id,
                domain_id=broker.domain_id,
                workers=n_workers,
                active_pipelines=n_active,
                status="ok",
            )

        @app.post("/metrics/export")
        async def metrics_export(request: Request) -> dict:
            body = await request.json()
            path = body.get("path", "results/local/metrics.csv")
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            await broker._metrics.export_csv(path)
            return {"status": "exported", "path": path}

        return app

    # ------------------------------------------------------------------
    # Periodic snapshot for crash resilience
    # ------------------------------------------------------------------

    async def _periodic_snapshot(self, interval_s: int = 60) -> None:
        """Write a partial CSV every *interval_s* seconds for crash resilience."""
        snapshot_path = os.environ.get("RESULT_FILE", "")
        if not snapshot_path:
            return
        partial_path = snapshot_path.replace(".csv", ".partial.csv")
        while True:
            await asyncio.sleep(interval_s)
            try:
                os.makedirs(os.path.dirname(partial_path) or ".", exist_ok=True)
                await self._metrics.export_csv(partial_path)
            except Exception:
                pass  # best-effort; don't crash the broker

    # ------------------------------------------------------------------
    # Hook for subclasses (called inside _workers_lock)
    # ------------------------------------------------------------------

    def _on_worker_change(self) -> None:
        """Called after a worker registers or deregisters.

        Subclasses may override to rebuild iterators or other state.
        The caller holds ``_workers_lock``.
        """
