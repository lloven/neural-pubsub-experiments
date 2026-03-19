"""Static Broker: round-robin and random baseline placement.

A minimal FastAPI broker that uses the same API surface as NeuralBroker
(/register, /publish, /result, /health, /metrics/export) but replaces the
neural placement engine with simple round-robin or random assignment.

No federation, no health monitoring, no governance constraints.

Usage:
    python -m src.broker.static_broker --domain d1 --port 8080
    PLACEMENT=random python -m src.broker.static_broker --domain d1 --port 8080
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

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
# Pipeline factory (same as neural_broker)
# ---------------------------------------------------------------------------

_PIPELINE_FACTORIES = {
    "cqi_prediction": lambda cfg: cqi_prediction_pipeline(),
    "anomaly_detection": lambda cfg: anomaly_detection_pipeline(),
    "sensor_fusion": lambda cfg: sensor_fusion_pipeline(
        n_sensors=int(cfg.get("n_sensors", 3))
    ),
    "map": lambda cfg: map_pipeline(
        stage_type=cfg.get("stage_type", "transform"),
        n_stages=int(cfg.get("n_stages", 3)),
    ),
    "funnel": lambda cfg: funnel_pipeline(
        n_inputs=int(cfg.get("n_inputs", 3)),
    ),
}


def _build_dag(pipeline_type: str, config: dict) -> PipelineDAG:
    factory = _PIPELINE_FACTORIES.get(pipeline_type)
    if factory is None:
        raise ValueError(
            f"Unknown pipeline_type '{pipeline_type}'. "
            f"Registered: {sorted(_PIPELINE_FACTORIES.keys())}."
        )
    return factory(config)


# ---------------------------------------------------------------------------
# Pydantic models (compatible with neural_broker API)
# ---------------------------------------------------------------------------


class PublishRequest(BaseModel):
    pipeline_type: str
    config: dict = {}


class PublishResponse(BaseModel):
    pipeline_id: str
    placement: dict[str, str]
    status: str


class RegisterRequest(BaseModel):
    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    url: str = ""


class RegisterResponse(BaseModel):
    status: str
    node_id: str


class StageResultRequest(BaseModel):
    pipeline_id: str
    stage_id: str
    node_id: str
    start_time: float
    end_time: float
    processing_time_ms: float
    output_data: str = ""
    success: bool = True
    error: Optional[str] = None


class HealthResponse(BaseModel):
    broker_id: str
    domain_id: str
    workers: int
    active_pipelines: int
    status: str


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------


@dataclass
class WorkerInfo:
    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    url: str


@dataclass
class PipelineState:
    pipeline_id: str
    pipeline_type: str
    dag: PipelineDAG
    placement: dict[str, str]
    completed_stages: set[str] = field(default_factory=set)
    failed: bool = False
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# StaticBroker
# ---------------------------------------------------------------------------


class StaticBroker:
    """Baseline broker with round-robin or random stage placement."""

    def __init__(self, domain_id: str, broker_id: str, placement: str = "round_robin") -> None:
        self.domain_id = domain_id
        self.broker_id = broker_id
        self.placement = placement  # "round_robin" or "random"

        self._workers: dict[str, WorkerInfo] = {}
        self._workers_lock = asyncio.Lock()
        self._worker_cycle: itertools.cycle | None = None

        self._active_pipelines: dict[str, PipelineState] = {}
        self._pipelines_lock = asyncio.Lock()

        self._metrics = MetricsCollector()
        self._http_client: httpx.AsyncClient | None = None

    def _rebuild_cycle(self) -> None:
        """Rebuild the round-robin iterator from current workers."""
        ids = sorted(self._workers.keys())
        self._worker_cycle = itertools.cycle(ids) if ids else None

    def _pick_worker(self) -> str:
        """Pick the next worker using the configured strategy."""
        if not self._workers:
            raise RuntimeError("No workers registered.")
        if self.placement == "random":
            return random.choice(list(self._workers.keys()))
        # round_robin
        if self._worker_cycle is None:
            self._rebuild_cycle()
        return next(self._worker_cycle)  # type: ignore[arg-type]

    def _compute_placement(self, dag: PipelineDAG) -> dict[str, str]:
        """Assign each stage in topological order to a worker."""
        order = dag.topological_sort()
        placement: dict[str, str] = {}
        for stage_id in order:
            placement[stage_id] = self._pick_worker()
        return placement

    # ------------------------------------------------------------------
    # Stage dispatch (simplified, no re-placement)
    # ------------------------------------------------------------------

    async def _dispatch_stage(self, ps: PipelineState, stage_id: str) -> None:
        node_id = ps.placement[stage_id]
        worker = self._workers.get(node_id)
        if worker is None:
            async with self._pipelines_lock:
                ps.failed = True
                ps.error = f"Worker '{node_id}' not registered."
                self._active_pipelines.pop(ps.pipeline_id, None)
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

    async def _dispatch_ready_stages(self, ps: PipelineState) -> None:
        if ps.failed:
            return
        dag = ps.dag
        ready = []
        for stage_id in dag.stages:
            if stage_id in ps.completed_stages:
                continue
            preds = dag.predecessors(stage_id)
            if all(p in ps.completed_stages for p in preds):
                ready.append(stage_id)
        if not ready:
            return
        tasks = [self._dispatch_stage(ps, sid) for sid in ready]
        await asyncio.gather(*tasks)

    # ------------------------------------------------------------------
    # FastAPI app
    # ------------------------------------------------------------------

    def build_app(self) -> FastAPI:
        app = FastAPI(title=f"StaticBroker [{self.broker_id}]")
        broker = self

        @app.on_event("startup")
        async def _startup() -> None:
            broker._http_client = httpx.AsyncClient()

        @app.on_event("shutdown")
        async def _shutdown() -> None:
            if broker._http_client is not None:
                await broker._http_client.aclose()

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
                broker._rebuild_cycle()
            return RegisterResponse(status="registered", node_id=req.node_id)

        @app.delete("/register/{node_id}")
        async def deregister(node_id: str) -> dict:
            async with broker._workers_lock:
                if node_id not in broker._workers:
                    raise HTTPException(status_code=404, detail=f"Worker '{node_id}' not registered.")
                del broker._workers[node_id]
                broker._rebuild_cycle()
            return {"status": "deregistered", "node_id": node_id}

        @app.post("/publish", response_model=PublishResponse)
        async def publish(req: PublishRequest) -> PublishResponse:
            try:
                dag = _build_dag(req.pipeline_type, req.config)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

            async with broker._workers_lock:
                if not broker._workers:
                    raise HTTPException(status_code=503, detail="No workers registered.")
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

            await broker._dispatch_ready_stages(ps)
            return PublishResponse(pipeline_id=pipeline_id, placement=placement, status="dispatched")

        @app.post("/result")
        async def result(req: StageResultRequest) -> dict:
            async with broker._pipelines_lock:
                ps = broker._active_pipelines.get(req.pipeline_id)
                if ps is None:
                    return {"status": "unknown_pipeline", "pipeline_id": req.pipeline_id}
                if req.success:
                    ps.completed_stages.add(req.stage_id)
                else:
                    ps.failed = True
                    ps.error = req.error or f"Stage '{req.stage_id}' failed."

            await broker._metrics.record(
                TimestampRecord(
                    pipeline_id=req.pipeline_id,
                    stage_id=req.stage_id,
                    event="stage_start",
                    timestamp=req.start_time,
                    node_id=req.node_id,
                    metadata={"pipeline_type": ps.pipeline_type},
                )
            )
            await broker._metrics.record(
                TimestampRecord(
                    pipeline_id=req.pipeline_id,
                    stage_id=req.stage_id,
                    event="stage_end",
                    timestamp=req.end_time,
                    node_id=req.node_id,
                    metadata={"pipeline_type": ps.pipeline_type},
                )
            )

            all_stages = set(ps.dag.stages.keys())
            if ps.failed:
                await broker._metrics.complete_pipeline(req.pipeline_id, success=False, error=ps.error)
                async with broker._pipelines_lock:
                    broker._active_pipelines.pop(req.pipeline_id, None)
                return {"status": "pipeline_failed", "pipeline_id": req.pipeline_id}

            if ps.completed_stages == all_stages:
                await broker._metrics.record(
                    TimestampRecord(
                        pipeline_id=req.pipeline_id,
                        stage_id=req.stage_id,
                        event="delivered",
                        timestamp=time.time(),
                        node_id=req.node_id,
                        metadata={"pipeline_type": ps.pipeline_type},
                    )
                )
                await broker._metrics.complete_pipeline(req.pipeline_id, success=True)
                async with broker._pipelines_lock:
                    broker._active_pipelines.pop(req.pipeline_id, None)
                return {"status": "pipeline_complete", "pipeline_id": req.pipeline_id}

            await broker._dispatch_ready_stages(ps)
            return {"status": "stage_recorded", "pipeline_id": req.pipeline_id, "stage_id": req.stage_id}

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
            import os as _os
            _os.makedirs(_os.path.dirname(path) or ".", exist_ok=True)
            await broker._metrics.export_csv(path)
            return {"status": "exported", "path": path}

        return app


# ---------------------------------------------------------------------------
# Module-level app (for uvicorn / Docker CMD)
# ---------------------------------------------------------------------------

_domain = os.environ.get("DOMAIN", "d1")
_broker_id = os.environ.get("BROKER_ID", f"static-{_domain}")
_placement = os.environ.get("PLACEMENT", "round_robin")

_broker = StaticBroker(domain_id=_domain, broker_id=_broker_id, placement=_placement)
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
        "--placement", default="round_robin",
        choices=["round_robin", "random"],
        help="Placement strategy (default: round_robin).",
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
    )
    local_app = broker.build_app()
    uvicorn.run(local_app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
