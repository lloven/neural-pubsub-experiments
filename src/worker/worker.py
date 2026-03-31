"""Worker process that executes pipeline stages assigned by the broker.

Each worker represents an ExecutionUnit from placement.py. It registers
with a broker, receives stage execution requests, simulates compute
(configurable processing time), and reports results back.

Usage:
    python -m src.worker --node-id d1-nearrt-1 --slice nearrt --domain d1 \
        --broker-url http://broker-d1:8080 --capacity 1.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tiered worker capabilities
# ---------------------------------------------------------------------------


class Tier:
    """Capability tier constants."""

    PRIMARY = "primary"
    SECONDARY = "secondary"
    IMPOSSIBLE = "impossible"

    _VALID = {PRIMARY, SECONDARY, IMPOSSIBLE}

    @classmethod
    def validate(cls, tier: str) -> str:
        if tier not in cls._VALID:
            raise ValueError(
                f"Invalid tier '{tier}'. Must be one of: {cls._VALID}"
            )
        return tier


@dataclass
class Capability:
    """A worker's capability for one stage type.

    Attributes:
        tier: PRIMARY (fast), SECONDARY (slow), or IMPOSSIBLE (rejected).
        compute_ms: Processing time in milliseconds. None for IMPOSSIBLE.
    """

    tier: str
    compute_ms: Optional[float] = None

    @staticmethod
    def resolve_compute_ms(
        capabilities: dict[str, "Capability"], stage_type: str
    ) -> Optional[float]:
        """Return compute time for a stage type, or None for legacy fallback.

        Returns None if:
        - capabilities is empty (no tiered config → use legacy processing_speed)
        - stage_type not in capabilities (unknown → legacy fallback)
        - tier is IMPOSSIBLE (worker should reject this stage)
        """
        if not capabilities:
            return None
        cap = capabilities.get(stage_type)
        if cap is None:
            return None  # Unknown stage → legacy fallback
        if cap.tier == Tier.IMPOSSIBLE:
            return None
        return cap.compute_ms

    @staticmethod
    def can_execute(
        capabilities: dict[str, "Capability"], stage_type: str
    ) -> bool:
        """Return True if the worker can execute this stage type.

        Returns True if:
        - capabilities is empty (no tiered config → accept everything)
        - stage_type not in capabilities (unknown → accept, backward compat)
        - tier is PRIMARY or SECONDARY
        Returns False only if tier is IMPOSSIBLE.
        """
        if not capabilities:
            return True
        cap = capabilities.get(stage_type)
        if cap is None:
            return True  # Unknown stage → accept (backward compat)
        return cap.tier != Tier.IMPOSSIBLE


def parse_capabilities(raw: Optional[str]) -> dict[str, Capability]:
    """Parse WORKER_CAPABILITIES JSON into a dict of Capability objects.

    Args:
        raw: JSON string or None/empty. Format:
            {"stage_type": {"tier": "primary|secondary|impossible", "compute_ms": 50}}

    Returns:
        Dict mapping stage_type to Capability. Empty dict if raw is None/empty.

    Raises:
        ValueError: If a tier value is not recognized.
    """
    if not raw:
        return {}
    import json as _json

    data = _json.loads(raw)
    result = {}
    for stage_type, spec in data.items():
        tier = Tier.validate(spec["tier"])
        compute_ms = spec.get("compute_ms")
        result[stage_type] = Capability(tier=tier, compute_ms=compute_ms)
    return result


# ---------------------------------------------------------------------------
# Configuration and data classes
# ---------------------------------------------------------------------------


@dataclass
class WorkerConfig:
    """Configuration for a worker node.

    Attributes:
        node_id: Unique identifier for this worker (matches ExecutionUnit.node_id).
        domain_id: Data-sovereignty domain this worker belongs to.
        slice_id: Network slice this worker is part of.
        capacity: Maximum processing capacity (normalised, as in Eq. 1).
        broker_url: Base URL of the broker to register with and report to.
        processing_speed: Multiplier applied to a stage's computational_demand
            to derive the simulated sleep time in seconds. A value of 1.0 means
            a stage with demand 0.5 sleeps for 0.5 s; 0.5 means it sleeps 0.25 s.
        port: Port on which the worker's HTTP server listens.
    """

    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    broker_url: str
    processing_speed: float = 1.0
    port: int = 8081
    bid_cost_ms: float = 0.0  # Market-mode: advertised processing cost per stage (ms)
    callback_url: str = ""  # Explicit callback URL for host networking; empty = auto from port


@dataclass
class StageAssignment:
    """A stage execution request sent by the broker to this worker.

    Attributes:
        pipeline_id: Identifier of the pipeline this stage belongs to.
        stage_id: Identifier of the stage to execute.
        stage_type: Semantic type of the stage (e.g. "predict", "aggregate").
        computational_demand: rho_v from the pipeline DAG (Eq. 1). Used to
            derive simulated processing time: sleep = demand * processing_speed.
        input_data: Raw bytes payload from the upstream stage (may be empty).
        metadata: Arbitrary key-value pairs forwarded from the broker.
    """

    pipeline_id: str
    stage_id: str
    stage_type: str
    computational_demand: float
    input_data: bytes = field(default_factory=bytes)
    metadata: dict = field(default_factory=dict)


@dataclass
class StageResult:
    """Result produced after executing a stage assignment.

    Attributes:
        pipeline_id: Pipeline this result belongs to.
        stage_id: Stage that was executed.
        node_id: Worker node that executed the stage.
        start_time: Unix timestamp (seconds) when execution began.
        end_time: Unix timestamp (seconds) when execution completed.
        processing_time_ms: Wall-clock processing duration in milliseconds.
        output_data: Bytes payload to forward to downstream stages (may be empty).
        success: True if the stage completed without error.
        error: Human-readable error message if success is False, else None.
    """

    pipeline_id: str
    stage_id: str
    node_id: str
    start_time: float
    end_time: float
    processing_time_ms: float
    output_data: bytes = field(default_factory=bytes)
    success: bool = True
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Pydantic models for the HTTP API
# ---------------------------------------------------------------------------


class StageAssignmentModel(BaseModel):
    pipeline_id: str
    stage_id: str
    stage_type: str
    computational_demand: float
    input_data: bytes = b""
    metadata: dict = {}


class StageResultModel(BaseModel):
    pipeline_id: str
    stage_id: str
    node_id: str
    start_time: float
    end_time: float
    processing_time_ms: float
    output_data: bytes = b""
    success: bool = True
    error: Optional[str] = None


class HealthModel(BaseModel):
    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    current_load: float
    registered: bool


# ---------------------------------------------------------------------------
# Worker class
# ---------------------------------------------------------------------------


class Worker:
    """HTTP worker that registers with a broker and executes pipeline stages.

    Lifecycle:
        1. ``await worker.register()`` — announce this node to the broker.
        2. ``await worker.run()`` — start the FastAPI server (blocks until shutdown).
        3. ``await worker.shutdown()`` — deregister and stop the server.

    The ``run()`` method starts a uvicorn server that exposes:
        * ``POST /execute`` — receive and execute a StageAssignment.
        * ``GET  /health``  — return current load and registration status.
        * ``POST /shutdown`` — trigger graceful shutdown.
    """

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self._current_load: float = 0.0
        self._load_lock = asyncio.Lock()
        self._registered: bool = False
        self._shutdown_event = asyncio.Event()
        self._http_client: Optional[httpx.AsyncClient] = None
        self._app = self._build_app()

    @property
    def local_url(self) -> str:
        """Return the worker's local URL (localhost + configured port)."""
        return f"http://localhost:{self.config.port}"

    def registration_payload(self) -> dict:
        """Build the registration payload for the broker.

        Includes callback URL (explicit or auto-generated from port)
        and bid_cost_ms for market-mode allocation.
        """
        url = self.config.callback_url or self.local_url
        return {
            "node_id": self.config.node_id,
            "domain_id": self.config.domain_id,
            "slice_id": self.config.slice_id,
            "capacity": self.config.capacity,
            "url": url,
            "bid_cost_ms": self.config.bid_cost_ms,
        }

    # ------------------------------------------------------------------
    # FastAPI application
    # ------------------------------------------------------------------

    def _build_app(self) -> FastAPI:
        app = FastAPI(title=f"Worker {self.config.node_id}")

        @app.post("/execute", response_model=StageResultModel)
        async def execute(assignment: StageAssignmentModel) -> StageResultModel:
            sa = StageAssignment(
                pipeline_id=assignment.pipeline_id,
                stage_id=assignment.stage_id,
                stage_type=assignment.stage_type,
                computational_demand=assignment.computational_demand,
                input_data=assignment.input_data,
                metadata=assignment.metadata,
            )
            result = await self.execute_stage(sa)
            return StageResultModel(
                pipeline_id=result.pipeline_id,
                stage_id=result.stage_id,
                node_id=result.node_id,
                start_time=result.start_time,
                end_time=result.end_time,
                processing_time_ms=result.processing_time_ms,
                output_data=result.output_data,
                success=result.success,
                error=result.error,
            )

        @app.get("/health", response_model=HealthModel)
        async def health() -> HealthModel:
            return HealthModel(
                node_id=self.config.node_id,
                domain_id=self.config.domain_id,
                slice_id=self.config.slice_id,
                capacity=self.config.capacity,
                current_load=self._current_load,
                registered=self._registered,
            )

        @app.post("/shutdown")
        async def shutdown_endpoint() -> dict:
            logger.info("Shutdown requested via HTTP endpoint.")
            self._shutdown_event.set()
            return {"status": "shutting_down", "node_id": self.config.node_id}

        return app

    # ------------------------------------------------------------------
    # Broker communication
    # ------------------------------------------------------------------

    async def register(self) -> None:
        """Register this worker with the broker.

        Sends node_id, domain, slice, and capacity to ``{broker_url}/register``.
        Sets ``self._registered = True`` on success.

        Raises:
            httpx.HTTPStatusError: If the broker returns a non-2xx status.
            httpx.RequestError: If the broker is unreachable.
        """
        payload = self.registration_payload()
        if self._http_client is None:
            raise RuntimeError("Worker not started; call run() first.")
        response = await self._http_client.post(
            f"{self.config.broker_url}/register",
            json=payload,
            timeout=10.0,
        )
        response.raise_for_status()
        self._registered = True
        logger.info(
            "Worker '%s' registered with broker at %s.",
            self.config.node_id,
            self.config.broker_url,
        )

    async def shutdown(self) -> None:
        """Deregister this worker from the broker and signal the server to stop.

        Sends a DELETE to ``{broker_url}/register/{node_id}``. Errors are
        logged but not re-raised so that shutdown always completes.
        """
        try:
            if self._http_client is not None:
                response = await self._http_client.delete(
                    f"{self.config.broker_url}/register/{self.config.node_id}",
                    timeout=5.0,
                )
                response.raise_for_status()
            logger.info("Worker '%s' deregistered from broker.", self.config.node_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to deregister worker '%s': %s", self.config.node_id, exc
            )
        finally:
            self._registered = False
            self._shutdown_event.set()
            if self._http_client is not None:
                await self._http_client.aclose()
                self._http_client = None

    # ------------------------------------------------------------------
    # Stage execution
    # ------------------------------------------------------------------

    async def execute_stage(self, assignment: StageAssignment) -> StageResult:
        """Simulate execution of a pipeline stage.

        The simulated processing time is::

            sleep_seconds = assignment.computational_demand * config.processing_speed

        After sleeping, reports the result back to the broker via
        ``{broker_url}/result``.

        Args:
            assignment: The stage to execute.

        Returns:
            A StageResult with timing metadata. ``success`` is True unless an
            unexpected exception occurs during execution or result reporting.
        """
        sleep_s = assignment.computational_demand * self.config.processing_speed

        # Track load (lock protects against concurrent coroutine interleaving)
        async with self._load_lock:
            self._current_load = min(
                self._current_load + assignment.computational_demand,
                self.config.capacity,
            )

        start_time = time.time()
        error: Optional[str] = None
        success = True

        try:
            logger.debug(
                "Executing stage '%s' (pipeline=%s, demand=%.3f, sleep=%.3f s).",
                assignment.stage_id,
                assignment.pipeline_id,
                assignment.computational_demand,
                sleep_s,
            )
            await asyncio.sleep(sleep_s)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            success = False
            logger.error(
                "Stage '%s' execution failed: %s", assignment.stage_id, exc
            )
        finally:
            async with self._load_lock:
                self._current_load = max(
                    self._current_load - assignment.computational_demand, 0.0
                )

        end_time = time.time()
        processing_time_ms = (end_time - start_time) * 1000.0

        result = StageResult(
            pipeline_id=assignment.pipeline_id,
            stage_id=assignment.stage_id,
            node_id=self.config.node_id,
            start_time=start_time,
            end_time=end_time,
            processing_time_ms=processing_time_ms,
            output_data=b"",
            success=success,
            error=error,
        )

        await self._report_result(result)
        return result

    async def _report_result(self, result: StageResult) -> None:
        """POST the stage result back to the broker.

        Sends to ``{broker_url}/result``. On failure, retries once after a 1 s
        delay. If both attempts fail, logs a WARNING with pipeline_id, stage_id,
        and the error (L39 compliance). Does not re-raise so the worker
        continues serving other requests.
        """
        payload = {
            "pipeline_id": result.pipeline_id,
            "stage_id": result.stage_id,
            "node_id": result.node_id,
            "start_time": result.start_time,
            "end_time": result.end_time,
            "processing_time_ms": result.processing_time_ms,
            "success": result.success,
            "error": result.error,
        }
        if self._http_client is None:
            logger.warning(
                "Cannot report result for pipeline '%s' stage '%s': "
                "no HTTP client available.",
                result.pipeline_id,
                result.stage_id,
            )
            return

        last_exc: Optional[Exception] = None
        for attempt in range(2):
            try:
                response = await self._http_client.post(
                    f"{self.config.broker_url}/result",
                    json=payload,
                    timeout=5.0,
                )
                response.raise_for_status()
                return  # success
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt == 0:
                    await asyncio.sleep(1.0)

        # Both attempts failed
        logger.warning(
            "Failed to report result for pipeline '%s' stage '%s' "
            "after 2 attempts: %s",
            result.pipeline_id,
            result.stage_id,
            last_exc,
        )

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start the FastAPI/uvicorn server and block until shutdown is signalled.

        Attempts to register with the broker before starting the server. If
        registration fails the server still starts (to allow the broker to
        reach this worker later).

        The server listens on ``0.0.0.0:{config.port}``.
        """
        self._http_client = httpx.AsyncClient()

        try:
            await self.register()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Initial registration failed (will retry later): %s", exc
            )

        uv_config = uvicorn.Config(
            app=self._app,
            host="0.0.0.0",
            port=self.config.port,
            log_level="info",
        )
        server = uvicorn.Server(uv_config)

        # Run server and shutdown-waiter concurrently; whichever finishes
        # first (the shutdown event) cancels the other.
        server_task = asyncio.create_task(server.serve())
        shutdown_task = asyncio.create_task(self._shutdown_event.wait())

        done, pending = await asyncio.wait(
            {server_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Initiate uvicorn shutdown if server is still running
        if not server_task.done():
            server.should_exit = True
            await server_task

        logger.info("Worker '%s' server stopped.", self.config.node_id)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Neural Pub/Sub worker node.",
    )
    parser.add_argument("--node-id", required=True, help="Unique node identifier.")
    parser.add_argument("--domain", required=True, dest="domain_id", help="Domain ID.")
    parser.add_argument("--slice", required=True, dest="slice_id", help="Slice ID.")
    parser.add_argument(
        "--broker-url",
        required=True,
        help="Broker base URL, e.g. http://broker:8080.",
    )
    parser.add_argument(
        "--capacity",
        type=float,
        default=1.0,
        help="Node capacity (normalised, default 1.0).",
    )
    parser.add_argument(
        "--processing-speed",
        type=float,
        default=1.0,
        dest="processing_speed",
        help="Multiplier on computational_demand to get sleep time (default 1.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8081,
        help="Port for the worker HTTP server (default 8081).",
    )
    parser.add_argument(
        "--callback-url",
        default="",
        dest="callback_url",
        help="Explicit callback URL for host networking (default: auto from port).",
    )
    parser.add_argument(
        "--bid-cost",
        type=float,
        default=0.0,
        dest="bid_cost_ms",
        help="Market-mode advertised processing cost per stage in ms (default 0.0).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point for the worker process."""
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    config = WorkerConfig(
        node_id=args.node_id,
        domain_id=args.domain_id,
        slice_id=args.slice_id,
        capacity=args.capacity,
        broker_url=args.broker_url,
        processing_speed=args.processing_speed,
        port=args.port,
        callback_url=args.callback_url,
        bid_cost_ms=args.bid_cost_ms,
    )
    worker = Worker(config)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
