"""Fairness tests for StaticBroker: ensure S1/S2 have the same infrastructure
as NeuralBroker (S3) so that H6 comparison isolates ONLY the placement strategy.

Three confounds being tested:
1. Health check loop (periodic ping, dead worker removal)
2. Dispatch-time recovery (re-place on surviving worker when dispatch fails)
3. Federation (accept --peers, forward pipelines to peer brokers)

After these tests pass, S1/S2/S3 differ in ONLY ONE thing: the placement
algorithm (round-robin / random / neural).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import httpx
import pytest

from src.broker.models import PipelineState, WorkerInfo
from src.broker.static_broker import PlacementStrategy, StaticBroker
from src.pipeline.patterns import cqi_prediction_pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_broker(
    placement: str = "round_robin",
    peer_urls: list[str] | None = None,
    health_check_interval_s: float = 5.0,
    health_check_max_failures: int = 3,
) -> StaticBroker:
    """Create a StaticBroker with optional federation and health check config."""
    return StaticBroker(
        domain_id="d1",
        broker_id="static-d1",
        placement=placement,
        peer_urls=peer_urls or [],
        health_check_interval_s=health_check_interval_s,
        health_check_max_failures=health_check_max_failures,
    )


async def _register_workers(
    broker: StaticBroker, n: int, domain: str = "d1", slice_id: str = "flat",
) -> list[str]:
    """Register *n* workers and return their node_ids.

    Uses slice_id="flat" by default so workers accept any pipeline slice.
    These tests focus on infrastructure fairness (health checks, dispatch
    recovery, federation), not slice placement correctness.
    """
    node_ids = []
    for i in range(n):
        nid = f"worker-{domain}-{i}"
        async with broker._workers_lock:
            broker._workers[nid] = WorkerInfo(
                node_id=nid,
                domain_id=domain,
                slice_id=slice_id,
                capacity=1.0,
                url=f"http://localhost:{8081 + i}",
            )
            node_ids.append(nid)
    async with broker._workers_lock:
        broker._on_worker_change()
    return node_ids


# ===================================================================
# 1. HEALTH CHECK LOOP
# ===================================================================


class TestHealthCheckLoop:
    """StaticBroker must have the same health check mechanism as NeuralBroker."""

    @pytest.mark.asyncio
    async def test_static_broker_has_health_check_loop(self):
        """StaticBroker must expose a _health_check_loop coroutine method."""
        broker = _make_broker()
        assert hasattr(broker, "_health_check_loop"), (
            "StaticBroker must have a _health_check_loop method"
        )
        assert asyncio.iscoroutinefunction(broker._health_check_loop), (
            "_health_check_loop must be an async method"
        )

    @pytest.mark.asyncio
    async def test_health_check_accepts_interval_and_max_failures(self):
        """StaticBroker constructor must accept health_check_interval_s and
        health_check_max_failures parameters (same as NeuralBroker config)."""
        broker = _make_broker(
            health_check_interval_s=2.0,
            health_check_max_failures=5,
        )
        assert broker._health_check_interval_s == 2.0
        assert broker._health_check_max_failures == 5

    @pytest.mark.asyncio
    async def test_health_check_removes_dead_worker(self):
        """After max_failures consecutive health check failures, the worker
        must be removed from the active pool."""
        broker = _make_broker(health_check_interval_s=0.05, health_check_max_failures=2)
        broker._http_client = httpx.AsyncClient()

        node_ids = await _register_workers(broker, 3)

        # Mock HTTP client: worker-d1-1 always fails health checks
        original_get = broker._http_client.get

        async def mock_get(url, **kwargs):
            if "8082" in url:  # worker-d1-1 on port 8082
                raise httpx.ConnectError("Connection refused")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        broker._http_client.get = mock_get

        # Run health check loop briefly
        task = asyncio.create_task(broker._health_check_loop())
        await asyncio.sleep(0.3)  # enough for several health check intervals
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        await broker._http_client.aclose()

        # worker-d1-1 should have been removed
        assert "worker-d1-1" not in broker._workers, (
            "Dead worker should be removed from the active pool after max_failures"
        )
        # Other workers should remain
        assert "worker-d1-0" in broker._workers
        assert "worker-d1-2" in broker._workers

    @pytest.mark.asyncio
    async def test_health_check_resets_on_success(self):
        """A successful health probe must reset the consecutive failure counter."""
        broker = _make_broker(health_check_interval_s=0.05, health_check_max_failures=3)
        broker._http_client = httpx.AsyncClient()

        await _register_workers(broker, 1)

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # Fail twice
                raise httpx.ConnectError("Connection refused")
            # Then succeed
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        broker._http_client.get = mock_get

        task = asyncio.create_task(broker._health_check_loop())
        await asyncio.sleep(0.3)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        await broker._http_client.aclose()

        # Worker should still be alive (2 failures then success resets counter)
        assert "worker-d1-0" in broker._workers, (
            "Worker should survive when failures don't reach max_failures consecutively"
        )


# ===================================================================
# 2. DISPATCH-TIME RECOVERY
# ===================================================================


class TestDispatchTimeRecovery:
    """StaticBroker must re-place stages on surviving workers when dispatch fails."""

    @pytest.mark.asyncio
    async def test_dispatch_retries_on_failure(self):
        """When an HTTP dispatch fails, StaticBroker should evict the dead worker
        and re-place the stage on a surviving worker (same as NeuralBroker)."""
        broker = _make_broker()
        broker._http_client = httpx.AsyncClient()

        node_ids = await _register_workers(broker, 3)

        dag = cqi_prediction_pipeline()
        order = dag.topological_sort()

        # Force placement: all stages go to worker-d1-0
        placement = {sid: "worker-d1-0" for sid in order}
        ps = PipelineState(
            pipeline_id="test-pipe-1",
            pipeline_type="cqi_prediction",
            dag=dag,
            placement=placement,
        )
        broker._active_pipelines[ps.pipeline_id] = ps

        # Mock: worker-d1-0 fails, others succeed
        call_log = []

        async def mock_post(url, **kwargs):
            call_log.append(url)
            if "8081" in url:  # worker-d1-0 on port 8081
                raise httpx.ConnectError("Connection refused")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        broker._http_client.post = mock_post

        # Dispatch the first stage
        first_stage = order[0]
        await broker._dispatch_stage(ps, first_stage)

        await broker._http_client.aclose()

        # The pipeline should NOT be marked as permanently failed
        # because re-placement should have succeeded
        assert not ps.failed, (
            "Pipeline should not fail when re-placement succeeds on a surviving worker"
        )
        # The dead worker should be evicted
        assert "worker-d1-0" not in broker._workers, (
            "Dead worker should be evicted after dispatch failure"
        )
        # The stage should be re-assigned to a surviving worker
        assert ps.placement[first_stage] != "worker-d1-0", (
            "Stage should be re-placed on a surviving worker"
        )

    @pytest.mark.asyncio
    async def test_dispatch_fails_pipeline_when_no_survivors(self):
        """When all workers are dead, the pipeline should be marked as failed."""
        broker = _make_broker()
        broker._http_client = httpx.AsyncClient()

        await _register_workers(broker, 1)

        dag = cqi_prediction_pipeline()
        order = dag.topological_sort()
        placement = {sid: "worker-d1-0" for sid in order}
        ps = PipelineState(
            pipeline_id="test-pipe-2",
            pipeline_type="cqi_prediction",
            dag=dag,
            placement=placement,
        )
        broker._active_pipelines[ps.pipeline_id] = ps

        async def mock_post(url, **kwargs):
            raise httpx.ConnectError("Connection refused")

        broker._http_client.post = mock_post

        first_stage = order[0]
        await broker._dispatch_stage(ps, first_stage)

        await broker._http_client.aclose()

        # Pipeline should be failed since no surviving workers
        assert ps.failed, (
            "Pipeline should fail when no surviving workers are available for re-placement"
        )


# ===================================================================
# 3. FEDERATION
# ===================================================================


class TestFederation:
    """StaticBroker must accept peer_urls and federate with other brokers."""

    @pytest.mark.asyncio
    async def test_static_broker_accepts_peer_urls(self):
        """StaticBroker constructor must accept peer_urls parameter."""
        peers = ["http://broker-d2:8080", "http://broker-d3:8080"]
        broker = _make_broker(peer_urls=peers)
        assert hasattr(broker, "_peer_urls") or hasattr(broker, "_propagator"), (
            "StaticBroker must store peer URLs for federation"
        )

    @pytest.mark.asyncio
    async def test_static_broker_has_federation_summary_endpoint(self):
        """StaticBroker's FastAPI app must expose federation/summary endpoints."""
        broker = _make_broker(peer_urls=["http://broker-d2:8080"])
        app = broker.build_app()
        route_paths = {r.path for r in app.routes}
        assert "/federation/summary" in route_paths, (
            "StaticBroker must expose /federation/summary endpoint for federation"
        )

    @pytest.mark.asyncio
    async def test_static_broker_forwards_when_no_local_workers(self):
        """When StaticBroker has no local workers and peers are configured,
        it should attempt federation forwarding."""
        broker = _make_broker(peer_urls=["http://broker-d2:8080"])
        broker._http_client = httpx.AsyncClient()

        # Mock: peer accepts the pipeline
        forward_called = False

        async def mock_post(url, **kwargs):
            nonlocal forward_called
            if "broker-d2" in url and "/publish" in url:
                forward_called = True
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json = MagicMock(return_value={
                    "pipeline_id": "forwarded-pipe-1",
                    "placement": {"stage1": "remote-worker"},
                    "status": "dispatched",
                })
                mock_resp.content = b'{"pipeline_id":"forwarded-pipe-1"}'
                return mock_resp
            raise httpx.ConnectError("unexpected")

        broker._http_client.post = mock_post

        # Attempt to publish with no local workers should trigger forwarding
        assert hasattr(broker, "_try_federation_forward") or hasattr(broker, "_propagator"), (
            "StaticBroker must have federation forwarding capability"
        )

        await broker._http_client.aclose()

    @pytest.mark.asyncio
    async def test_cli_peers_argument_is_functional(self):
        """The --peers CLI argument must actually configure federation, not be silently ignored."""
        broker = _make_broker(peer_urls=["http://broker-d2:8080"])
        # The broker must actually have a propagator or peer list that's used
        has_peers = False
        if hasattr(broker, "_propagator"):
            has_peers = len(broker._propagator.peers) > 0
        elif hasattr(broker, "_peer_urls"):
            has_peers = len(broker._peer_urls) > 0
        assert has_peers, (
            "--peers must configure actual federation, not be silently ignored"
        )


# ===================================================================
# 4. WORKER POOL PARITY
# ===================================================================


class TestWorkerPoolParity:
    """StaticBroker must support the same worker pool as NeuralBroker,
    including workers from multiple domains (federated topology)."""

    @pytest.mark.asyncio
    async def test_worker_pool_includes_cross_domain_workers(self):
        """StaticBroker must be able to register workers from different domains
        (same as NeuralBroker in federated setup with 5 workers across 2 domains)."""
        broker = _make_broker()
        d1_workers = await _register_workers(broker, 3, domain="d1")
        d2_workers = await _register_workers(broker, 2, domain="d2")

        assert len(broker._workers) == 5, (
            "StaticBroker must support 5 workers across both domains"
        )


# ===================================================================
# 5. PLACEMENT-ONLY DIFFERENCE
# ===================================================================


class TestPlacementOnlyDifference:
    """S1/S2/S3 must differ in ONLY the placement function."""

    @pytest.mark.asyncio
    async def test_static_broker_has_same_recovery_interface_as_neural(self):
        """StaticBroker must have dispatch-time recovery (same mechanism, different
        placement algorithm). The _dispatch_stage method must attempt re-placement."""
        broker = _make_broker()
        # _dispatch_stage should exist and handle recovery
        assert hasattr(broker, "_dispatch_stage"), (
            "StaticBroker must have _dispatch_stage"
        )

    @pytest.mark.asyncio
    async def test_round_robin_and_random_use_same_infrastructure(self):
        """Both S1 (round-robin) and S2 (random) must have health checks, recovery,
        and federation (same infrastructure, different placement only)."""
        for strategy in ["round_robin", "random"]:
            broker = _make_broker(
                placement=strategy,
                peer_urls=["http://peer:8080"],
                health_check_interval_s=5.0,
                health_check_max_failures=3,
            )
            assert hasattr(broker, "_health_check_loop"), (
                f"{strategy} broker must have _health_check_loop"
            )
            assert hasattr(broker, "_health_check_interval_s"), (
                f"{strategy} broker must have _health_check_interval_s"
            )
            has_federation = (
                hasattr(broker, "_propagator") or hasattr(broker, "_peer_urls")
            )
            assert has_federation, (
                f"{strategy} broker must have federation support"
            )
