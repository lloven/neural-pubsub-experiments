"""Unit tests for the Worker class (worker/worker.py).

Tests cover: registration, execute_stage, report_result, health check,
error handling, processing time fidelity, concurrent execution, and
slice affinity.  All broker interactions are mocked via httpx transport.

TDD approach: tests written first against the existing Worker interface.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio

from src.worker.worker import (
    HealthModel,
    StageAssignment,
    StageResult,
    Worker,
    WorkerConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_config(**overrides) -> WorkerConfig:
    """Build a WorkerConfig with sensible defaults for testing."""
    defaults = dict(
        node_id="test-worker-1",
        domain_id="d1",
        slice_id="URLLC",
        capacity=1.0,
        broker_url="http://fake-broker:8080",
        processing_speed=0.01,  # Fast sleep for unit tests
        port=9999,
    )
    defaults.update(overrides)
    return WorkerConfig(**defaults)


def _make_assignment(**overrides) -> StageAssignment:
    """Build a StageAssignment with sensible defaults."""
    defaults = dict(
        pipeline_id="pipe-001",
        stage_id="stage-A",
        stage_type="predict",
        computational_demand=0.5,
        input_data=b"",
        metadata={},
    )
    defaults.update(overrides)
    return StageAssignment(**defaults)


class FakeBrokerTransport(httpx.AsyncBaseTransport):
    """Mock transport that records requests and returns configurable responses.

    Attributes:
        requests: List of (method, url, json_body) tuples recorded.
        register_status: HTTP status code for POST /register.
        result_status: HTTP status code for POST /result.
        should_raise: If set, raises this exception instead of responding.
    """

    def __init__(
        self,
        register_status: int = 200,
        result_status: int = 200,
        should_raise: Exception | None = None,
    ):
        self.requests: list[tuple[str, str, dict | None]] = []
        self.register_status = register_status
        self.result_status = result_status
        self.should_raise = should_raise

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.should_raise is not None:
            raise self.should_raise

        # Read and decode body
        body = request.content
        json_body = None
        if body:
            import json
            try:
                json_body = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        url_str = str(request.url)
        self.requests.append((request.method, url_str, json_body))

        if "/register" in url_str and request.method == "POST":
            return httpx.Response(self.register_status, json={"status": "ok"})
        if "/result" in url_str and request.method == "POST":
            return httpx.Response(self.result_status, json={"status": "ok"})
        if "/register/" in url_str and request.method == "DELETE":
            return httpx.Response(200, json={"status": "ok"})

        return httpx.Response(404, json={"detail": "not found"})


@pytest.fixture
def config() -> WorkerConfig:
    return _default_config()


@pytest.fixture
def transport() -> FakeBrokerTransport:
    return FakeBrokerTransport()


@pytest.fixture
def worker_with_client(config, transport):
    """Create a Worker with a pre-initialised httpx client using fake transport."""
    worker = Worker(config)
    worker._http_client = httpx.AsyncClient(transport=transport)
    return worker


# ===================================================================
# 1. Registration tests
# ===================================================================


class TestRegistration:
    """Worker registers with broker on startup, sends correct metadata."""

    @pytest.mark.asyncio
    async def test_register_sends_correct_metadata(self, worker_with_client, transport, config):
        """Registration POST must include node_id, domain_id, slice_id, capacity."""
        await worker_with_client.register()

        assert len(transport.requests) == 1
        method, url, body = transport.requests[0]
        assert method == "POST"
        assert "/register" in url
        assert body["node_id"] == config.node_id
        assert body["domain_id"] == config.domain_id
        assert body["slice_id"] == config.slice_id
        assert body["capacity"] == config.capacity

    @pytest.mark.asyncio
    async def test_register_sets_registered_flag(self, worker_with_client):
        """After successful registration, _registered must be True."""
        assert worker_with_client._registered is False
        await worker_with_client.register()
        assert worker_with_client._registered is True

    @pytest.mark.asyncio
    async def test_register_targets_broker_url(self, transport):
        """Registration URL is {broker_url}/register."""
        cfg = _default_config(broker_url="http://my-broker:9090")
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        await worker.register()

        _, url, _ = transport.requests[0]
        assert url == "http://my-broker:9090/register"

    @pytest.mark.asyncio
    async def test_register_raises_on_non_2xx(self):
        """Non-2xx broker response must raise HTTPStatusError."""
        transport = FakeBrokerTransport(register_status=500)
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        with pytest.raises(httpx.HTTPStatusError):
            await worker.register()

        # _registered must stay False
        assert worker._registered is False


# ===================================================================
# 2. Execute stage tests
# ===================================================================


class TestExecuteStage:
    """Worker receives stage task, simulates processing, returns result."""

    @pytest.mark.asyncio
    async def test_execute_returns_stage_result(self, worker_with_client):
        """execute_stage returns a StageResult with correct pipeline/stage IDs."""
        assignment = _make_assignment(pipeline_id="p1", stage_id="s1")
        result = await worker_with_client.execute_stage(assignment)

        assert isinstance(result, StageResult)
        assert result.pipeline_id == "p1"
        assert result.stage_id == "s1"
        assert result.success is True
        assert result.error is None

    @pytest.mark.asyncio
    async def test_execute_sets_node_id_on_result(self, worker_with_client, config):
        """Result node_id must match the worker's node_id."""
        assignment = _make_assignment()
        result = await worker_with_client.execute_stage(assignment)
        assert result.node_id == config.node_id

    @pytest.mark.asyncio
    async def test_execute_records_timing(self, worker_with_client):
        """start_time < end_time and processing_time_ms > 0."""
        assignment = _make_assignment(computational_demand=0.1)
        result = await worker_with_client.execute_stage(assignment)

        assert result.start_time < result.end_time
        assert result.processing_time_ms > 0

    @pytest.mark.asyncio
    async def test_execute_restores_load_after_completion(self, worker_with_client):
        """Current load must return to 0 after a single stage completes."""
        worker_with_client._current_load = 0.0
        assignment = _make_assignment(computational_demand=0.3)
        await worker_with_client.execute_stage(assignment)
        assert worker_with_client._current_load == pytest.approx(0.0, abs=1e-9)

    @pytest.mark.asyncio
    async def test_execute_caps_load_at_capacity(self, worker_with_client, config):
        """Load during execution must not exceed worker capacity."""
        # Use a demand greater than capacity
        assignment = _make_assignment(computational_demand=config.capacity + 0.5)

        loads_during_exec = []
        original_sleep = asyncio.sleep

        async def spy_sleep(delay):
            loads_during_exec.append(worker_with_client._current_load)
            await original_sleep(delay)

        with patch("src.worker.worker.asyncio.sleep", side_effect=spy_sleep):
            await worker_with_client.execute_stage(assignment)

        for load in loads_during_exec:
            assert load <= config.capacity


# ===================================================================
# 3. Report result tests
# ===================================================================


class TestReportResult:
    """Worker sends result back to broker with correct payload."""

    @pytest.mark.asyncio
    async def test_report_sends_correct_payload(self, worker_with_client, transport):
        """_report_result POSTs pipeline_id, stage_id, node_id, timing."""
        assignment = _make_assignment(pipeline_id="p42", stage_id="sX")
        result = await worker_with_client.execute_stage(assignment)

        # Find the /result request
        result_reqs = [(m, u, b) for m, u, b in transport.requests if "/result" in u]
        assert len(result_reqs) == 1

        _, url, body = result_reqs[0]
        assert body["pipeline_id"] == "p42"
        assert body["stage_id"] == "sX"
        assert body["node_id"] == worker_with_client.config.node_id
        assert "start_time" in body
        assert "end_time" in body
        assert "processing_time_ms" in body
        assert body["success"] is True

    @pytest.mark.asyncio
    async def test_report_targets_broker_result_endpoint(self, transport):
        """Result POST goes to {broker_url}/result."""
        cfg = _default_config(broker_url="http://results-broker:7070")
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignment = _make_assignment()
        await worker.execute_stage(assignment)

        result_reqs = [(m, u, b) for m, u, b in transport.requests if "/result" in u]
        assert len(result_reqs) == 1
        _, url, _ = result_reqs[0]
        assert url == "http://results-broker:7070/result"

    @pytest.mark.asyncio
    async def test_report_includes_error_on_failure(self, worker_with_client, transport):
        """When execute_stage catches an exception, result includes error string."""
        assignment = _make_assignment()

        # Force an exception during sleep
        with patch(
            "src.worker.worker.asyncio.sleep",
            side_effect=RuntimeError("simulated crash"),
        ):
            result = await worker_with_client.execute_stage(assignment)

        assert result.success is False
        assert "simulated crash" in result.error

        result_reqs = [(m, u, b) for m, u, b in transport.requests if "/result" in u]
        assert len(result_reqs) == 1
        _, _, body = result_reqs[0]
        assert body["success"] is False
        assert "simulated crash" in body["error"]


# ===================================================================
# 4. Health check tests
# ===================================================================


class TestHealthCheck:
    """Worker responds to health pings with correct state."""

    @pytest.mark.asyncio
    async def test_health_endpoint_returns_correct_fields(self, config):
        """GET /health returns node_id, domain_id, slice_id, capacity, load, registered."""
        from httpx import ASGITransport, AsyncClient

        worker = Worker(config)
        async with AsyncClient(
            transport=ASGITransport(app=worker._app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")

        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == config.node_id
        assert data["domain_id"] == config.domain_id
        assert data["slice_id"] == config.slice_id
        assert data["capacity"] == config.capacity
        assert data["current_load"] == 0.0
        assert data["registered"] is False

    @pytest.mark.asyncio
    async def test_health_reflects_registered_state(self, config, transport):
        """After registration, health endpoint shows registered=True."""
        from httpx import ASGITransport, AsyncClient

        worker = Worker(config)
        worker._http_client = httpx.AsyncClient(transport=transport)
        await worker.register()

        async with AsyncClient(
            transport=ASGITransport(app=worker._app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")

        assert resp.json()["registered"] is True


# ===================================================================
# 5. Error handling tests
# ===================================================================


class TestErrorHandling:
    """Broker unreachable during registration and result reporting."""

    @pytest.mark.asyncio
    async def test_register_raises_on_connection_error(self):
        """ConnectError from broker must propagate as exception."""
        transport = FakeBrokerTransport(
            should_raise=httpx.ConnectError("connection refused")
        )
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        with pytest.raises(httpx.ConnectError):
            await worker.register()

        assert worker._registered is False

    @pytest.mark.asyncio
    async def test_report_result_swallows_connection_error(self):
        """If broker is unreachable during result reporting, execute_stage still returns."""
        transport = FakeBrokerTransport()
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        # Registration succeeds, then swap transport to one that fails
        await worker.register()

        failing_transport = FakeBrokerTransport(
            should_raise=httpx.ConnectError("gone")
        )
        worker._http_client = httpx.AsyncClient(transport=failing_transport)

        # execute_stage should NOT raise, even though reporting fails
        assignment = _make_assignment()
        result = await worker.execute_stage(assignment)

        assert result.success is True  # execution itself succeeded
        assert result.pipeline_id == assignment.pipeline_id

    @pytest.mark.asyncio
    async def test_report_result_swallows_http_500(self):
        """If broker returns 500 during result reporting, execute_stage still returns."""
        transport = FakeBrokerTransport(result_status=500)
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignment = _make_assignment()
        result = await worker.execute_stage(assignment)

        # The stage execution succeeded; only the reporting failed
        assert result.success is True

    @pytest.mark.asyncio
    async def test_execute_with_no_http_client(self):
        """If _http_client is None, _report_result should not crash."""
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = None  # Simulate no client

        assignment = _make_assignment()
        result = await worker.execute_stage(assignment)

        assert result.success is True


# ===================================================================
# 6. Processing time fidelity
# ===================================================================


class TestProcessingTimeFidelity:
    """Verify processing time matches configured delay."""

    @pytest.mark.asyncio
    async def test_sleep_equals_demand_times_speed(self):
        """sleep_seconds = computational_demand * processing_speed."""
        calls = []
        original_sleep = asyncio.sleep

        async def record_sleep(delay):
            calls.append(delay)
            # Don't actually sleep to keep test fast
            return

        cfg = _default_config(processing_speed=2.0)
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(
            transport=FakeBrokerTransport()
        )

        assignment = _make_assignment(computational_demand=0.5)

        with patch("src.worker.worker.asyncio.sleep", side_effect=record_sleep):
            await worker.execute_stage(assignment)

        assert len(calls) == 1
        assert calls[0] == pytest.approx(1.0)  # 0.5 * 2.0

    @pytest.mark.asyncio
    async def test_processing_time_ms_reflects_wall_clock(self):
        """processing_time_ms should approximate actual elapsed time."""
        cfg = _default_config(processing_speed=0.01)
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(
            transport=FakeBrokerTransport()
        )

        assignment = _make_assignment(computational_demand=1.0)
        # Expected sleep: 1.0 * 0.01 = 0.01 s = 10 ms

        t0 = time.time()
        result = await worker.execute_stage(assignment)
        wall_ms = (time.time() - t0) * 1000

        # processing_time_ms should be close to wall clock
        assert result.processing_time_ms == pytest.approx(wall_ms, rel=0.5)

    @pytest.mark.asyncio
    async def test_zero_demand_zero_sleep(self):
        """Zero computational demand should result in zero sleep time."""
        calls = []

        async def record_sleep(delay):
            calls.append(delay)

        cfg = _default_config(processing_speed=1.0)
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(
            transport=FakeBrokerTransport()
        )

        assignment = _make_assignment(computational_demand=0.0)

        with patch("src.worker.worker.asyncio.sleep", side_effect=record_sleep):
            await worker.execute_stage(assignment)

        assert calls[0] == pytest.approx(0.0)


# ===================================================================
# 7. Concurrent execution
# ===================================================================


class TestConcurrentExecution:
    """Multiple stages executing simultaneously."""

    @pytest.mark.asyncio
    async def test_concurrent_stages_both_complete(self):
        """Two stages launched concurrently both produce results."""
        cfg = _default_config(processing_speed=0.01)
        transport = FakeBrokerTransport()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        a1 = _make_assignment(pipeline_id="p1", stage_id="s1", computational_demand=0.1)
        a2 = _make_assignment(pipeline_id="p2", stage_id="s2", computational_demand=0.1)

        r1, r2 = await asyncio.gather(
            worker.execute_stage(a1),
            worker.execute_stage(a2),
        )

        assert r1.success is True
        assert r2.success is True
        assert r1.pipeline_id == "p1"
        assert r2.pipeline_id == "p2"

    @pytest.mark.asyncio
    async def test_concurrent_load_tracking(self):
        """During concurrent execution, load increases then returns to 0."""
        cfg = _default_config(processing_speed=0.05, capacity=2.0)
        transport = FakeBrokerTransport()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        peak_loads = []
        original_sleep = asyncio.sleep

        async def spy_sleep(delay):
            peak_loads.append(worker._current_load)
            await original_sleep(delay)

        a1 = _make_assignment(computational_demand=0.5)
        a2 = _make_assignment(computational_demand=0.5)

        with patch("src.worker.worker.asyncio.sleep", side_effect=spy_sleep):
            await asyncio.gather(
                worker.execute_stage(a1),
                worker.execute_stage(a2),
            )

        # After both complete, load should be back to 0
        assert worker._current_load == pytest.approx(0.0, abs=1e-9)

        # During execution, at least one spy should have seen load > 0
        assert any(load > 0 for load in peak_loads)

    @pytest.mark.asyncio
    async def test_concurrent_reports_all_sent(self):
        """Each concurrent stage should report its result independently."""
        cfg = _default_config(processing_speed=0.01)
        transport = FakeBrokerTransport()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignments = [
            _make_assignment(pipeline_id=f"p{i}", stage_id=f"s{i}")
            for i in range(3)
        ]

        await asyncio.gather(*(worker.execute_stage(a) for a in assignments))

        result_reqs = [b for m, u, b in transport.requests if "/result" in u]
        assert len(result_reqs) == 3

        reported_pipelines = {r["pipeline_id"] for r in result_reqs}
        assert reported_pipelines == {"p0", "p1", "p2"}


# ===================================================================
# 8. Slice affinity
# ===================================================================


class TestSliceAffinity:
    """Worker reports correct slice_id in all contexts."""

    @pytest.mark.asyncio
    async def test_registration_includes_slice_id(self):
        """Registration payload must include the configured slice_id."""
        transport = FakeBrokerTransport()
        cfg = _default_config(slice_id="eMBB")
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        await worker.register()

        _, _, body = transport.requests[0]
        assert body["slice_id"] == "eMBB"

    @pytest.mark.asyncio
    async def test_health_reports_slice_id(self):
        """Health endpoint must report the configured slice_id."""
        from httpx import ASGITransport, AsyncClient

        cfg = _default_config(slice_id="URLLC")
        worker = Worker(cfg)

        async with AsyncClient(
            transport=ASGITransport(app=worker._app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")

        assert resp.json()["slice_id"] == "URLLC"

    @pytest.mark.asyncio
    async def test_flat_slice_id_works(self):
        """A 'flat' slice_id (non-sliced baseline) must be accepted."""
        transport = FakeBrokerTransport()
        cfg = _default_config(slice_id="flat")
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        await worker.register()
        _, _, body = transport.requests[0]
        assert body["slice_id"] == "flat"

        # Also verify execute works with flat slice
        assignment = _make_assignment()
        result = await worker.execute_stage(assignment)
        assert result.success is True


# ===================================================================
# 9. Shutdown / deregistration
# ===================================================================


class TestShutdown:
    """Shutdown deregisters from broker and cleans up."""

    @pytest.mark.asyncio
    async def test_shutdown_sends_deregister(self, transport, config):
        """Shutdown DELETEs /register/{node_id}."""
        worker = Worker(config)
        worker._http_client = httpx.AsyncClient(transport=transport)
        worker._registered = True

        await worker.shutdown()

        delete_reqs = [(m, u) for m, u, _ in transport.requests if m == "DELETE"]
        assert len(delete_reqs) == 1
        _, url = delete_reqs[0]
        assert f"/register/{config.node_id}" in url

    @pytest.mark.asyncio
    async def test_shutdown_clears_registered_flag(self, transport, config):
        """After shutdown, _registered must be False."""
        worker = Worker(config)
        worker._http_client = httpx.AsyncClient(transport=transport)
        worker._registered = True

        await worker.shutdown()
        assert worker._registered is False

    @pytest.mark.asyncio
    async def test_shutdown_closes_http_client(self, transport, config):
        """After shutdown, _http_client must be None."""
        worker = Worker(config)
        worker._http_client = httpx.AsyncClient(transport=transport)

        await worker.shutdown()
        assert worker._http_client is None

    @pytest.mark.asyncio
    async def test_shutdown_tolerates_broker_failure(self):
        """Shutdown must complete even if deregistration fails."""
        transport = FakeBrokerTransport(
            should_raise=httpx.ConnectError("broker gone")
        )
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)
        worker._registered = True

        # Should NOT raise
        await worker.shutdown()

        assert worker._registered is False
        assert worker._http_client is None


# ===================================================================
# 10. Execute endpoint (HTTP API)
# ===================================================================


class TestExecuteEndpoint:
    """POST /execute accepts a StageAssignment and returns a StageResult."""

    @pytest.mark.asyncio
    async def test_execute_endpoint_returns_result(self, config, transport):
        """POST /execute with valid payload returns 200 with result fields."""
        from httpx import ASGITransport, AsyncClient

        worker = Worker(config)
        worker._http_client = httpx.AsyncClient(transport=transport)

        payload = {
            "pipeline_id": "p-http",
            "stage_id": "s-http",
            "stage_type": "aggregate",
            "computational_demand": 0.01,
        }

        async with AsyncClient(
            transport=ASGITransport(app=worker._app), base_url="http://test"
        ) as client:
            resp = await client.post("/execute", json=payload)

        assert resp.status_code == 200
        data = resp.json()
        assert data["pipeline_id"] == "p-http"
        assert data["stage_id"] == "s-http"
        assert data["node_id"] == config.node_id
        assert data["success"] is True


# ===================================================================
# 11. Bug documentation (DO NOT FIX — document only)
# ===================================================================


class TestBugFixes:
    """Tests for the three Worker bugs. Written RED-first per TDD skill."""

    # --- Bug 1: register() before run() should raise RuntimeError ---

    @pytest.mark.asyncio
    async def test_register_before_run_raises_runtime_error(self):
        """register() without run() must raise RuntimeError, not AssertionError."""
        cfg = _default_config()
        worker = Worker(cfg)
        # _http_client is None because run() was never called

        with pytest.raises(RuntimeError, match="Worker not started"):
            await worker.register()

    @pytest.mark.asyncio
    async def test_register_before_run_does_not_raise_assertion_error(self):
        """Bare AssertionError must not escape from register()."""
        cfg = _default_config()
        worker = Worker(cfg)

        with pytest.raises(RuntimeError):
            await worker.register()
        # If it raised AssertionError, the above would fail

    # --- Bug 2: load tracking must use asyncio.Lock ---

    @pytest.mark.asyncio
    async def test_worker_has_load_lock(self):
        """Worker must have an asyncio.Lock for _current_load protection."""
        cfg = _default_config()
        worker = Worker(cfg)
        assert hasattr(worker, "_load_lock")
        assert isinstance(worker._load_lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_concurrent_load_returns_to_zero_with_lock(self):
        """After many concurrent stages, load must be exactly 0.0 (lock-protected)."""
        cfg = _default_config(processing_speed=0.001, capacity=10.0)
        transport = FakeBrokerTransport()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignments = [
            _make_assignment(computational_demand=0.1, pipeline_id=f"p{i}", stage_id=f"s{i}")
            for i in range(20)
        ]
        await asyncio.gather(*(worker.execute_stage(a) for a in assignments))

        assert worker._current_load == pytest.approx(0.0, abs=1e-9)

    # --- Bug 3: _report_result must retry once and log WARNING ---

    @pytest.mark.asyncio
    async def test_report_result_retries_once_on_failure(self):
        """_report_result must attempt the POST twice before giving up."""
        transport = FakeBrokerTransport(result_status=500)
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignment = _make_assignment(pipeline_id="p-retry", stage_id="s-retry")
        result = await worker.execute_stage(assignment)

        # Worker must not raise (continues serving)
        assert result.success is True

        # Two /result attempts (original + 1 retry)
        result_reqs = [(m, u, b) for m, u, b in transport.requests if "/result" in u]
        assert len(result_reqs) == 2

    @pytest.mark.asyncio
    async def test_report_result_logs_warning_with_ids_on_failure(self):
        """On report failure after retry, a WARNING must include pipeline_id and stage_id."""
        transport = FakeBrokerTransport(result_status=500)
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignment = _make_assignment(pipeline_id="p-warn", stage_id="s-warn")

        with patch("src.worker.worker.logger") as mock_logger:
            await worker.execute_stage(assignment)

            # Find the WARNING call that includes both IDs
            warning_calls = [
                call for call in mock_logger.warning.call_args_list
                if "p-warn" in str(call) and "s-warn" in str(call)
            ]
            assert len(warning_calls) >= 1, (
                f"Expected WARNING with pipeline_id='p-warn' and stage_id='s-warn', "
                f"got: {mock_logger.warning.call_args_list}"
            )

    @pytest.mark.asyncio
    async def test_report_result_succeeds_on_retry(self):
        """If the first attempt fails but the retry succeeds, only one retry happens."""
        # Custom transport: first /result returns 500, second returns 200
        call_count = 0
        original_transport = FakeBrokerTransport()

        class RetryOnceTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                nonlocal call_count
                url_str = str(request.url)
                if "/result" in url_str and request.method == "POST":
                    call_count += 1
                    if call_count == 1:
                        return httpx.Response(500, json={"error": "temporary"})
                    return httpx.Response(200, json={"status": "ok"})
                return await original_transport.handle_async_request(request)

        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=RetryOnceTransport())

        assignment = _make_assignment()
        result = await worker.execute_stage(assignment)

        assert result.success is True
        assert call_count == 2  # first failed, retry succeeded

    @pytest.mark.asyncio
    async def test_report_result_does_not_raise_on_double_failure(self):
        """If both attempts fail, worker must not raise (continues serving)."""
        transport = FakeBrokerTransport(
            should_raise=httpx.ConnectError("broker gone")
        )
        cfg = _default_config()
        worker = Worker(cfg)
        worker._http_client = httpx.AsyncClient(transport=transport)

        assignment = _make_assignment()
        # Must not raise
        result = await worker.execute_stage(assignment)
        assert result.success is True
