"""Tests for health-check syncing worker current_load into the broker registry.

Regression: 2026-04-22. At sat-15 (15 pps, no failure) market-quad produced
CR 71.8% vs oracle-global 82.9% on cqi-chain. One root cause: the broker's
health-check loop in ``neural_broker.py`` only probes ``GET /health`` for
availability and discards the ``HealthModel`` response body that contains
each worker's actual ``current_load``. The broker's view of worker
utilization is therefore a local estimate that lags real worker state.

At 15 pps, 5-second lag = 75 pipelines of in-flight load invisible to the
broker, so M/M/1 prices computed from ``current_load`` never spike in time
to guide placement away from saturating workers.

Fix: the health-check loop must parse ``HealthModel.current_load`` from the
JSON response body and sync it into ``WorkerInfo.current_load`` in the
broker's registry. This test pins that behaviour.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import httpx
import pytest

from src.broker.models import WorkerInfo
from src.broker.neural_broker import BrokerConfig, NeuralBroker


def _make_broker(
    health_check_interval_s: float = 0.05,
    health_check_max_failures: int = 3,
) -> NeuralBroker:
    cfg = BrokerConfig(
        domain_id="d1",
        broker_id="neural-d1",
        health_check_interval_s=health_check_interval_s,
        health_check_max_failures=health_check_max_failures,
    )
    return NeuralBroker(cfg)


async def _register_worker(
    broker: NeuralBroker, nid: str, url: str,
    capacity: float = 1.0, current_load: float = 0.0,
) -> None:
    async with broker._workers_lock:
        broker._workers[nid] = WorkerInfo(
            node_id=nid,
            domain_id="d1",
            slice_id="URLLC",
            capacity=capacity,
            current_load=current_load,
            url=url,
        )


class TestHealthCheckSyncsCurrentLoad:
    """NeuralBroker's health-check loop must sync current_load from the
    worker's HealthModel response into the broker's WorkerInfo registry."""

    @pytest.mark.asyncio
    async def test_health_response_updates_worker_current_load(self):
        """After a health probe that returns a HealthModel with non-zero
        current_load, the broker's WorkerInfo must reflect that value.
        Without the fix, broker registry stays at whatever load was last
        computed locally from dispatch/completion events."""
        broker = _make_broker()
        broker._http_client = httpx.AsyncClient()

        await _register_worker(
            broker, nid="w0", url="http://localhost:8081",
            capacity=1.0, current_load=0.0,
        )

        async def mock_get(url, **kwargs):
            # Worker reports current_load=0.7 (heavily loaded)
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            mock_resp.json = MagicMock(return_value={
                "node_id": "w0",
                "domain_id": "d1",
                "slice_id": "URLLC",
                "capacity": 1.0,
                "current_load": 0.7,
                "registered": True,
            })
            return mock_resp

        broker._http_client.get = mock_get

        task = asyncio.create_task(broker._health_check_loop())
        await asyncio.sleep(0.2)  # multiple health check intervals
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await broker._http_client.aclose()

        assert broker._workers["w0"].current_load == pytest.approx(0.7), (
            "Health check must sync current_load from HealthModel response; "
            f"got {broker._workers['w0'].current_load}"
        )

    @pytest.mark.asyncio
    async def test_health_response_updates_across_probes(self):
        """Worker load is time-varying. Each successful probe must refresh
        the broker's view; stale values between probes are allowed (5 s
        interval) but the MOST RECENT probe's value must be reflected."""
        broker = _make_broker(health_check_interval_s=0.05)
        broker._http_client = httpx.AsyncClient()

        await _register_worker(broker, "w0", "http://localhost:8081")

        # Load rises over time: 0.2 → 0.5 → 0.9
        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            load_values = [0.2, 0.5, 0.9]
            load = load_values[min(call_count - 1, len(load_values) - 1)]
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            mock_resp.json = MagicMock(return_value={
                "node_id": "w0", "domain_id": "d1", "slice_id": "URLLC",
                "capacity": 1.0, "current_load": load, "registered": True,
            })
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

        # After several probes, load should have converged to the last
        # reported value (0.9)
        assert broker._workers["w0"].current_load == pytest.approx(0.9), (
            f"Latest health probe value (0.9) must be reflected; "
            f"got {broker._workers['w0'].current_load}"
        )

    @pytest.mark.asyncio
    async def test_failed_probe_does_not_overwrite_load(self):
        """A failed health probe (connection error) must NOT overwrite
        current_load with zero or garbage. The worker's load state stays
        at its last known value."""
        broker = _make_broker(health_check_interval_s=0.05, health_check_max_failures=10)
        broker._http_client = httpx.AsyncClient()

        await _register_worker(
            broker, "w0", "http://localhost:8081",
            capacity=1.0, current_load=0.4,
        )

        async def mock_get(url, **kwargs):
            raise httpx.ConnectError("Connection refused")

        broker._http_client.get = mock_get

        task = asyncio.create_task(broker._health_check_loop())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await broker._http_client.aclose()

        # Worker still in registry (under max_failures), load untouched
        assert "w0" in broker._workers
        assert broker._workers["w0"].current_load == pytest.approx(0.4), (
            "Failed probe must not reset current_load; "
            f"got {broker._workers['w0'].current_load}"
        )

    @pytest.mark.asyncio
    async def test_health_response_missing_current_load_is_tolerated(self):
        """Backwards compatibility: if a worker returns a response without
        current_load (e.g., older worker version or parsing failure), the
        broker must not crash or reset the field. It should skip the sync
        for that probe."""
        broker = _make_broker()
        broker._http_client = httpx.AsyncClient()

        await _register_worker(
            broker, "w0", "http://localhost:8081", current_load=0.3,
        )

        async def mock_get(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            # JSON without current_load
            mock_resp.json = MagicMock(return_value={
                "node_id": "w0", "domain_id": "d1", "slice_id": "URLLC",
                "capacity": 1.0, "registered": True,
            })
            return mock_resp

        broker._http_client.get = mock_get

        task = asyncio.create_task(broker._health_check_loop())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await broker._http_client.aclose()

        # Worker retained, current_load unchanged
        assert "w0" in broker._workers
        assert broker._workers["w0"].current_load == pytest.approx(0.3)
