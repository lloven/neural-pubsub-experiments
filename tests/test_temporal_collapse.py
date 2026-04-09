"""Reproduce the temporal collapse bug: ~80 pipelines succeed, then failure cascade.

Root cause hypothesis: httpx default connection pool (20 keepalive) is
exhausted when 48 workers receive concurrent dispatches + health checks.

This test creates a broker with many workers and fires rapid publishes
to verify whether the broker can sustain throughput beyond the collapse point.
We use mock workers (echo servers) to avoid real HTTP calls.
"""

from __future__ import annotations

import asyncio

import pytest

from src.broker.neural_broker import BrokerConfig, NeuralBroker
from src.broker.models import WorkerInfo


def _make_broker(n_workers: int = 48, placement_mode: str = "neural") -> NeuralBroker:
    broker = NeuralBroker(BrokerConfig(
        domain_id="d1", broker_id="b1",
        placement_mode=placement_mode, governance_enabled=False,
    ))
    for i in range(n_workers):
        d = "d%d" % (1 + i // 12)
        broker._workers["w%d" % i] = WorkerInfo(
            node_id="w%d" % i, domain_id=d, slice_id="flat",
            capacity=5.0, url="http://127.0.0.1:%d" % (9100 + i),
            bid_cost_ms=100.0,
        )
    broker._rebuild_topology()
    return broker


class TestTemporalCollapse:
    """Verify the broker can sustain >80 publishes without collapse."""

    @pytest.mark.asyncio
    async def test_100_sequential_publishes_all_placed(self):
        """100 sequential publishes should all get valid placements.

        Even if dispatch fails (no real workers), placement should succeed
        for every publish. If connection pool or load tracking causes
        placement failures after ~80, this test catches it.
        """
        broker = _make_broker(48)

        from httpx import ASGITransport, AsyncClient
        app = broker.build_app()

        successes = 0
        failures = 0
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            for i in range(100):
                resp = await c.post("/publish", json={
                    "pipeline_type": "cqi_chain", "config": {},
                })
                if resp.status_code == 200:
                    successes += 1
                else:
                    failures += 1

        # All 100 should get placement (200 OK).
        # Dispatch to workers may fail (no real servers), but placement
        # itself must succeed.
        assert successes == 100, (
            f"Expected 100 successful placements, got {successes} "
            f"({failures} failures). Temporal collapse at publish #{successes+1}?"
        )

    @pytest.mark.asyncio
    async def test_100_publishes_entangled_dag(self):
        """100 publishes of entangled DAG should all get placements."""
        broker = _make_broker(48)

        from httpx import ASGITransport, AsyncClient
        app = broker.build_app()

        successes = 0
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            for i in range(100):
                resp = await c.post("/publish", json={
                    "pipeline_type": "ran_entangled", "config": {},
                })
                if resp.status_code == 200:
                    successes += 1

        assert successes == 100, (
            f"Entangled DAG: {successes}/100 placements succeeded. "
            f"Collapse at #{successes+1}?"
        )

    @pytest.mark.asyncio
    async def test_200_publishes_entangled_dag(self):
        """200 publishes of entangled DAG should all get placements.

        With per-stage load reservation (at dispatch, not placement),
        the broker should sustain indefinite publishes because only
        in-flight stages occupy capacity, not entire pipelines.
        """
        broker = _make_broker(48)

        from httpx import ASGITransport, AsyncClient
        app = broker.build_app()

        successes = 0
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            for i in range(200):
                resp = await c.post("/publish", json={
                    "pipeline_type": "ran_entangled", "config": {},
                })
                if resp.status_code == 200:
                    successes += 1

        assert successes == 200, (
            f"Entangled DAG: {successes}/200 placements succeeded. "
            f"Load reservation still exhausting capacity?"
        )

    @pytest.mark.asyncio
    async def test_worker_load_does_not_grow_unbounded(self):
        """After 50 publishes, total worker load should not exceed capacity.

        Load reservation increases load, dispatch failure should release it.
        If release is broken, load accumulates and placement fails.
        """
        broker = _make_broker(48)

        from httpx import ASGITransport, AsyncClient
        app = broker.build_app()

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            for i in range(50):
                await c.post("/publish", json={
                    "pipeline_type": "cqi_chain", "config": {},
                })

        total_load = sum(w.current_load for w in broker._workers.values())
        total_capacity = sum(5.0 for _ in broker._workers.values())  # capacity=5.0 each
        assert total_load <= total_capacity, (
            f"Total load {total_load:.1f} exceeds capacity {total_capacity:.1f}. "
            f"Load reservation not properly released after dispatch failure?"
        )
