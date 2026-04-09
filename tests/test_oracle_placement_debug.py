"""Debug tests for oracle placement with 8-stage pipelines.

Verifies that find_placement() works correctly with:
- 12 URLLC workers (single domain, as in the broken oracle deployment)
- 48 mixed-slice workers (4 domains, as in the correct oracle deployment)
"""

from __future__ import annotations

import pytest

from src.broker.neural_broker import BrokerConfig, NeuralBroker
from src.broker.models import WorkerInfo


def _register_workers(broker: NeuralBroker, domains: dict[str, tuple[str, int]]):
    """Register workers with the broker.

    Args:
        domains: {domain_id: (slice_id, count)} mapping.
    """
    for d, (slice_id, count) in domains.items():
        for i in range(count):
            wid = f"{d}-worker-{i}"
            broker._workers[wid] = WorkerInfo(
                node_id=wid,
                domain_id=d,
                slice_id=slice_id,
                capacity=1.0,
                url=f"http://10.0.0.1:{8081 + i}",
                bid_cost_ms=100.0,
            )
    broker._rebuild_topology()


@pytest.mark.asyncio
async def test_find_placement_12_urllc_workers_cqi_chain():
    """12 URLLC workers in d1 can place an 8-stage CQI chain (neural mode)."""
    broker = NeuralBroker(BrokerConfig(
        domain_id="d1", broker_id="b1",
        placement_mode="neural", governance_enabled=True,
    ))
    _register_workers(broker, {"d1": ("URLLC", 12)})

    from httpx import ASGITransport, AsyncClient
    app = broker.build_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/publish", json={"pipeline_type": "cqi_chain", "config": {}})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    assert len(resp.json()["placement"]) == 8


@pytest.mark.asyncio
async def test_find_placement_12_urllc_workers_anomaly_sp():
    """12 URLLC workers in d1 can place an 8-stage anomaly detection (neural mode)."""
    broker = NeuralBroker(BrokerConfig(
        domain_id="d1", broker_id="b1",
        placement_mode="neural", governance_enabled=True,
    ))
    _register_workers(broker, {"d1": ("URLLC", 12)})

    from httpx import ASGITransport, AsyncClient
    app = broker.build_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/publish", json={"pipeline_type": "anomaly_sp", "config": {}})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    assert len(resp.json()["placement"]) == 8


@pytest.mark.asyncio
async def test_find_placement_48_mixed_workers_cqi_chain():
    """48 workers across 4 domains can place 8-stage CQI chain (oracle topology)."""
    broker = NeuralBroker(BrokerConfig(
        domain_id="d1", broker_id="b1",
        placement_mode="neural", governance_enabled=True,
    ))
    _register_workers(broker, {
        "d1": ("URLLC", 12),
        "d2": ("eMBB", 12),
        "d3": ("eMBB", 12),
        "d4": ("best-effort", 12),
    })

    from httpx import ASGITransport, AsyncClient
    app = broker.build_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/publish", json={"pipeline_type": "cqi_chain", "config": {}})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert len(data["placement"]) == 8
    # Note: DP solver may place all stages in d1 (lowest cross-domain
    # penalty). Multi-domain spread is not required for correctness.


@pytest.mark.asyncio
async def test_find_placement_48_workers_ran_entangled():
    """48 workers can place 8-stage RAN Intelligence Suite (entangled, non-tree)."""
    broker = NeuralBroker(BrokerConfig(
        domain_id="d1", broker_id="b1",
        placement_mode="neural", governance_enabled=True,
    ))
    _register_workers(broker, {
        "d1": ("URLLC", 12),
        "d2": ("eMBB", 12),
        "d3": ("eMBB", 12),
        "d4": ("best-effort", 12),
    })

    from httpx import ASGITransport, AsyncClient
    app = broker.build_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/publish", json={"pipeline_type": "ran_entangled", "config": {}})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    assert len(resp.json()["placement"]) == 8
