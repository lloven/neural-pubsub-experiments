"""Unit tests for baseline brokers (static_broker and kafka_broker).

Tests the StaticBroker and KafkaBroker placement logic without requiring
a running Kafka cluster or Docker infrastructure.
"""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest

from src.broker.kafka_broker import KafkaBroker
from src.broker.models import WorkerInfo
from src.broker.static_broker import PlacementStrategy, StaticBroker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _register_workers(broker: StaticBroker | KafkaBroker, n: int) -> list[str]:
    """Register *n* workers with the broker and return their node_ids."""
    node_ids = []
    for i in range(n):
        nid = f"worker-{i}"
        async with broker._workers_lock:
            broker._workers[nid] = WorkerInfo(
                node_id=nid,
                domain_id="d1",
                slice_id="eMBB",
                capacity=1.0,
                url=f"http://localhost:{8081 + i}",
            )
            node_ids.append(nid)
    async with broker._workers_lock:
        broker._on_worker_change()
    return node_ids


# ---------------------------------------------------------------------------
# StaticBroker: round-robin
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_static_broker_round_robin():
    """StaticBroker round-robin assigns stages cyclically across workers."""
    broker = StaticBroker(domain_id="d1", broker_id="test-rr", placement="round_robin")
    node_ids = await _register_workers(broker, 3)

    from src.pipeline.patterns import cqi_prediction_pipeline

    dag = cqi_prediction_pipeline()
    order = dag.topological_sort()
    assert len(order) == 3

    async with broker._workers_lock:
        placement = broker._compute_placement(dag)

    # All 3 stages should be assigned
    assert set(placement.keys()) == set(order)

    # Each stage should be assigned to a registered worker
    for stage_id, node_id in placement.items():
        assert node_id in node_ids, f"Stage '{stage_id}' assigned to unknown worker '{node_id}'"

    # Round-robin should distribute evenly across 3 workers for 3 stages
    assigned_workers = [placement[sid] for sid in order]
    assert len(set(assigned_workers)) == 3, (
        f"Expected 3 distinct workers for 3 stages, got {assigned_workers}"
    )

    # Verify round-robin cycling: a second pipeline should restart the cycle
    # from where the first left off (or wrap around)
    async with broker._workers_lock:
        placement2 = broker._compute_placement(dag)
    for stage_id, node_id in placement2.items():
        assert node_id in node_ids


@pytest.mark.asyncio
async def test_static_broker_round_robin_wraps():
    """Round-robin wraps around when there are more stages than workers."""
    broker = StaticBroker(domain_id="d1", broker_id="test-rr-wrap", placement="round_robin")
    node_ids = await _register_workers(broker, 2)

    from src.pipeline.patterns import map_pipeline

    dag = map_pipeline(stage_type="transform", n_stages=4)
    order = dag.topological_sort()
    assert len(order) == 4

    async with broker._workers_lock:
        placement = broker._compute_placement(dag)

    assigned = [placement[sid] for sid in order]
    # With 2 workers and 4 stages, each worker should get exactly 2 stages
    counts = Counter(assigned)
    assert len(counts) == 2
    assert all(c == 2 for c in counts.values()), f"Expected even distribution, got {counts}"


# ---------------------------------------------------------------------------
# StaticBroker: random
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_static_broker_random():
    """StaticBroker random placement assigns all stages to registered workers."""
    broker = StaticBroker(domain_id="d1", broker_id="test-rand", placement="random")
    node_ids = await _register_workers(broker, 3)

    from src.pipeline.patterns import cqi_prediction_pipeline

    dag = cqi_prediction_pipeline()
    order = dag.topological_sort()

    async with broker._workers_lock:
        placement = broker._compute_placement(dag)

    # All stages assigned
    assert set(placement.keys()) == set(order)

    # All assigned to valid workers
    for stage_id, node_id in placement.items():
        assert node_id in node_ids, f"Stage '{stage_id}' assigned to unknown worker '{node_id}'"

    # Run multiple times to verify randomness does not crash
    for _ in range(10):
        async with broker._workers_lock:
            p = broker._compute_placement(dag)
        assert set(p.keys()) == set(order)
        for nid in p.values():
            assert nid in node_ids


# ---------------------------------------------------------------------------
# StaticBroker: edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_static_broker_no_workers():
    """StaticBroker raises RuntimeError when no workers are registered."""
    broker = StaticBroker(domain_id="d1", broker_id="test-empty", placement="round_robin")
    from src.pipeline.patterns import cqi_prediction_pipeline

    dag = cqi_prediction_pipeline()
    with pytest.raises(RuntimeError, match="No workers registered"):
        broker._compute_placement(dag)


@pytest.mark.asyncio
async def test_static_broker_single_worker():
    """All stages land on the sole worker regardless of placement strategy."""
    for strategy in ("round_robin", "random"):
        broker = StaticBroker(domain_id="d1", broker_id=f"test-{strategy}", placement=strategy)
        node_ids = await _register_workers(broker, 1)

        from src.pipeline.patterns import anomaly_detection_pipeline

        dag = anomaly_detection_pipeline()
        async with broker._workers_lock:
            placement = broker._compute_placement(dag)

        assert all(nid == node_ids[0] for nid in placement.values())


# ---------------------------------------------------------------------------
# PlacementStrategy enum
# ---------------------------------------------------------------------------


def test_placement_strategy_values():
    """PlacementStrategy enum has the expected members."""
    assert PlacementStrategy.ROUND_ROBIN.value == "round_robin"
    assert PlacementStrategy.RANDOM.value == "random"
    assert len(PlacementStrategy) == 2


def test_placement_strategy_from_string():
    """PlacementStrategy can be constructed from string values."""
    assert PlacementStrategy("round_robin") is PlacementStrategy.ROUND_ROBIN
    assert PlacementStrategy("random") is PlacementStrategy.RANDOM
    with pytest.raises(ValueError):
        PlacementStrategy("invalid")


def test_static_broker_accepts_enum():
    """StaticBroker accepts PlacementStrategy enum directly."""
    broker = StaticBroker(
        domain_id="d1",
        broker_id="test-enum",
        placement=PlacementStrategy.RANDOM,
    )
    assert broker.placement is PlacementStrategy.RANDOM


def test_static_broker_accepts_string():
    """StaticBroker accepts string placement and converts to enum."""
    broker = StaticBroker(
        domain_id="d1",
        broker_id="test-str",
        placement="round_robin",
    )
    assert broker.placement is PlacementStrategy.ROUND_ROBIN


# ---------------------------------------------------------------------------
# KafkaBroker
# ---------------------------------------------------------------------------


def test_kafka_broker_instantiation():
    """KafkaBroker can be instantiated with default parameters."""
    broker = KafkaBroker(domain_id="d1", broker_id="kafka-test")
    assert broker.domain_id == "d1"
    assert broker.broker_id == "kafka-test"
    assert broker.kafka_bootstrap == "kafka:9092"
    assert broker._producer is None


def test_kafka_broker_custom_bootstrap():
    """KafkaBroker respects custom bootstrap servers."""
    broker = KafkaBroker(
        domain_id="d2",
        broker_id="kafka-d2",
        kafka_bootstrap="localhost:9093",
    )
    assert broker.kafka_bootstrap == "localhost:9093"


def test_kafka_broker_placement_returns_sentinel():
    """KafkaBroker._compute_placement returns 'kafka' for all stages."""
    broker = KafkaBroker(domain_id="d1", broker_id="kafka-test")

    from src.pipeline.patterns import cqi_prediction_pipeline

    dag = cqi_prediction_pipeline()
    placement = broker._compute_placement(dag)

    assert set(placement.keys()) == set(dag.stages.keys())
    assert all(v == "kafka" for v in placement.values())


@pytest.mark.asyncio
async def test_kafka_broker_placement_all_pipeline_types():
    """KafkaBroker placement works for all registered pipeline types."""
    from src.broker.base import _PIPELINE_FACTORIES

    broker = KafkaBroker(domain_id="d1", broker_id="kafka-test")

    for pipeline_type in _PIPELINE_FACTORIES:
        dag = _PIPELINE_FACTORIES[pipeline_type]({})
        placement = broker._compute_placement(dag)
        assert all(v == "kafka" for v in placement.values()), (
            f"Pipeline type '{pipeline_type}' has non-kafka placement"
        )
