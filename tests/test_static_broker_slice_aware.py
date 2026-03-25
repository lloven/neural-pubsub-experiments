"""Slice-aware placement tests for StaticBroker (S1/S2).

Ensures that round-robin (S1) and random (S2) placement strategies respect
network slice boundaries, placing eMBB pipelines on eMBB workers and URLLC
pipelines on URLLC workers. This makes S1/S2 comparable to S3 (neural), which
already respects slices via the placement solver.

TDD: these tests are written BEFORE the implementation.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from src.broker.models import PipelineState, WorkerInfo
from src.broker.static_broker import PlacementStrategy, StaticBroker
from src.pipeline.patterns import (
    anomaly_detection_pipeline,
    cqi_prediction_pipeline,
    map_pipeline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_broker(
    placement: str = "round_robin",
    peer_urls: list[str] | None = None,
) -> StaticBroker:
    """Create a StaticBroker for testing."""
    return StaticBroker(
        domain_id="d1",
        broker_id="static-d1",
        placement=placement,
        peer_urls=peer_urls or [],
    )


async def _register_worker(
    broker: StaticBroker,
    node_id: str,
    slice_id: str,
    domain_id: str = "d1",
    capacity: float = 2.0,
) -> None:
    """Register a single worker with specified slice."""
    async with broker._workers_lock:
        broker._workers[node_id] = WorkerInfo(
            node_id=node_id,
            domain_id=domain_id,
            slice_id=slice_id,
            capacity=capacity,
            url=f"http://{node_id}:8081",
        )
    # Trigger cycle rebuild outside the lock (mimicking real registration flow)
    async with broker._workers_lock:
        broker._on_worker_change()


async def _register_sliced_workers(broker: StaticBroker) -> dict[str, list[str]]:
    """Register a realistic mix of sliced workers.

    Returns dict mapping slice_id to list of node_ids.
    """
    workers = {
        "URLLC": ["d1-urllc-1", "d1-urllc-2"],
        "eMBB": ["d1-embb-1", "d1-embb-2", "d1-embb-3"],
    }
    for slice_id, node_ids in workers.items():
        for nid in node_ids:
            await _register_worker(broker, nid, slice_id)
    return workers


# ===================================================================
# 1. Round-robin respects slice boundaries
# ===================================================================


class TestRoundRobinRespectsSlice:
    """S1 round-robin must cycle WITHIN each slice, not across all workers."""

    @pytest.mark.asyncio
    async def test_embb_pipeline_placed_on_embb_workers_only(self):
        """An eMBB pipeline (anomaly detection) must only be placed on eMBB workers."""
        broker = _make_broker(placement="round_robin")
        workers = await _register_sliced_workers(broker)

        dag = anomaly_detection_pipeline()  # all stages have slice_requirement="eMBB"
        placement = broker._compute_placement(dag)

        embb_workers = set(workers["eMBB"])
        for stage_id, node_id in placement.items():
            assert node_id in embb_workers, (
                f"Stage '{stage_id}' (eMBB) placed on '{node_id}' which is not an eMBB worker. "
                f"eMBB workers: {embb_workers}"
            )

    @pytest.mark.asyncio
    async def test_urllc_pipeline_placed_on_urllc_workers_only(self):
        """A URLLC pipeline (CQI prediction) must only be placed on URLLC workers."""
        broker = _make_broker(placement="round_robin")
        workers = await _register_sliced_workers(broker)

        dag = cqi_prediction_pipeline()  # all stages have slice_requirement="URLLC"
        placement = broker._compute_placement(dag)

        urllc_workers = set(workers["URLLC"])
        for stage_id, node_id in placement.items():
            assert node_id in urllc_workers, (
                f"Stage '{stage_id}' (URLLC) placed on '{node_id}' which is not a URLLC worker. "
                f"URLLC workers: {urllc_workers}"
            )

    @pytest.mark.asyncio
    async def test_round_robin_cycles_within_embb_slice(self):
        """Multiple eMBB pipelines should cycle across eMBB workers only,
        distributing stages evenly within the slice."""
        broker = _make_broker(placement="round_robin")
        workers = await _register_sliced_workers(broker)

        embb_workers = sorted(workers["eMBB"])
        placed_nodes = []

        # Place several single-stage eMBB pipelines to observe the cycle
        for _ in range(len(embb_workers) * 2):
            dag = map_pipeline("transform", n_stages=1, slice_requirement="eMBB")
            placement = broker._compute_placement(dag)
            placed_nodes.append(list(placement.values())[0])

        # All placed nodes must be eMBB workers
        for node_id in placed_nodes:
            assert node_id in set(embb_workers), (
                f"Node '{node_id}' is not an eMBB worker"
            )

        # Round-robin should hit every eMBB worker at least once
        assert set(placed_nodes) == set(embb_workers), (
            f"Round-robin did not cycle through all eMBB workers. "
            f"Placed on: {set(placed_nodes)}, expected: {set(embb_workers)}"
        )


# ===================================================================
# 2. Random respects slice boundaries
# ===================================================================


class TestRandomRespectsSlice:
    """S2 random must choose WITHIN each slice, not from all workers."""

    @pytest.mark.asyncio
    async def test_embb_pipeline_random_on_embb_workers_only(self):
        """Random placement of eMBB pipeline must only hit eMBB workers."""
        broker = _make_broker(placement="random")
        workers = await _register_sliced_workers(broker)

        embb_workers = set(workers["eMBB"])

        # Run many times to reduce chance of false pass
        for _ in range(20):
            dag = anomaly_detection_pipeline()
            placement = broker._compute_placement(dag)
            for stage_id, node_id in placement.items():
                assert node_id in embb_workers, (
                    f"Random placed eMBB stage '{stage_id}' on '{node_id}' "
                    f"which is not eMBB."
                )

    @pytest.mark.asyncio
    async def test_urllc_pipeline_random_on_urllc_workers_only(self):
        """Random placement of URLLC pipeline must only hit URLLC workers."""
        broker = _make_broker(placement="random")
        workers = await _register_sliced_workers(broker)

        urllc_workers = set(workers["URLLC"])

        for _ in range(20):
            dag = cqi_prediction_pipeline()
            placement = broker._compute_placement(dag)
            for stage_id, node_id in placement.items():
                assert node_id in urllc_workers, (
                    f"Random placed URLLC stage '{stage_id}' on '{node_id}' "
                    f"which is not URLLC."
                )


# ===================================================================
# 3. No slice requirement uses all workers
# ===================================================================


class TestNoSliceRequirement:
    """Stages without a slice requirement should use any available worker."""

    @pytest.mark.asyncio
    async def test_no_slice_requirement_uses_all_workers(self):
        """A pipeline with no slice requirement should be placeable on any worker."""
        broker = _make_broker(placement="round_robin")
        workers = await _register_sliced_workers(broker)
        all_workers = set(workers["URLLC"]) | set(workers["eMBB"])

        placed_nodes = set()
        # sensor_fusion has no slice_requirement on its stages
        from src.pipeline.patterns import sensor_fusion_pipeline
        for _ in range(20):
            dag = sensor_fusion_pipeline(n_sensors=1)
            placement = broker._compute_placement(dag)
            placed_nodes.update(placement.values())

        # Should eventually use workers from both slices
        assert placed_nodes & set(workers["URLLC"]), (
            "Pipeline with no slice requirement should use URLLC workers too"
        )
        assert placed_nodes & set(workers["eMBB"]), (
            "Pipeline with no slice requirement should use eMBB workers too"
        )


# ===================================================================
# 4. Flat workers appear in all slice cycles
# ===================================================================


class TestFlatWorkers:
    """Flat-topology workers (slice_id='flat') must appear in every slice cycle."""

    @pytest.mark.asyncio
    async def test_flat_workers_serve_embb_pipelines(self):
        """Flat workers should be eligible for eMBB pipelines."""
        broker = _make_broker(placement="round_robin")
        # Register ONLY flat workers (no eMBB-specific workers)
        await _register_worker(broker, "flat-1", "flat")
        await _register_worker(broker, "flat-2", "flat")

        dag = anomaly_detection_pipeline()  # eMBB slice requirement
        placement = broker._compute_placement(dag)

        # Should not raise; flat workers accept any slice
        for stage_id, node_id in placement.items():
            assert node_id in {"flat-1", "flat-2"}, (
                f"Stage '{stage_id}' not placed on a flat worker"
            )

    @pytest.mark.asyncio
    async def test_flat_workers_serve_urllc_pipelines(self):
        """Flat workers should be eligible for URLLC pipelines."""
        broker = _make_broker(placement="round_robin")
        await _register_worker(broker, "flat-1", "flat")

        dag = cqi_prediction_pipeline()  # URLLC slice requirement
        placement = broker._compute_placement(dag)

        for stage_id, node_id in placement.items():
            assert node_id == "flat-1"

    @pytest.mark.asyncio
    async def test_flat_workers_mixed_with_sliced(self):
        """When flat workers are mixed with sliced workers, flat workers
        should appear in the cycle for each slice."""
        broker = _make_broker(placement="round_robin")
        await _register_worker(broker, "embb-1", "eMBB")
        await _register_worker(broker, "flat-1", "flat")

        placed = set()
        for _ in range(10):
            dag = map_pipeline("t", n_stages=1, slice_requirement="eMBB")
            placement = broker._compute_placement(dag)
            placed.update(placement.values())

        # Both the eMBB and flat worker should be used
        assert "embb-1" in placed, "eMBB worker should be used for eMBB pipelines"
        assert "flat-1" in placed, "Flat worker should also be used for eMBB pipelines"


# ===================================================================
# 5. Cycle rebuilt on worker death
# ===================================================================


class TestCycleRebuiltOnWorkerDeath:
    """Per-slice cycles must be rebuilt when workers register/deregister."""

    @pytest.mark.asyncio
    async def test_dead_embb_worker_removed_from_embb_cycle(self):
        """After an eMBB worker is removed, it must not appear in placements."""
        broker = _make_broker(placement="round_robin")
        await _register_worker(broker, "embb-1", "eMBB")
        await _register_worker(broker, "embb-2", "eMBB")
        await _register_worker(broker, "urllc-1", "URLLC")

        # Remove embb-1
        async with broker._workers_lock:
            del broker._workers["embb-1"]
            broker._on_worker_change()

        # All subsequent eMBB placements must go to embb-2 only
        for _ in range(5):
            dag = map_pipeline("t", n_stages=1, slice_requirement="eMBB")
            placement = broker._compute_placement(dag)
            for node_id in placement.values():
                assert node_id == "embb-2", (
                    f"Dead worker 'embb-1' still appearing in eMBB cycle"
                )

    @pytest.mark.asyncio
    async def test_new_worker_added_to_cycle(self):
        """A newly registered worker should appear in subsequent placements."""
        broker = _make_broker(placement="round_robin")
        await _register_worker(broker, "embb-1", "eMBB")

        # Place a few pipelines (only embb-1 available)
        for _ in range(3):
            dag = map_pipeline("t", n_stages=1, slice_requirement="eMBB")
            placement = broker._compute_placement(dag)
            assert list(placement.values()) == ["embb-1"]

        # Register a second eMBB worker
        await _register_worker(broker, "embb-2", "eMBB")

        # Now placements should include both
        placed = set()
        for _ in range(10):
            dag = map_pipeline("t", n_stages=1, slice_requirement="eMBB")
            placement = broker._compute_placement(dag)
            placed.update(placement.values())

        assert "embb-2" in placed, "Newly registered worker not in cycle"


# ===================================================================
# 6. Empty slice forwards via federation
# ===================================================================


class TestEmptySliceForwarding:
    """When all workers in a slice are dead, placement should raise RuntimeError
    (no eligible workers) so the broker can forward via federation."""

    @pytest.mark.asyncio
    async def test_empty_embb_slice_raises(self):
        """If no eMBB (or flat) workers exist, placing an eMBB pipeline
        should raise RuntimeError to trigger federation forwarding."""
        broker = _make_broker(placement="round_robin")
        # Register only URLLC workers
        await _register_worker(broker, "urllc-1", "URLLC")
        await _register_worker(broker, "urllc-2", "URLLC")

        dag = anomaly_detection_pipeline()  # eMBB requirement

        with pytest.raises(RuntimeError, match="[Nn]o.*worker"):
            broker._compute_placement(dag)

    @pytest.mark.asyncio
    async def test_empty_urllc_slice_raises(self):
        """If no URLLC (or flat) workers exist, placing a URLLC pipeline
        should raise RuntimeError."""
        broker = _make_broker(placement="random")
        # Register only eMBB workers
        await _register_worker(broker, "embb-1", "eMBB")

        dag = cqi_prediction_pipeline()  # URLLC requirement

        with pytest.raises(RuntimeError, match="[Nn]o.*worker"):
            broker._compute_placement(dag)


# ===================================================================
# 7. Mixed pipeline with stages having different slice requirements
# ===================================================================


class TestMixedSlicePipeline:
    """A hypothetical pipeline with stages requiring different slices
    should place each stage on the correct slice's workers."""

    @pytest.mark.asyncio
    async def test_per_stage_slice_routing(self):
        """Each stage should be placed according to its own slice_requirement."""
        from src.pipeline.dag import Edge, PipelineDAG, Stage

        broker = _make_broker(placement="round_robin")
        await _register_worker(broker, "embb-1", "eMBB")
        await _register_worker(broker, "urllc-1", "URLLC")

        # Build a custom 2-stage pipeline: stage1=eMBB, stage2=URLLC
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "collect", 0.2, 5.0, slice_requirement="eMBB"))
        dag.add_stage(Stage("s2", "predict", 0.3, 1.0, slice_requirement="URLLC"))
        dag.add_edge(Edge("s1", "s2", latency_bound=10.0))

        placement = broker._compute_placement(dag)

        assert placement["s1"] == "embb-1", (
            f"eMBB stage placed on {placement['s1']}, expected embb-1"
        )
        assert placement["s2"] == "urllc-1", (
            f"URLLC stage placed on {placement['s2']}, expected urllc-1"
        )
