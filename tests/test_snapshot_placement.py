"""Tests for snapshot-and-release placement (lock contention fix).

Verifies that:
1. The publish handler releases _workers_lock before computing placement.
2. Concurrent publishes are not serialized on the lock.
3. Placement uses a topology snapshot, immune to concurrent changes.
4. Market mode uses a workers snapshot for clearing prices.
5. All 5 placement modes produce valid results via _dispatch_placement_on.
"""

import asyncio

import pytest

from src.broker.models import WorkerInfo
from src.broker.neural_broker import BrokerConfig, NeuralBroker
from src.broker.placement import find_placement as real_find_placement
from src.pipeline.dag import PipelineDAG, Stage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_broker(placement_mode: str = "neural", governance_enabled: bool = False):
    """Create a NeuralBroker with 2 registered workers and built topology."""
    config = BrokerConfig(
        domain_id="d1",
        broker_id="broker-d1-0",
        placement_mode=placement_mode,
        governance_enabled=governance_enabled,
        wan_cost_ms=5.0,
    )
    broker = NeuralBroker(config)
    for wid, did, bid in [("w1", "d1", 10.0), ("w2", "d2", 20.0)]:
        broker._workers[wid] = WorkerInfo(
            node_id=wid,
            domain_id=did,
            slice_id="flat",
            capacity=5.0,
            url=f"http://{wid}:8081",
            bid_cost_ms=bid,
        )
    broker._rebuild_topology()
    return broker


def _simple_dag() -> PipelineDAG:
    """A single-stage DAG that any placement mode can handle."""
    dag = PipelineDAG()
    dag.add_stage(Stage("s1", "predict", computational_demand=0.1, output_data_rate=1.0))
    return dag


# ---------------------------------------------------------------------------
# Test 1: Lock is NOT held during placement
# ---------------------------------------------------------------------------


class TestPublishReleasesLockBeforePlacement:
    """The publish handler must release _workers_lock before calling
    the placement algorithm.  We verify by patching find_placement
    to check the lock state from inside placement computation."""

    @pytest.mark.asyncio
    async def test_lock_not_held_during_find_placement(self):
        broker = _make_broker()
        lock_was_held_during_placement = None

        def checking_find_placement(*args, **kwargs):
            nonlocal lock_was_held_during_placement
            # Capture only the first call (the publish path).
            # Subsequent calls may come from failure-recovery paths
            # that legitimately hold the lock.
            if lock_was_held_during_placement is None:
                lock_was_held_during_placement = broker._workers_lock.locked()
            return real_find_placement(*args, **kwargs)

        # Patch the name imported into neural_broker module
        import src.broker.neural_broker as nb_mod

        original = nb_mod.find_placement
        nb_mod.find_placement = checking_find_placement
        try:
            app = broker.build_app()

            from httpx import ASGITransport, AsyncClient

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.post(
                    "/publish",
                    json={"pipeline_type": "cqi_prediction", "config": {}},
                )
            # The publish may return 200 or 500 (dispatch to fake workers fails),
            # but placement itself should have been called.
            assert lock_was_held_during_placement is not None, (
                "find_placement was never called"
            )
            assert lock_was_held_during_placement is False, (
                "_workers_lock was held during find_placement. "
                "The publish handler must release the lock before "
                "computing placement (snapshot-and-release)."
            )
        finally:
            nb_mod.find_placement = original


# ---------------------------------------------------------------------------
# Test 2: Lock hold time is independent of placement duration
# ---------------------------------------------------------------------------


class TestLockHoldTimeIndependentOfPlacement:
    """The lock must be held only for the snapshot, not during placement.
    We verify by measuring lock acquisition time while a slow placement
    is in progress.  In async Python with sync placement, the event loop
    is blocked during placement itself, so we test indirectly: the lock
    must be unlocked WHEN find_placement is entered."""

    @pytest.mark.asyncio
    async def test_lock_unlocked_across_placement_modes(self):
        """All non-market placement modes must not hold the lock."""
        for mode in ["neural", "locality", "latency", "spillover"]:
            broker = _make_broker(placement_mode=mode)
            lock_held = None

            # Patch the specific function each mode calls
            import src.broker.neural_broker as nb_mod

            originals = {}
            target_funcs = {
                "neural": "find_placement",
                "locality": "locality_placement",
                "latency": "latency_greedy_placement",
                "spillover": "spillover_placement",
            }
            target = target_funcs[mode]
            original = getattr(nb_mod, target)
            originals[target] = original

            def make_checker(orig_fn):
                def checker(*args, **kwargs):
                    nonlocal lock_held
                    # Capture only the first call (publish path).
                    if lock_held is None:
                        lock_held = broker._workers_lock.locked()
                    return orig_fn(*args, **kwargs)
                return checker

            setattr(nb_mod, target, make_checker(original))
            try:
                app = broker.build_app()
                from httpx import ASGITransport, AsyncClient

                async with AsyncClient(
                    transport=ASGITransport(app=app), base_url="http://test"
                ) as client:
                    await client.post(
                        "/publish",
                        json={"pipeline_type": "cqi_prediction", "config": {}},
                    )

                assert lock_held is False, (
                    f"Mode '{mode}': _workers_lock was held during "
                    f"{target}(). Must use snapshot-and-release."
                )
            finally:
                setattr(nb_mod, target, original)


# ---------------------------------------------------------------------------
# Test 3: Placement uses topology snapshot
# ---------------------------------------------------------------------------


class TestPlacementUsesTopologySnapshot:
    """After the fix, _dispatch_placement_on receives a topology parameter.
    It must use that parameter, not self._topology."""

    def test_dispatch_placement_on_uses_passed_topology(self):
        broker = _make_broker()
        dag = _simple_dag()

        # Create a different topology with only 1 node
        from src.broker.placement import ExecutionUnit, NetworkTopology

        alt_topo = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="alt-w1",
                    domain_id="d1",
                    slice_id="flat",
                    capacity=5.0,
                )
            ],
            latency_matrix={},
        )

        # _dispatch_placement_on should use alt_topo, not self._topology
        # Before the fix, this method doesn't exist → AttributeError
        placement = broker._dispatch_placement_on(
            dag, alt_topo, broker._governance, dict(broker._workers)
        )
        assert placement is not None
        # The placement should use the alt topology's node
        for node_id in placement.values():
            assert node_id == "alt-w1", (
                f"Placement used node '{node_id}' instead of 'alt-w1'. "
                f"_dispatch_placement_on must use the passed topology, "
                f"not self._topology."
            )


# ---------------------------------------------------------------------------
# Test 4: Market mode uses workers snapshot
# ---------------------------------------------------------------------------


class TestMarketModeUsesWorkersSnapshot:
    """Market clearing prices must be computed from the passed-in workers,
    not from self._workers."""

    def test_clearing_prices_from_passed_workers(self):
        broker = _make_broker(placement_mode="market")

        # Create a subset: only w1
        subset_workers = {
            "w1": broker._workers["w1"],
        }

        # _compute_clearing_prices_from should use the subset
        # Before the fix, this method doesn't exist → AttributeError
        prices = broker._compute_clearing_prices_from(subset_workers)

        assert prices is not None
        assert "d1" in prices, "Expected prices for domain d1"
        # Only d1 should have prices (w1 is in d1); d2 should be absent
        # because w2 (in d2) was excluded from the subset
        assert "d2" not in prices, (
            "Prices include d2, but only w1 (d1) was in the workers subset. "
            "_compute_clearing_prices_from must use the passed workers, "
            "not self._workers."
        )


# ---------------------------------------------------------------------------
# Test 5: All placement modes produce valid results
# ---------------------------------------------------------------------------


class TestAllPlacementModesWork:
    """Every placement mode must produce a valid placement via
    _dispatch_placement_on (snapshot-accepting method)."""

    @pytest.mark.parametrize(
        "mode",
        ["neural", "market", "locality", "latency", "spillover"],
    )
    def test_mode_produces_valid_placement(self, mode):
        broker = _make_broker(placement_mode=mode)
        dag = _simple_dag()

        topo = broker._topology
        gov = broker._governance
        workers = dict(broker._workers)

        # Before implementation, this raises AttributeError.
        placement = broker._dispatch_placement_on(dag, topo, gov, workers)

        assert placement is not None, f"Mode '{mode}' returned None"
        assert set(placement.keys()) == set(dag.stages.keys()), (
            f"Mode '{mode}': placement keys {set(placement.keys())} "
            f"don't match DAG stages {set(dag.stages.keys())}"
        )
        for stage_id, node_id in placement.items():
            assert node_id in ("w1", "w2"), (
                f"Mode '{mode}': stage '{stage_id}' placed on "
                f"unknown node '{node_id}'"
            )
