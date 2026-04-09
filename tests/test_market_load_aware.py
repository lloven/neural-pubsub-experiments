"""Tests for market_mode_placement load-awareness feature flag.

The legacy behaviour (load_aware=False) is preserved for the main
campaign's market runs, which were already collected with the buggy
first-feasible-worker selection. The ablation experiment sets
load_aware=True via BrokerConfig.market_load_aware.
"""

from src.broker.placement import (
    market_mode_placement,
    ExecutionUnit,
    NetworkTopology,
    GovernancePolicy,
)
from src.pipeline.dag import PipelineDAG, Stage


def _make_topo(n_workers: int = 4, domain: str = "d1") -> NetworkTopology:
    nodes = [
        ExecutionUnit(
            f"{domain}-w{i}", domain, "URLLC",
            capacity=1.0, current_load=0.0,
        )
        for i in range(n_workers)
    ]
    lat = {
        (a.node_id, b.node_id): 2.0
        for i, a in enumerate(nodes)
        for j, b in enumerate(nodes) if j > i
    }
    return NetworkTopology(nodes=nodes, latency_matrix=lat)


def _trivial_dag() -> PipelineDAG:
    dag = PipelineDAG()
    dag.add_stage(Stage("s1", "predict", 0.1, 1.0))
    return dag


def _trivial_prices(domain: str = "d1") -> dict:
    return {domain: {"predict": 100.0}}


class TestMarketLegacyBehavior:
    """Without load-awareness, market picks first feasible worker."""

    def test_legacy_picks_first_worker_when_all_unloaded(self):
        topo = _make_topo(4)
        placement = market_mode_placement(
            _trivial_dag(), topo, GovernancePolicy(),
            _trivial_prices(), wan_cost=50.0, local_domain="d1",
            load_aware=False,
        )
        assert placement["s1"] == "d1-w0"

    def test_legacy_skips_full_worker(self):
        topo = _make_topo(4)
        for n in topo.nodes:
            if n.node_id == "d1-w0":
                n.current_load = 1.0  # full
        placement = market_mode_placement(
            _trivial_dag(), topo, GovernancePolicy(),
            _trivial_prices(), wan_cost=50.0, local_domain="d1",
            load_aware=False,
        )
        # Legacy: picks d1-w1 (first worker WITH capacity)
        assert placement["s1"] == "d1-w1"


class TestMarketLoadAwareBehavior:
    """With load-aware flag, market picks least-loaded feasible worker."""

    def test_load_aware_avoids_loaded_worker(self):
        topo = _make_topo(4)
        for n in topo.nodes:
            if n.node_id == "d1-w0":
                n.current_load = 0.5  # partially loaded but still has capacity
        placement = market_mode_placement(
            _trivial_dag(), topo, GovernancePolicy(),
            _trivial_prices(), wan_cost=50.0, local_domain="d1",
            load_aware=True,
        )
        # Load-aware: should NOT pick d1-w0 since others are unloaded
        assert placement["s1"] != "d1-w0"
        assert placement["s1"] in {"d1-w1", "d1-w2", "d1-w3"}

    def test_load_aware_picks_minimum_loaded(self):
        topo = _make_topo(4)
        load_map = {
            "d1-w0": 0.5, "d1-w1": 0.3, "d1-w2": 0.1, "d1-w3": 0.4,
        }
        for n in topo.nodes:
            n.current_load = load_map[n.node_id]
        placement = market_mode_placement(
            _trivial_dag(), topo, GovernancePolicy(),
            _trivial_prices(), wan_cost=50.0, local_domain="d1",
            load_aware=True,
        )
        # d1-w2 has the lowest load
        assert placement["s1"] == "d1-w2"

    def test_load_aware_default_picks_first_when_tied(self):
        topo = _make_topo(4)
        # All loads = 0; min() returns the first
        placement = market_mode_placement(
            _trivial_dag(), topo, GovernancePolicy(),
            _trivial_prices(), wan_cost=50.0, local_domain="d1",
            load_aware=True,
        )
        assert placement["s1"] == "d1-w0"


class TestBrokerConfigPlumbing:
    """BrokerConfig.market_load_aware defaults to False and is honoured."""

    def test_default_is_false(self):
        from src.broker.neural_broker import BrokerConfig
        cfg = BrokerConfig(domain_id="d1", broker_id="b1")
        assert cfg.market_load_aware is False

    def test_can_be_enabled(self):
        from src.broker.neural_broker import BrokerConfig
        cfg = BrokerConfig(
            domain_id="d1", broker_id="b1", market_load_aware=True,
        )
        assert cfg.market_load_aware is True


class TestBrokerDispatchIntegration:
    """End-to-end: BrokerConfig.market_load_aware flows through
    NeuralBroker._dispatch_placement_on into market_mode_placement.

    This is the in-process integration smoke covering the full plumbing
    chain (config -> broker -> dispatch -> placement) without spinning up
    a Docker stack. It complements the targeted unit tests above by
    exercising the same code path the production broker takes on the
    publish hot path.
    """

    @staticmethod
    def _make_broker(load_aware: bool):
        from src.broker.neural_broker import BrokerConfig, NeuralBroker
        cfg = BrokerConfig(
            domain_id="d1",
            broker_id="b1",
            placement_mode="market",
            market_load_aware=load_aware,
            wan_cost_ms=50.0,
        )
        return NeuralBroker(cfg)

    @staticmethod
    def _make_workers(load_map: dict[str, float]):
        from src.broker.models import WorkerInfo
        return {
            wid: WorkerInfo(
                node_id=wid,
                domain_id="d1",
                slice_id="URLLC",
                capacity=1.0,
                url=f"http://{wid}:8081",
                current_load=load,
                bid_cost_ms=100.0,
            )
            for wid, load in load_map.items()
        }

    @staticmethod
    def _make_topology(workers):
        from src.broker.placement import ExecutionUnit, NetworkTopology
        nodes = [
            ExecutionUnit(
                node_id=w.node_id,
                domain_id=w.domain_id,
                slice_id=w.slice_id,
                capacity=w.capacity,
                current_load=w.current_load,
            )
            for w in workers.values()
        ]
        latency_matrix = {}
        node_list = list(workers.values())
        for i, a in enumerate(node_list):
            for j, b in enumerate(node_list):
                if i >= j:
                    continue
                latency_matrix[(a.node_id, b.node_id)] = 2.0
        return NetworkTopology(nodes=nodes, latency_matrix=latency_matrix)

    def test_dispatch_legacy_picks_first_feasible(self):
        from src.broker.placement import GovernancePolicy
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(load_aware=False)
        workers = self._make_workers({
            "d1-w0": 0.5, "d1-w1": 0.3, "d1-w2": 0.1, "d1-w3": 0.4,
        })
        topology = self._make_topology(workers)
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        placement = broker._dispatch_placement_on(
            dag, topology, GovernancePolicy(), workers,
        )
        # Legacy: first feasible worker in iteration order, regardless of load
        assert placement is not None
        assert placement["s1"] == "d1-w0"

    def test_dispatch_load_aware_picks_least_loaded(self):
        from src.broker.placement import GovernancePolicy
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(load_aware=True)
        workers = self._make_workers({
            "d1-w0": 0.5, "d1-w1": 0.3, "d1-w2": 0.1, "d1-w3": 0.4,
        })
        topology = self._make_topology(workers)
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        placement = broker._dispatch_placement_on(
            dag, topology, GovernancePolicy(), workers,
        )
        # Load-aware: d1-w2 has the lowest current_load (0.1)
        assert placement is not None
        assert placement["s1"] == "d1-w2"
