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


class TestDynamicBiddingDispatchIntegration:
    """End-to-end: dynamic bidding flows through the full broker dispatch
    path and enables cross-domain routing under heterogeneous load.

    Complements TestDynamicBidding (unit-level clearing price checks) by
    exercising BrokerConfig → _compute_clearing_prices_from →
    _dispatch_placement_on → market_mode_placement in one shot.
    """

    @staticmethod
    def _make_broker(dynamic_bidding: bool):
        from src.broker.neural_broker import BrokerConfig, NeuralBroker
        return NeuralBroker(BrokerConfig(
            domain_id="d1",
            broker_id="b1",
            placement_mode="market",
            market_load_aware=True,
            dynamic_bidding=dynamic_bidding,
            wan_cost_ms=50.0,
        ))

    @staticmethod
    def _make_heterogeneous_workers():
        from src.broker.models import WorkerInfo
        workers = {}
        for i in range(2):
            wid = f"d1-w{i}"
            workers[wid] = WorkerInfo(
                node_id=wid, domain_id="d1", slice_id="URLLC",
                capacity=1.0, url=f"http://{wid}:8081",
                current_load=0.4, bid_cost_ms=100.0,
            )
        for i in range(2):
            wid = f"d2-w{i}"
            workers[wid] = WorkerInfo(
                node_id=wid, domain_id="d2", slice_id="URLLC",
                capacity=1.0, url=f"http://{wid}:8081",
                current_load=0.13, bid_cost_ms=100.0,
            )
        return workers

    @staticmethod
    def _make_topology(workers):
        from src.broker.placement import ExecutionUnit, NetworkTopology
        nodes = [
            ExecutionUnit(w.node_id, w.domain_id, w.slice_id,
                          w.capacity, w.current_load)
            for w in workers.values()
        ]
        lat = {}
        node_list = list(workers.values())
        for i, a in enumerate(node_list):
            for j, b in enumerate(node_list):
                if j > i:
                    lat[(a.node_id, b.node_id)] = (
                        2.0 if a.domain_id == b.domain_id else 20.0
                    )
        return NetworkTopology(nodes=nodes, latency_matrix=lat)

    def test_dynamic_dispatch_routes_to_fast_domain(self):
        from src.broker.placement import GovernancePolicy
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(dynamic_bidding=True)
        workers = self._make_heterogeneous_workers()
        topo = self._make_topology(workers)
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        placement = broker._dispatch_placement_on(
            dag, topo, GovernancePolicy(), workers,
        )
        assert placement is not None
        assert placement["s1"].startswith("d2"), (
            f"Dynamic bidding should route from loaded d1 to cheaper d2, "
            f"got {placement['s1']}"
        )

    def test_static_dispatch_stays_local(self):
        from src.broker.placement import GovernancePolicy
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(dynamic_bidding=False)
        workers = self._make_heterogeneous_workers()
        topo = self._make_topology(workers)
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        placement = broker._dispatch_placement_on(
            dag, topo, GovernancePolicy(), workers,
        )
        assert placement is not None
        assert placement["s1"].startswith("d1"), (
            f"Static bidding should stay local (equal prices, WAN penalty), "
            f"got {placement['s1']}"
        )


class TestWorkerBidScalesWithSpeed:
    """Workers must scale bid_cost_ms by processing_speed at registration.

    A 2x slower worker (processing_speed=2.0) takes 2x longer per stage
    and should bid 2x the base cost. This feeds directly into clearing
    prices so the market can differentiate fast and slow domains even at
    low utilization where the M/M/1 congestion term is negligible.
    """

    def test_bid_scales_with_processing_speed(self):
        from src.worker.worker import WorkerConfig, Worker
        cfg = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            broker_url="http://localhost:8080",
            capacity=1.0,
            bid_cost_ms=100.0,
            processing_speed=2.0,
        )
        w = Worker(cfg)
        payload = w.registration_payload()
        assert payload["bid_cost_ms"] == 200.0, (
            f"bid should be 100 * 2.0 = 200, got {payload['bid_cost_ms']}"
        )

    def test_bid_at_default_speed_is_unchanged(self):
        from src.worker.worker import WorkerConfig, Worker
        cfg = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            broker_url="http://localhost:8080",
            capacity=1.0,
            bid_cost_ms=100.0,
            processing_speed=1.0,
        )
        w = Worker(cfg)
        payload = w.registration_payload()
        assert payload["bid_cost_ms"] == 100.0

    def test_fast_worker_bids_less(self):
        from src.worker.worker import WorkerConfig, Worker
        cfg = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            broker_url="http://localhost:8080",
            capacity=1.0,
            bid_cost_ms=100.0,
            processing_speed=0.67,
        )
        w = Worker(cfg)
        payload = w.registration_payload()
        assert abs(payload["bid_cost_ms"] - 67.0) < 1.0


class TestDynamicBidding:
    """Dynamic congestion pricing: clearing prices reflect worker utilization.

    The M/M/1 model cost = bid₀ / (1 - utilization) makes loaded workers
    bid higher, so their domain's clearing price increases and the market
    routes pipelines to cheaper (less-loaded) domains. This is the missing
    Walrasian price discovery mechanism identified by systematic-debugging
    of the heterogeneous ablation scenario.
    """

    @staticmethod
    def _make_broker(dynamic_bidding: bool):
        from src.broker.neural_broker import BrokerConfig, NeuralBroker
        return NeuralBroker(BrokerConfig(
            domain_id="d1",
            broker_id="b1",
            placement_mode="market",
            market_load_aware=True,
            dynamic_bidding=dynamic_bidding,
            wan_cost_ms=50.0,
        ))

    @staticmethod
    def _make_workers_two_domains(d1_load: float, d2_load: float):
        """Two domains, 2 workers each, with specified load levels."""
        from src.broker.models import WorkerInfo
        workers = {}
        for i in range(2):
            wid = f"d1-w{i}"
            workers[wid] = WorkerInfo(
                node_id=wid, domain_id="d1", slice_id="URLLC",
                capacity=1.0, url=f"http://{wid}:8081",
                current_load=d1_load, bid_cost_ms=100.0,
            )
        for i in range(2):
            wid = f"d2-w{i}"
            workers[wid] = WorkerInfo(
                node_id=wid, domain_id="d2", slice_id="URLLC",
                capacity=1.0, url=f"http://{wid}:8081",
                current_load=d2_load, bid_cost_ms=100.0,
            )
        return workers

    def test_static_bidding_default(self):
        """dynamic_bidding=False → clearing prices use raw bid_cost_ms."""
        broker = self._make_broker(dynamic_bidding=False)
        workers = self._make_workers_two_domains(d1_load=0.5, d2_load=0.1)
        prices = broker._compute_clearing_prices_from(workers)
        # With static bids, both domains have the same clearing price
        for st in prices.get("d1", {}):
            assert prices["d1"][st] == prices["d2"][st], (
                "Static bidding: prices should be equal regardless of load"
            )

    def test_dynamic_bidding_loaded_domain_more_expensive(self):
        """dynamic_bidding=True → loaded domain has higher clearing price."""
        broker = self._make_broker(dynamic_bidding=True)
        workers = self._make_workers_two_domains(d1_load=0.5, d2_load=0.1)
        prices = broker._compute_clearing_prices_from(workers)
        for st in prices.get("d1", {}):
            assert prices["d1"][st] > prices["d2"][st], (
                f"Dynamic bidding: d1 (load=0.5) should be more expensive "
                f"than d2 (load=0.1), got d1={prices['d1'][st]:.1f} "
                f"d2={prices['d2'][st]:.1f}"
            )

    def test_dynamic_bidding_unloaded_equals_static(self):
        """At zero load, dynamic bid equals static bid (M/M/1: 1/(1-0) = 1)."""
        broker = self._make_broker(dynamic_bidding=True)
        workers = self._make_workers_two_domains(d1_load=0.0, d2_load=0.0)
        prices = broker._compute_clearing_prices_from(workers)
        for st in prices.get("d1", {}):
            assert abs(prices["d1"][st] - 100.0) < 1.0, (
                f"Zero-load dynamic price should equal bid₀=100, got {prices['d1'][st]}"
            )

    def test_dynamic_bidding_high_load_diverges(self):
        """Near-capacity utilization → cost much higher than bid₀."""
        broker = self._make_broker(dynamic_bidding=True)
        workers = self._make_workers_two_domains(d1_load=0.95, d2_load=0.1)
        prices = broker._compute_clearing_prices_from(workers)
        for st in prices.get("d1", {}):
            # At util=0.95: cost = 100 / (1-0.95) = 2000
            assert prices["d1"][st] > 500, (
                f"High-load (0.95) price should diverge, got {prices['d1'][st]:.0f}"
            )

    def test_dynamic_bidding_enables_cross_domain_routing(self):
        """The heterogeneous scenario: slow domain (high load) should be
        more expensive than fast domain + WAN cost, enabling cross-domain
        trade in market_mode_placement.

        Numerical check from the plan:
          slow (util=0.4): 100/(1-0.4) = 167
          fast (util=0.13): 100/(1-0.13) = 115
          fast + WAN(50): 165 < 167 → trade happens
        """
        from src.broker.placement import (
            market_mode_placement, ExecutionUnit, NetworkTopology, GovernancePolicy,
        )
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(dynamic_bidding=True)
        workers = self._make_workers_two_domains(d1_load=0.4, d2_load=0.13)
        prices = broker._compute_clearing_prices_from(workers)

        # Build topology with 2 domains
        nodes = [
            ExecutionUnit(w.node_id, w.domain_id, w.slice_id,
                          w.capacity, w.current_load)
            for w in workers.values()
        ]
        lat = {}
        for i, a in enumerate(nodes):
            for j, b in enumerate(nodes):
                if j > i:
                    if a.domain_id == b.domain_id:
                        lat[(a.node_id, b.node_id)] = 2.0
                    else:
                        lat[(a.node_id, b.node_id)] = 20.0
        topo = NetworkTopology(nodes=nodes, latency_matrix=lat)

        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        # Place from d1 (the "slow" / loaded domain)
        placement = market_mode_placement(
            dag, topo, GovernancePolicy(), prices,
            wan_cost=50.0, local_domain="d1", load_aware=True,
        )
        assert placement is not None
        # With dynamic pricing, d1 is expensive (167) and d2+WAN (165) is cheaper
        # → market should route to d2
        assert placement["s1"].startswith("d2"), (
            f"Expected cross-domain routing to d2, got {placement['s1']} — "
            f"d1 price={prices['d1'].get('predict', '?')}, "
            f"d2 price={prices['d2'].get('predict', '?')}"
        )


class TestFederatedPriceExchange:
    """Federated price signal exchange: brokers share clearing prices so
    market_mode_placement can route cross-domain.

    Without federation, each broker only sees its local workers → clearing
    prices contain 1 domain → no cross-domain routing possible. This test
    class verifies that _peer_prices from federation are merged into the
    prices dict passed to market_mode_placement.
    """

    @staticmethod
    def _make_broker(domain_id: str = "d1", dynamic_bidding: bool = True):
        from src.broker.neural_broker import BrokerConfig, NeuralBroker
        return NeuralBroker(BrokerConfig(
            domain_id=domain_id,
            broker_id=f"b-{domain_id}",
            placement_mode="market",
            market_load_aware=True,
            dynamic_bidding=dynamic_bidding,
            wan_cost_ms=50.0,
        ))

    @staticmethod
    def _make_local_workers(domain_id: str, load: float, bid: float):
        from src.broker.models import WorkerInfo
        return {
            f"{domain_id}-w{i}": WorkerInfo(
                node_id=f"{domain_id}-w{i}",
                domain_id=domain_id,
                slice_id="URLLC",
                capacity=1.0,
                url=f"http://{domain_id}-w{i}:8081",
                current_load=load,
                bid_cost_ms=bid,
            )
            for i in range(2)
        }

    @staticmethod
    def _make_topology(workers):
        from src.broker.placement import ExecutionUnit, NetworkTopology
        nodes = [
            ExecutionUnit(w.node_id, w.domain_id, w.slice_id,
                          w.capacity, w.current_load)
            for w in workers.values()
        ]
        lat = {}
        node_list = list(workers.values())
        for i, a in enumerate(node_list):
            for j, b in enumerate(node_list):
                if j > i:
                    lat[(a.node_id, b.node_id)] = (
                        2.0 if a.domain_id == b.domain_id else 20.0
                    )
        return NetworkTopology(nodes=nodes, latency_matrix=lat)

    def test_peer_prices_cache_initially_empty(self):
        broker = self._make_broker()
        assert broker._peer_prices == {}

    def test_dispatch_merges_peer_prices(self):
        """When _peer_prices has a cheaper remote domain, market routes there."""
        from src.broker.placement import GovernancePolicy
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(domain_id="d1")
        # Only d1 workers registered (simulating federated single-domain broker)
        workers = self._make_local_workers("d1", load=0.0, bid=200.0)
        broker._workers = workers
        topo = self._make_topology(workers)

        # Simulate receiving d2's cheaper prices via federation
        broker._peer_prices["d2"] = {"predict": 67.0}

        # Need d2 workers in the topology for placement to find them
        from src.broker.placement import ExecutionUnit
        d2_nodes = [
            ExecutionUnit(f"d2-w{i}", "d2", "URLLC", 1.0, 0.0)
            for i in range(2)
        ]
        topo.nodes.extend(d2_nodes)
        for d2n in d2_nodes:
            for d1n in [n for n in topo.nodes if n.domain_id == "d1"]:
                topo.latency_matrix[(d1n.node_id, d2n.node_id)] = 20.0

        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        placement = broker._dispatch_placement_on(
            dag, topo, GovernancePolicy(), workers,
        )
        assert placement is not None
        # d1 price=200, d2 price=67+WAN(50)=117 < 200 → route to d2
        assert placement["s1"].startswith("d2"), (
            f"Expected cross-domain routing to d2 via federation prices, "
            f"got {placement['s1']}"
        )

    def test_dispatch_without_peer_prices_stays_local(self):
        """Without federation prices, single-domain broker stays local."""
        from src.broker.placement import GovernancePolicy
        from src.pipeline.dag import PipelineDAG, Stage

        broker = self._make_broker(domain_id="d1")
        workers = self._make_local_workers("d1", load=0.0, bid=200.0)
        broker._workers = workers
        topo = self._make_topology(workers)

        # No _peer_prices set → only d1 prices available
        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "predict", 0.1, 1.0))

        placement = broker._dispatch_placement_on(
            dag, topo, GovernancePolicy(), workers,
        )
        assert placement is not None
        assert placement["s1"].startswith("d1"), (
            f"Without peer prices, should stay local d1, got {placement['s1']}"
        )
