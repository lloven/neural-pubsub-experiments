"""TDD tests for ShardedOracleBroker.

Tests are written FIRST and watched to FAIL before implementation
(per Tools/superpowers/skills/test-driven-development).

The ShardedOracleBroker is a 4-broker centralised orchestrator with a
designated leader. The leader (env var IS_COORDINATOR=true) pulls peer
state via HTTP, merges with local state, runs the existing global
placement solver, and dispatches across all 4 brokers. State-owners
(IS_COORDINATOR unset/false) expose a /sharded-oracle/state endpoint
returning their local worker registry snapshot.

This is the fair process-count comparator for F1: same broker count
(4) as market-quad, but global-optimum decision making like
oracle-global.
"""
from __future__ import annotations

import os

import pytest

from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
)


# --------------------------------------------------------------------------
# Pure-function tests: state merging
# --------------------------------------------------------------------------


class TestStateMerging:
    """The coordinator's core operation: merge local + peer state into a
    single NetworkTopology for the global solver."""

    def test_merge_two_disjoint_topologies(self):
        """Two single-node topologies merge into a 2-node topology."""
        from src.broker.sharded_oracle_broker import merge_topologies

        local = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d1-w1", domain_id="d1", slice_id="urllc",
                    capacity=10.0,
                ),
            ],
            latency_matrix={},
        )
        peer = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d2-w1", domain_id="d2", slice_id="embb",
                    capacity=10.0,
                ),
            ],
            latency_matrix={},
        )
        cross_lat = {("d1-w1", "d2-w1"): 50.0}
        merged = merge_topologies([local, peer], cross_lat)

        assert len(merged.nodes) == 2
        assert {n.node_id for n in merged.nodes} == {"d1-w1", "d2-w1"}
        assert merged.latency(  # cross-domain latency injected
            "d1-w1", "d2-w1") == 50.0

    def test_merge_preserves_intra_topology_latencies(self):
        """Within-topology latencies are preserved through the merge."""
        from src.broker.sharded_oracle_broker import merge_topologies

        local = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d1-w1", domain_id="d1", slice_id="urllc",
                    capacity=10.0,
                ),
                ExecutionUnit(
                    node_id="d1-w2", domain_id="d1", slice_id="urllc",
                    capacity=10.0,
                ),
            ],
            latency_matrix={("d1-w1", "d1-w2"): 0.5},
        )
        peer = NetworkTopology(nodes=[], latency_matrix={})
        merged = merge_topologies([local, peer], {})

        assert merged.latency("d1-w1", "d1-w2") == 0.5

    def test_merge_rejects_node_id_collision(self):
        """Two topologies sharing a node_id raise ValueError."""
        from src.broker.sharded_oracle_broker import merge_topologies

        a = NetworkTopology(
            nodes=[ExecutionUnit(
                node_id="x", domain_id="d1", slice_id="urllc", capacity=1.0,
            )],
            latency_matrix={},
        )
        b = NetworkTopology(
            nodes=[ExecutionUnit(
                node_id="x", domain_id="d2", slice_id="embb", capacity=1.0,
            )],
            latency_matrix={},
        )
        with pytest.raises(ValueError, match="node_id collision"):
            merge_topologies([a, b], {})

    def test_merge_preserves_current_load(self):
        """current_load (queue depth) is preserved through merging — the
        coordinator must see live load, not just static capacity."""
        from src.broker.sharded_oracle_broker import merge_topologies

        local = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d1-w1", domain_id="d1", slice_id="urllc",
                    capacity=10.0, current_load=7.0,
                ),
            ],
            latency_matrix={},
        )
        peer = NetworkTopology(nodes=[], latency_matrix={})
        merged = merge_topologies([local, peer], {})

        assert merged.get_node("d1-w1").current_load == 7.0


# --------------------------------------------------------------------------
# Pure-function tests: governance merging
# --------------------------------------------------------------------------


class TestGovernanceMerging:
    """Governance constraints from each shard merge into a single policy."""

    def test_merge_governance_unions_local_stage_types(self):
        from src.broker.sharded_oracle_broker import merge_governance

        gov_a = GovernancePolicy(
            local_stage_types={"raw_cqi"},
            trust_levels={("d1", "d2"): 0.9},
        )
        gov_b = GovernancePolicy(
            local_stage_types={"alerting"},
            trust_levels={("d2", "d3"): 0.8},
        )
        merged = merge_governance([gov_a, gov_b])

        assert merged.local_stage_types == {"raw_cqi", "alerting"}
        assert merged.get_trust("d1", "d2") == 0.9
        assert merged.get_trust("d2", "d3") == 0.8

    def test_merge_governance_intersects_trust_when_conflicting(self):
        """If two shards report conflicting trust for the same pair, the
        coordinator takes the MIN (most conservative). This is a safety
        property — partial enforcement at one shard should not loosen
        the global policy."""
        from src.broker.sharded_oracle_broker import merge_governance

        gov_a = GovernancePolicy(trust_levels={("d1", "d2"): 0.9})
        gov_b = GovernancePolicy(trust_levels={("d1", "d2"): 0.3})
        merged = merge_governance([gov_a, gov_b])

        assert merged.get_trust("d1", "d2") == 0.3


# --------------------------------------------------------------------------
# Coordinator role tests
# --------------------------------------------------------------------------


class TestCoordinatorRole:
    """The IS_COORDINATOR env var selects coordinator vs state-owner."""

    def test_is_coordinator_when_env_var_true(self, monkeypatch):
        from src.broker.sharded_oracle_broker import is_coordinator_role

        monkeypatch.setenv("IS_COORDINATOR", "true")
        assert is_coordinator_role() is True

    def test_is_state_owner_when_env_var_unset(self, monkeypatch):
        from src.broker.sharded_oracle_broker import is_coordinator_role

        monkeypatch.delenv("IS_COORDINATOR", raising=False)
        assert is_coordinator_role() is False

    def test_is_state_owner_when_env_var_false(self, monkeypatch):
        from src.broker.sharded_oracle_broker import is_coordinator_role

        monkeypatch.setenv("IS_COORDINATOR", "false")
        assert is_coordinator_role() is False

    def test_is_coordinator_case_insensitive(self, monkeypatch):
        from src.broker.sharded_oracle_broker import is_coordinator_role

        monkeypatch.setenv("IS_COORDINATOR", "TRUE")
        assert is_coordinator_role() is True
        monkeypatch.setenv("IS_COORDINATOR", "True")
        assert is_coordinator_role() is True


# --------------------------------------------------------------------------
# State serialization (over the wire)
# --------------------------------------------------------------------------


class TestStateSerialization:
    """The /sharded-oracle/state endpoint must round-trip the state
    snapshot via JSON without loss of capacity / load / domain / slice."""

    def test_snapshot_topology_to_dict(self):
        from src.broker.sharded_oracle_broker import topology_to_snapshot

        topo = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d1-w1", domain_id="d1", slice_id="urllc",
                    capacity=10.0, current_load=3.0,
                ),
            ],
            latency_matrix={("d1-w1", "d1-w1"): 0.0},
        )
        snap = topology_to_snapshot(topo)

        assert snap["nodes"][0]["node_id"] == "d1-w1"
        assert snap["nodes"][0]["domain_id"] == "d1"
        assert snap["nodes"][0]["capacity"] == 10.0
        assert snap["nodes"][0]["current_load"] == 3.0

    def test_snapshot_round_trips(self):
        """Coordinator deserialises peer snapshot back to NetworkTopology."""
        from src.broker.sharded_oracle_broker import (
            snapshot_to_topology,
            topology_to_snapshot,
        )

        original = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d1-w1", domain_id="d1", slice_id="urllc",
                    capacity=10.0, current_load=3.0,
                ),
            ],
            latency_matrix={},
        )
        snap = topology_to_snapshot(original)
        restored = snapshot_to_topology(snap)

        assert len(restored.nodes) == 1
        n = restored.nodes[0]
        assert n.node_id == "d1-w1"
        assert n.domain_id == "d1"
        assert n.slice_id == "urllc"
        assert n.capacity == 10.0
        assert n.current_load == 3.0


# --------------------------------------------------------------------------
# End-to-end: coordinator pulls, merges, decides
# --------------------------------------------------------------------------


class TestCoordinatorDecide:
    """The coordinator's full decide() flow with mocked peer state."""

    def test_decide_uses_merged_topology_for_global_placement(self):
        """Given local 1 worker + 3 mocked peer state-owners (1 worker
        each), the coordinator must call find_placement against the
        4-worker merged topology, not just its 1 local worker."""
        from src.pipeline.dag import Edge, PipelineDAG, Stage
        from src.broker.sharded_oracle_broker import decide_globally

        local = NetworkTopology(
            nodes=[
                ExecutionUnit(
                    node_id="d1-w1", domain_id="d1", slice_id="urllc",
                    capacity=100.0,
                ),
            ],
            latency_matrix={},
        )
        peers = [
            NetworkTopology(
                nodes=[ExecutionUnit(
                    node_id=f"d{d}-w1", domain_id=f"d{d}", slice_id="embb",
                    capacity=100.0,
                )],
                latency_matrix={},
            )
            for d in (2, 3, 4)
        ]
        all_nodes = ["d1-w1", "d2-w1", "d3-w1", "d4-w1"]
        cross_lat = {
            (a, b): 50.0 for a in all_nodes for b in all_nodes if a != b
        }
        gov = GovernancePolicy()

        # 4-stage chain (tree): trust eMBB-or-URLLC on every stage
        dag = PipelineDAG()
        for i in range(4):
            dag.add_stage(Stage(
                id=f"s{i}",
                stage_type=f"type{i}",
                computational_demand=1.0,
                output_data_rate=1.0,
            ))
        for i in range(3):
            dag.add_edge(Edge(
                source_id=f"s{i}",
                target_id=f"s{i+1}",
                latency_bound=1000.0,
            ))

        placement = decide_globally(
            dag=dag,
            local_topology=local,
            peer_topologies=peers,
            cross_latencies=cross_lat,
            governance=gov,
        )

        # All 4 stages get assigned — coordinator can use ALL 4 workers,
        # not just its 1 local worker.
        assert len(placement) == 4
        assigned_nodes = set(placement.values())
        assert assigned_nodes.issubset(set(all_nodes))
