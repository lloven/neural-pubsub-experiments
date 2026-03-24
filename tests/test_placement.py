"""Unit tests for pipeline placement (src/broker/placement.py)."""

import pytest

from src.broker.placement import (
    ExecutionUnit,
    GovernancePolicy,
    NetworkTopology,
    check_feasibility,
    find_placement,
)
from src.pipeline.dag import Edge, PipelineDAG, Stage
from src.pipeline.patterns import cqi_prediction_pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_topology(nodes, latencies):
    """Helper to create a NetworkTopology."""
    return NetworkTopology(nodes=nodes, latency_matrix=latencies)


def _simple_stage(sid: str, demand: float = 0.3, slice_req=None, domain=None) -> Stage:
    return Stage(
        id=sid,
        stage_type="test",
        computational_demand=demand,
        output_data_rate=5.0,
        slice_requirement=slice_req,
        data_sovereignty_domain=domain,
    )


def _simple_node(
    nid: str,
    domain: str = "d1",
    sl: str = "eMBB",
    capacity: float = 1.0,
    load: float = 0.0,
) -> ExecutionUnit:
    return ExecutionUnit(
        node_id=nid,
        domain_id=domain,
        slice_id=sl,
        capacity=capacity,
        current_load=load,
    )


def _empty_governance() -> GovernancePolicy:
    return GovernancePolicy()


# ---------------------------------------------------------------------------
# test_single_stage_single_node
# ---------------------------------------------------------------------------

def test_single_stage_single_node():
    """Trivial case: one stage, one node, placement must be deterministic."""
    dag = PipelineDAG()
    dag.add_stage(_simple_stage("s1", demand=0.5))
    node = _simple_node("n1")
    topo = make_topology([node], {})
    gov = _empty_governance()

    placement = find_placement(dag, topo, gov)
    assert placement == {"s1": "n1"}


# ---------------------------------------------------------------------------
# test_greedy_respects_capacity
# ---------------------------------------------------------------------------

def test_greedy_respects_capacity():
    """Stage with demand 0.8 must skip a node whose residual capacity is only 0.4."""
    dag = PipelineDAG()
    dag.add_stage(_simple_stage("s1", demand=0.8))
    overloaded = _simple_node("n_full", capacity=1.0, load=0.6)
    sufficient = _simple_node("n_ok", capacity=1.0, load=0.0)
    # Diamond latency matrix not needed for single stage
    topo = make_topology([overloaded, sufficient], {("n_full", "n_ok"): 1.0})
    gov = _empty_governance()

    placement = find_placement(dag, topo, gov)
    assert placement["s1"] == "n_ok"


# ---------------------------------------------------------------------------
# test_greedy_respects_slice
# ---------------------------------------------------------------------------

def test_greedy_respects_slice():
    """Stage requiring URLLC must not be placed on an eMBB-only node."""
    dag = PipelineDAG()
    dag.add_stage(_simple_stage("s1", demand=0.3, slice_req="URLLC"))
    wrong_slice = _simple_node("n_embb", sl="eMBB")
    right_slice = _simple_node("n_urllc", sl="URLLC")
    topo = make_topology([wrong_slice, right_slice], {("n_embb", "n_urllc"): 1.0})
    gov = _empty_governance()

    placement = find_placement(dag, topo, gov)
    assert placement["s1"] == "n_urllc"


# ---------------------------------------------------------------------------
# test_greedy_respects_sovereignty
# ---------------------------------------------------------------------------

def test_greedy_respects_sovereignty():
    """Stage with data_sovereignty_domain must be placed in the matching domain."""
    dag = PipelineDAG()
    dag.add_stage(
        Stage(
            id="collect",
            stage_type="collect",
            computational_demand=0.1,
            output_data_rate=10.0,
            data_sovereignty_domain="radio_local",
        )
    )
    foreign = _simple_node("n_foreign", domain="other_domain")
    local = _simple_node("n_local", domain="radio_local")
    topo = make_topology([foreign, local], {("n_foreign", "n_local"): 1.0})
    gov = _empty_governance()

    placement = find_placement(dag, topo, gov)
    assert placement["collect"] == "n_local"


# ---------------------------------------------------------------------------
# test_dp_placement_tree
# ---------------------------------------------------------------------------

def test_dp_placement_tree():
    """DP placement of CQI pipeline (tree DAG) on 3 URLLC nodes respects sovereignty and slice."""
    # The 'collect' stage uses __local__ sovereignty domain, which the broker
    # resolves to the actual domain_id at runtime. In this unit test we
    # simulate that resolution by setting the sovereignty to "edge_local"
    # and matching the node domain.
    dag = cqi_prediction_pipeline()
    # Resolve __local__ to "edge_local" (simulating what the broker does)
    for stage in dag.stages.values():
        if stage.data_sovereignty_domain == "__local__":
            stage.data_sovereignty_domain = "edge_local"

    nodes = [
        ExecutionUnit("n0", domain_id="edge_local", slice_id="URLLC", capacity=1.0),
        ExecutionUnit("n1", domain_id="cloud", slice_id="URLLC", capacity=1.0),
        ExecutionUnit("n2", domain_id="cloud", slice_id="URLLC", capacity=1.0),
    ]
    latencies = {
        ("n0", "n1"): 1.0,
        ("n0", "n2"): 1.0,
        ("n1", "n2"): 0.5,
    }
    topo = make_topology(nodes, latencies)
    gov = GovernancePolicy(
        trust_levels={("edge_local", "cloud"): 1.0, ("cloud", "edge_local"): 1.0}
    )

    placement = find_placement(dag, topo, gov)
    # All three stages must be placed
    assert set(placement.keys()) == {"collect", "feature_extract", "predict"}
    # 'collect' must be on the edge_local node (sovereignty constraint)
    assert placement["collect"] == "n0"
    # 'feature_extract' and 'predict' must be on URLLC nodes
    for stage_id in ("feature_extract", "predict"):
        assigned_node = next(n for n in nodes if n.node_id == placement[stage_id])
        assert assigned_node.slice_id == "URLLC"


# ---------------------------------------------------------------------------
# test_feasibility_check_pass
# ---------------------------------------------------------------------------

def test_feasibility_check_pass():
    """A valid placement on a single node with sufficient capacity reports no violations."""
    dag = PipelineDAG()
    dag.add_stage(_simple_stage("s1", demand=0.3))
    dag.add_stage(_simple_stage("s2", demand=0.3))
    dag.add_edge(Edge("s1", "s2", latency_bound=10.0))

    node = _simple_node("n1", capacity=1.0)
    topo = make_topology([node], {})
    gov = _empty_governance()

    placement = {"s1": "n1", "s2": "n1"}
    feasible, violations = check_feasibility(placement, dag, topo, gov)
    assert feasible is True
    assert violations == []


# ---------------------------------------------------------------------------
# test_feasibility_check_violations
# ---------------------------------------------------------------------------

def test_feasibility_check_violations():
    """Placement with slice mismatch and cross-node latency exceeding the edge bound is infeasible."""
    dag = PipelineDAG()
    dag.add_stage(Stage("s1", "ingest", computational_demand=0.9, output_data_rate=5.0, slice_requirement="URLLC"))
    dag.add_stage(Stage("s2", "process", computational_demand=0.9, output_data_rate=2.0))
    dag.add_edge(Edge("s1", "s2", latency_bound=1.0))  # tight latency bound

    # Two nodes: n1 has wrong slice, capacity 1.0; n2 has right slice, capacity 1.0
    n1 = ExecutionUnit("n1", domain_id="d1", slice_id="eMBB", capacity=1.0)
    n2 = ExecutionUnit("n2", domain_id="d1", slice_id="eMBB", capacity=1.0)
    # High latency between nodes violates the edge bound of 1.0 ms
    topo = make_topology([n1, n2], {("n1", "n2"): 50.0})
    gov = _empty_governance()

    # Placement: s1 on n1 (wrong slice), s2 on n2 (latency violation + capacity)
    placement = {"s1": "n1", "s2": "n2"}
    feasible, violations = check_feasibility(placement, dag, topo, gov)
    assert feasible is False
    # Should have at least one violation (slice mismatch and/or latency)
    assert len(violations) >= 1
    # Slice violation for s1
    assert any("Slice" in v and "s1" in v for v in violations)
    # Latency violation
    assert any("Latency" in v for v in violations)


# ---------------------------------------------------------------------------
# test_cross_domain_trust
# ---------------------------------------------------------------------------

def test_cross_domain_trust():
    """Cross-domain edge without a trust entry in GovernancePolicy must be flagged infeasible."""
    dag = PipelineDAG()
    dag.add_stage(_simple_stage("s1", demand=0.2))
    dag.add_stage(_simple_stage("s2", demand=0.2))
    dag.add_edge(Edge("s1", "s2", latency_bound=10.0))

    n1 = _simple_node("n1", domain="domain_A")
    n2 = _simple_node("n2", domain="domain_B")
    topo = make_topology([n1, n2], {("n1", "n2"): 2.0})

    # No trust between domain_A and domain_B
    gov = GovernancePolicy(trust_levels={})

    placement = {"s1": "n1", "s2": "n2"}
    feasible, violations = check_feasibility(placement, dag, topo, gov)
    assert feasible is False
    assert any("trust" in v.lower() or "Governance" in v for v in violations)


# ===========================================================================
# Wildcard slice matching for flat topology (Phase B bug fix)
# ===========================================================================


class TestFlatSliceWildcard:
    """Flat workers (slice_id='flat') must accept ANY slice requirement.

    In B1/B1eq configurations, all workers register with slice_id='flat'.
    Pipeline templates define hard slice requirements (URLLC for CQI, eMBB
    for anomaly). Without wildcard matching, 80% of pipelines are rejected
    with HTTP 503 because no worker has the required slice.
    """

    def test_flat_worker_accepts_urllc_stage(self):
        """A flat worker must accept a stage requiring URLLC slice."""
        dag = PipelineDAG()
        dag.add_stage(_simple_stage("s1", demand=0.3, slice_req="URLLC"))
        flat_node = _simple_node("n_flat", sl="flat")
        topo = make_topology([flat_node], {})
        gov = _empty_governance()

        placement = find_placement(dag, topo, gov)
        assert placement == {"s1": "n_flat"}

    def test_flat_worker_accepts_embb_stage(self):
        """A flat worker must accept a stage requiring eMBB slice."""
        dag = PipelineDAG()
        dag.add_stage(_simple_stage("s1", demand=0.3, slice_req="eMBB"))
        flat_node = _simple_node("n_flat", sl="flat")
        topo = make_topology([flat_node], {})
        gov = _empty_governance()

        placement = find_placement(dag, topo, gov)
        assert placement == {"s1": "n_flat"}

    def test_flat_worker_accepts_full_cqi_pipeline(self):
        """Flat workers must accept an entire CQI prediction pipeline (URLLC requirement)."""
        dag = cqi_prediction_pipeline()
        # Resolve __local__ sovereignty to match our flat node domain
        for stage in dag.stages.values():
            if stage.data_sovereignty_domain == "__local__":
                stage.data_sovereignty_domain = "d1"

        flat_nodes = [
            _simple_node(f"n_flat_{i}", sl="flat", domain="d1", capacity=2.0)
            for i in range(3)
        ]
        latencies = {
            ("n_flat_0", "n_flat_1"): 1.0,
            ("n_flat_0", "n_flat_2"): 1.0,
            ("n_flat_1", "n_flat_2"): 0.5,
        }
        topo = make_topology(flat_nodes, latencies)
        gov = _empty_governance()

        placement = find_placement(dag, topo, gov)
        assert set(placement.keys()) == {"collect", "feature_extract", "predict"}

    def test_flat_worker_accepts_full_anomaly_pipeline(self):
        """Flat workers must accept an entire anomaly detection pipeline (eMBB requirement)."""
        from src.pipeline.patterns import anomaly_detection_pipeline

        dag = anomaly_detection_pipeline()
        flat_nodes = [
            _simple_node(f"n_flat_{i}", sl="flat", capacity=2.0)
            for i in range(3)
        ]
        latencies = {
            ("n_flat_0", "n_flat_1"): 1.0,
            ("n_flat_0", "n_flat_2"): 1.0,
            ("n_flat_1", "n_flat_2"): 0.5,
        }
        topo = make_topology(flat_nodes, latencies)
        gov = _empty_governance()

        placement = find_placement(dag, topo, gov)
        assert set(placement.keys()) == {"collect", "feature_extract", "detect"}

    def test_sliced_worker_still_enforces_strict_matching(self):
        """Regression: non-flat workers must still enforce strict slice matching."""
        dag = PipelineDAG()
        dag.add_stage(_simple_stage("s1", demand=0.3, slice_req="URLLC"))
        embb_node = _simple_node("n_embb", sl="eMBB")
        # Only an eMBB node available; URLLC stage must fail
        topo = make_topology([embb_node], {})
        gov = _empty_governance()

        with pytest.raises(RuntimeError, match="No feasible"):
            find_placement(dag, topo, gov)

    def test_check_feasibility_flat_worker_no_slice_violation(self):
        """check_feasibility must not report a slice violation when a flat worker hosts a sliced stage."""
        dag = PipelineDAG()
        dag.add_stage(_simple_stage("s1", demand=0.3, slice_req="URLLC"))
        flat_node = _simple_node("n_flat", sl="flat")
        topo = make_topology([flat_node], {})
        gov = _empty_governance()

        placement = {"s1": "n_flat"}
        feasible, violations = check_feasibility(placement, dag, topo, gov)
        assert feasible is True
        assert not any("Slice" in v for v in violations)

    def test_check_feasibility_sliced_mismatch_still_violations(self):
        """Regression: check_feasibility must still flag non-flat slice mismatches."""
        dag = PipelineDAG()
        dag.add_stage(_simple_stage("s1", demand=0.3, slice_req="URLLC"))
        embb_node = _simple_node("n_embb", sl="eMBB")
        topo = make_topology([embb_node], {})
        gov = _empty_governance()

        placement = {"s1": "n_embb"}
        feasible, violations = check_feasibility(placement, dag, topo, gov)
        assert feasible is False
        assert any("Slice" in v for v in violations)
