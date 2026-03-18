"""Unit tests for federation modules (summary, routing, integrator)."""

import numpy as np
import pytest

from src.federation.integrator import ExecutionUnit, compute_composite_capacity, register_pipeline_type
from src.federation.routing import (
    GovernanceConstraints,
    apply_governance_filter,
    route_locally,
)
from src.federation.summary import (
    ClusterSummary,
    SubscriptionSummary,
    deserialize,
    serialize,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unit_vec(dim: int, idx: int) -> np.ndarray:
    """Return a unit vector with 1.0 in position idx and 0s elsewhere."""
    v = np.zeros(dim, dtype=np.float32)
    v[idx] = 1.0
    return v


def _make_summary(domain_id: str, centroid: np.ndarray, radius: float = 0.1, capacity: float = 100.0) -> SubscriptionSummary:
    cluster = ClusterSummary(
        cluster_id="c0",
        centroid_embedding=centroid,
        radius=radius,
        available_capacity=capacity,
    )
    return SubscriptionSummary(domain_id=domain_id, clusters=[cluster], timestamp=1000.0)


# ---------------------------------------------------------------------------
# test_subscription_summary_serialize_roundtrip
# ---------------------------------------------------------------------------

def test_subscription_summary_serialize_roundtrip():
    centroid = _unit_vec(4, 0)
    original = _make_summary("domain-X", centroid, radius=0.15, capacity=200.0)

    data = serialize(original)
    assert isinstance(data, bytes)
    assert len(data) > 0

    recovered = deserialize(data)
    assert recovered.domain_id == original.domain_id
    assert abs(recovered.timestamp - original.timestamp) < 1e-6
    assert len(recovered.clusters) == 1
    c_orig = original.clusters[0]
    c_recv = recovered.clusters[0]
    assert c_recv.cluster_id == c_orig.cluster_id
    assert abs(c_recv.radius - c_orig.radius) < 1e-6
    assert abs(c_recv.available_capacity - c_orig.available_capacity) < 1e-6
    np.testing.assert_allclose(c_recv.centroid_embedding, c_orig.centroid_embedding, atol=1e-5)


# ---------------------------------------------------------------------------
# test_route_locally_match
# ---------------------------------------------------------------------------

def test_route_locally_match():
    # Publication embedding is very close to the cluster centroid
    centroid = _unit_vec(4, 0)
    summary = _make_summary("domain-A", centroid, radius=0.1)

    # Use the exact centroid as the publication embedding
    pub_emb = centroid.copy()
    result = route_locally(pub_emb, summary, threshold=0.2)

    assert result is not None
    assert result.matched is True
    assert result.forwarded is False
    assert result.target_domain == "domain-A"
    assert result.confidence > 0.9


# ---------------------------------------------------------------------------
# test_route_locally_no_match
# ---------------------------------------------------------------------------

def test_route_locally_no_match():
    # Publication embedding is orthogonal to the centroid (cosine distance = 1.0)
    centroid = _unit_vec(4, 0)
    summary = _make_summary("domain-A", centroid, radius=0.1)

    # Orthogonal vector: distance = 1.0
    pub_emb = _unit_vec(4, 1)
    result = route_locally(pub_emb, summary, threshold=0.2)

    assert result is None


# ---------------------------------------------------------------------------
# test_governance_filter_blocks
# ---------------------------------------------------------------------------

def test_governance_filter_blocks():
    # Data type is local-only: all candidates should be removed
    candidates = [
        ("domain-B", "c0", 0.1),
        ("domain-C", "c1", 0.2),
    ]
    gov = GovernanceConstraints(
        local_data_types={"medical"},
        trust_levels={"domain-B": 0.9, "domain-C": 0.8},
        min_trust=0.5,
    )
    filtered = apply_governance_filter(candidates, data_type="medical", governance=gov)
    assert filtered == []


# ---------------------------------------------------------------------------
# test_governance_filter_passes
# ---------------------------------------------------------------------------

def test_governance_filter_passes():
    # Non-local data type with sufficient trust: candidates should pass
    candidates = [
        ("domain-B", "c0", 0.05),
        ("domain-C", "c1", 0.15),
        ("domain-D", "c2", 0.10),
    ]
    gov = GovernanceConstraints(
        local_data_types={"medical"},
        trust_levels={"domain-B": 0.9, "domain-C": 0.7, "domain-D": 0.2},
        min_trust=0.5,
    )
    filtered = apply_governance_filter(candidates, data_type="telemetry", governance=gov)
    # domain-D has trust 0.2 < min_trust 0.5, so it should be filtered out
    domain_ids = [d for d, _, _ in filtered]
    assert "domain-B" in domain_ids
    assert "domain-C" in domain_ids
    assert "domain-D" not in domain_ids


# ---------------------------------------------------------------------------
# test_composite_capacity
# ---------------------------------------------------------------------------

def test_composite_capacity():
    # Register a simple 2-stage pipeline type for testing
    register_pipeline_type("test_pipe", ["stage_a", "stage_b"])

    # ExecutionUnit from placement uses (node_id, domain_id, slice_id, capacity, current_load).
    # The integrator's _node_supports_stage() returns True for all stages by default,
    # so composite capacity = min over stages of sum of residual capacities.
    nodes = [
        ExecutionUnit("u0", domain_id="d1", slice_id="s1", capacity=10.0, current_load=2.0),
        ExecutionUnit("u1", domain_id="d1", slice_id="s1", capacity=8.0, current_load=1.0),
        ExecutionUnit("u2", domain_id="d1", slice_id="s1", capacity=6.0, current_load=0.0),
    ]

    capacity = compute_composite_capacity(nodes, "test_pipe")

    # All nodes support all stages (homogeneous assumption):
    # stage_a available: u0=8 + u1=7 + u2=6 = 21
    # stage_b available: u0=8 + u1=7 + u2=6 = 21
    # composite = min(21, 21) = 21
    assert abs(capacity - 21.0) < 1e-6
