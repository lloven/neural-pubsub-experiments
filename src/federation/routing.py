"""Cross-domain routing protocol for federated Neural Pub/Sub (paper Section 4.2.3).

Implements the 5-step federated routing protocol:

  1. **Local match** -- check if the publication embedding matches any local
     subscription cluster within the cosine distance threshold.
  2. **Federation candidate selection** -- scan peer subscription summaries
     for clusters whose centroid is within (radius + threshold) cosine
     distance of the publication embedding.
  3. **Governance filter** -- remove candidates that violate data sovereignty
     or trust constraints (Section 4.3.1).
  4. **Rank and select** -- pick the best remaining candidate by distance.
  5. **Forward** -- route the publication to the selected remote domain.

All distance computations use cosine distance (1 - cosine_similarity) to
match the Neural Router's internal metric.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from .summary import SubscriptionSummary


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RoutingResult:
    """Outcome of the federated routing protocol.

    Attributes:
        matched: True if a matching cluster was found (local or remote).
        target_domain: Domain id of the matched cluster, or None.
        target_cluster: Cluster id of the matched cluster, or None.
        confidence: 1 - cosine_distance to the matched cluster centroid.
            Higher means more confident. 0.0 if no match.
        forwarded: True if the match was found in a remote domain
            (federation route), False if local.
        governance_filtered: Number of federation candidates removed by
            the governance filter (step 3).
    """

    matched: bool
    target_domain: str | None
    target_cluster: str | None
    confidence: float
    forwarded: bool
    governance_filtered: int


@dataclass
class GovernanceConstraints:
    """Data sovereignty and trust constraints for cross-domain routing.

    Implements the governance layer described in Section 4.3.1. Each domain
    maintains a set of constraints that restrict which publications may be
    forwarded to which peers.

    Attributes:
        local_data_types: Data types (e.g., "medical", "financial") that
            must not leave this domain regardless of match quality.
        trust_levels: Mapping from peer domain_id to a trust score in
            [0, 1]. Domains not in this dict are implicitly untrusted (0).
        min_trust: Minimum trust level required for cross-domain routing.
            Candidates from peers below this threshold are filtered out.
    """

    local_data_types: set[str] = field(default_factory=set)
    trust_levels: dict[str, float] = field(default_factory=dict)
    min_trust: float = 0.5


# ---------------------------------------------------------------------------
# Helper: cosine distance between a vector and a set of cluster centroids
# ---------------------------------------------------------------------------

def _cosine_distance(vec: np.ndarray, centroids: np.ndarray) -> np.ndarray:
    """Compute cosine distance (1 - cosine_similarity) from *vec* to each row of *centroids*.

    Args:
        vec: 1-D embedding vector.
        centroids: 2-D array of shape (n, dim).

    Returns:
        1-D array of cosine distances, shape (n,).
    """
    sims = cosine_similarity(vec.reshape(1, -1), centroids).flatten()
    return 1.0 - sims


# ---------------------------------------------------------------------------
# Step 1: Local routing
# ---------------------------------------------------------------------------

def route_locally(
    publication_embedding: np.ndarray,
    local_summary: SubscriptionSummary,
    threshold: float,
) -> RoutingResult | None:
    """Check if the publication matches any local cluster (protocol step 1).

    Scans all clusters in the local summary and returns the best match whose
    cosine distance is within ``threshold``.

    Args:
        publication_embedding: L2-normalised embedding of the incoming
            publication/event.
        local_summary: The domain's own subscription summary.
        threshold: Maximum cosine distance for a match (analogous to tau
            in the Neural Router, Section 3.3).

    Returns:
        A ``RoutingResult`` with ``forwarded=False`` if a local match is
        found, or ``None`` if no local cluster matches.
    """
    if not local_summary.clusters:
        return None

    centroids = np.array([c.centroid_embedding for c in local_summary.clusters])
    distances = _cosine_distance(publication_embedding, centroids)

    best_idx = int(np.argmin(distances))
    best_dist = float(distances[best_idx])

    if best_dist <= threshold:
        return RoutingResult(
            matched=True,
            target_domain=local_summary.domain_id,
            target_cluster=local_summary.clusters[best_idx].cluster_id,
            confidence=1.0 - best_dist,
            forwarded=False,
            governance_filtered=0,
        )
    return None


# ---------------------------------------------------------------------------
# Step 2: Federation candidate selection
# ---------------------------------------------------------------------------

def select_federation_candidates(
    publication_embedding: np.ndarray,
    peer_summaries: dict[str, SubscriptionSummary],
    threshold: float,
) -> list[tuple[str, str, float]]:
    """Identify federation candidates from peer summaries (protocol step 2).

    A remote cluster is a candidate if the cosine distance from the
    publication embedding to the cluster centroid is less than
    (radius + threshold). The radius accounts for the spread of the
    cluster: even if the centroid is not close enough, a member
    subscription within the cluster might be.

    Args:
        publication_embedding: L2-normalised publication embedding.
        peer_summaries: Mapping from peer domain_id to its subscription
            summary.
        threshold: Base cosine distance threshold (same as local routing).

    Returns:
        List of ``(domain_id, cluster_id, distance)`` tuples, sorted by
        ascending distance. Only candidates within the effective radius
        are included.
    """
    candidates: list[tuple[str, str, float]] = []

    for domain_id, summary in peer_summaries.items():
        if not summary.clusters:
            continue
        centroids = np.array([c.centroid_embedding for c in summary.clusters])
        distances = _cosine_distance(publication_embedding, centroids)

        for i, cluster in enumerate(summary.clusters):
            effective_threshold = cluster.radius + threshold
            if distances[i] < effective_threshold:
                candidates.append((domain_id, cluster.cluster_id, float(distances[i])))

    candidates.sort(key=lambda x: x[2])
    return candidates


# ---------------------------------------------------------------------------
# Step 3: Governance filter
# ---------------------------------------------------------------------------

def apply_governance_filter(
    candidates: list[tuple[str, str, float]],
    data_type: str,
    governance: GovernanceConstraints,
) -> list[tuple[str, str, float]]:
    """Remove candidates that violate data sovereignty or trust (step 3).

    Two checks are applied (Section 4.3.1):

    1. **Data sovereignty**: If ``data_type`` is in
       ``governance.local_data_types``, all candidates are removed (the
       data must not leave the domain).
    2. **Trust threshold**: Candidates from peers whose trust level is
       below ``governance.min_trust`` are removed.

    Args:
        candidates: List of ``(domain_id, cluster_id, distance)`` from
            ``select_federation_candidates``.
        data_type: Semantic type of the publication (e.g., "telemetry",
            "medical").
        governance: The local domain's governance constraints.

    Returns:
        Filtered candidate list (same format, order preserved).
    """
    # Data sovereignty: block all federation if this type is local-only
    if data_type in governance.local_data_types:
        return []

    # Trust filter
    return [
        (did, cid, dist)
        for did, cid, dist in candidates
        if governance.trust_levels.get(did, 0.0) >= governance.min_trust
    ]


# ---------------------------------------------------------------------------
# Full 5-step protocol
# ---------------------------------------------------------------------------

def federated_route(
    publication_embedding: np.ndarray,
    local_summary: SubscriptionSummary,
    peer_summaries: dict[str, SubscriptionSummary],
    threshold: float,
    data_type: str,
    governance: GovernanceConstraints,
) -> RoutingResult:
    """Execute the full 5-step federated routing protocol (Section 4.2.3).

    Steps:
        1. Attempt local routing.
        2. If no local match, select federation candidates from peers.
        3. Apply governance filter to candidates.
        4. Rank remaining candidates by cosine distance (ascending).
        5. Return the best candidate as a forwarded route, or a no-match
           result if no candidates survive.

    Args:
        publication_embedding: L2-normalised publication embedding.
        local_summary: This domain's subscription summary.
        peer_summaries: Mapping from peer domain_id to its summary.
        threshold: Cosine distance threshold (tau).
        data_type: Semantic type of the publication for governance checks.
        governance: Data sovereignty and trust constraints.

    Returns:
        A ``RoutingResult``. If ``forwarded`` is True, the publication
        should be sent to ``target_domain`` for delivery to
        ``target_cluster``.
    """
    # --- Step 1: Check local match ---
    # Compute cosine distance from the publication embedding to every cluster
    # centroid in the local subscription summary.  If the nearest centroid is
    # within `threshold` (tau), the publication is delivered locally and no
    # federation traffic is generated.
    local_result = route_locally(publication_embedding, local_summary, threshold)
    if local_result is not None:
        return local_result

    # --- Step 2: Query peer summaries and compute cosine similarity ---
    # Scan each peer domain's subscription summary.  A remote cluster is a
    # candidate if the cosine distance to its centroid is less than
    # (cluster.radius + threshold).  The radius accounts for intra-cluster
    # spread, so even if the centroid is not close enough, a member
    # subscription might be.  Candidates are returned sorted by distance.
    candidates = select_federation_candidates(
        publication_embedding, peer_summaries, threshold
    )

    # --- Step 3: Apply governance filter ---
    # Remove candidates that violate data sovereignty (publication type is
    # restricted to the local domain) or trust constraints (the peer
    # domain's trust level is below the minimum threshold).  The number of
    # filtered candidates is tracked for diagnostic reporting.
    pre_filter_count = len(candidates)
    candidates = apply_governance_filter(candidates, data_type, governance)
    governance_filtered = pre_filter_count - len(candidates)

    # --- Steps 4-5: Rank and return best match (or None) ---
    # Candidates are already sorted by ascending cosine distance from
    # step 2, so candidates[0] is the best match.  If no candidates
    # survived the governance filter, return a no-match result.
    if not candidates:
        return RoutingResult(
            matched=False,
            target_domain=None,
            target_cluster=None,
            confidence=0.0,
            forwarded=False,
            governance_filtered=governance_filtered,
        )

    best_domain, best_cluster, best_dist = candidates[0]
    return RoutingResult(
        matched=True,
        target_domain=best_domain,
        target_cluster=best_cluster,
        confidence=1.0 - best_dist,  # confidence = cosine similarity
        forwarded=True,
        governance_filtered=governance_filtered,
    )
