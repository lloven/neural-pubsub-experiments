"""Subscription summary for cross-domain federation (paper Section 4.2.2).

Each domain (broker) periodically compresses its local subscription state into
a compact ``SubscriptionSummary`` that can be exchanged with federation peers.
The summary contains one ``ClusterSummary`` per Neural Router cluster, with
the centroid embedding, the cluster radius, and available capacity.

Equation 7 defines the summary tuple:

    S_k = { (bar{e}_{k,i}, r_{k,i}, kappa_{k,i}) | i in [1..C_k] }

where bar{e}_{k,i} is the centroid of cluster i in domain k, r_{k,i} is the
maximum cosine distance from the centroid to any member embedding, and
kappa_{k,i} is the available processing capacity for that cluster.

Serialisation uses msgpack with numpy arrays encoded as lists for compact
wire transfer (Section 4.5.1).

Second-level summary compression (Section 4.5.2) re-clusters the centroids
into fewer super-clusters using k-means, reducing bandwidth for domains with
many clusters.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import msgpack
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ClusterSummary:
    """Summary of a single subscription cluster within a domain.

    Corresponds to one element of the summary tuple S_k (Eq. 7).

    Attributes:
        cluster_id: Unique identifier of the cluster within this domain.
        centroid_embedding: bar{e}_{k,i} -- mean embedding of the cluster's
            subscriptions (L2-normalised).
        radius: r_{k,i} -- maximum cosine distance (1 - cosine_similarity)
            from the centroid to any member embedding. Defines the sphere
            within which a publication is considered a potential match.
        available_capacity: kappa_{k,i} -- maximum additional throughput
            (events/s) this cluster can absorb before overload.
    """

    cluster_id: str
    centroid_embedding: np.ndarray  # bar{e}_{k,i}
    radius: float  # r_{k,i}
    available_capacity: float  # kappa_{k,i}


@dataclass
class SubscriptionSummary:
    """Aggregated subscription summary for one domain (Eq. 7).

    Exchanged between federation peers via the propagation service
    (Section 4.2.5). Contains one ``ClusterSummary`` per Neural Router
    cluster.

    Attributes:
        domain_id: Identifier of the domain (broker) this summary represents.
        clusters: List of per-cluster summaries.
        timestamp: Unix epoch when this summary was computed; used for
            freshness checks during cross-domain routing.
    """

    domain_id: str
    clusters: list[ClusterSummary]
    timestamp: float


# ---------------------------------------------------------------------------
# Summary construction
# ---------------------------------------------------------------------------

def create_summary(
    domain_id: str,
    clusters: list[dict[str, Any]],
    embeddings: dict[str, np.ndarray],
    capacities: dict[str, float] | None = None,
) -> SubscriptionSummary:
    """Create a SubscriptionSummary from the Neural Router's cluster state.

    Reads the cluster centroids and member embeddings to compute each
    cluster's radius (max cosine distance from centroid to any member).

    Args:
        domain_id: Identifier of the local domain (broker).
        clusters: List of cluster dicts, each with keys ``id`` (str or int)
            and ``subscription_ids`` (list of subscription identifiers).
        embeddings: Mapping from subscription id to its L2-normalised
            embedding vector (as produced by ``EmbeddingModel.encode``
            with ``normalize=True``).
        capacities: Optional mapping from cluster id to available capacity
            (events/s). Defaults to 0.0 for clusters not present.

    Returns:
        A ``SubscriptionSummary`` with one ``ClusterSummary`` per cluster.
    """
    capacities = capacities or {}
    cluster_summaries: list[ClusterSummary] = []

    for cluster in clusters:
        cid = str(cluster["id"])
        sub_ids = cluster["subscription_ids"]

        # Gather member embeddings
        member_embs = np.array([embeddings[sid] for sid in sub_ids])

        # Centroid: mean of L2-normalised embeddings, re-normalised
        centroid = member_embs.mean(axis=0)
        norm = np.linalg.norm(centroid)
        if norm > 0:
            centroid = centroid / norm

        # Radius: max cosine distance from centroid to any member
        # cosine_distance = 1 - cosine_similarity
        sims = cosine_similarity(centroid.reshape(1, -1), member_embs).flatten()
        radius = float(1.0 - sims.min()) if len(sims) > 0 else 0.0

        cluster_summaries.append(
            ClusterSummary(
                cluster_id=cid,
                centroid_embedding=centroid,
                radius=radius,
                available_capacity=capacities.get(cid, 0.0),
            )
        )

    return SubscriptionSummary(
        domain_id=domain_id,
        clusters=cluster_summaries,
        timestamp=time.time(),
    )


# ---------------------------------------------------------------------------
# Serialisation (Section 4.5.1)
# ---------------------------------------------------------------------------

def serialize(summary: SubscriptionSummary) -> bytes:
    """Serialize a ``SubscriptionSummary`` to msgpack bytes.

    Numpy arrays are converted to lists of floats for msgpack compatibility.
    The resulting bytes are suitable for wire transfer between federation
    peers.

    Args:
        summary: The subscription summary to serialize.

    Returns:
        Compact msgpack-encoded bytes.
    """
    payload = {
        "domain_id": summary.domain_id,
        "timestamp": summary.timestamp,
        "clusters": [
            {
                "cluster_id": cs.cluster_id,
                "centroid_embedding": cs.centroid_embedding.tolist(),
                "radius": cs.radius,
                "available_capacity": cs.available_capacity,
            }
            for cs in summary.clusters
        ],
    }
    return msgpack.packb(payload, use_bin_type=True)


def deserialize(data: bytes) -> SubscriptionSummary:
    """Deserialize msgpack bytes into a ``SubscriptionSummary``.

    Reconstructs numpy arrays from lists and rebuilds the dataclass
    hierarchy.

    Args:
        data: msgpack-encoded bytes (as produced by ``serialize``).

    Returns:
        Reconstructed ``SubscriptionSummary``.
    """
    payload = msgpack.unpackb(data, raw=False)
    clusters = [
        ClusterSummary(
            cluster_id=c["cluster_id"],
            centroid_embedding=np.array(c["centroid_embedding"], dtype=np.float32),
            radius=c["radius"],
            available_capacity=c["available_capacity"],
        )
        for c in payload["clusters"]
    ]
    return SubscriptionSummary(
        domain_id=payload["domain_id"],
        clusters=clusters,
        timestamp=payload["timestamp"],
    )


# ---------------------------------------------------------------------------
# Second-level compression (Section 4.5.2)
# ---------------------------------------------------------------------------

def compress_summary(
    summary: SubscriptionSummary,
    max_clusters: int,
) -> SubscriptionSummary:
    """Compress a summary by re-clustering centroids into super-clusters.

    When a domain has many clusters, propagating all of them increases
    bandwidth. This function applies k-means to the centroid embeddings
    to produce ``max_clusters`` super-clusters, each with:

    - A new centroid (mean of the merged centroids, re-normalised).
    - A radius that is the maximum of the merged radii plus the distance
      from the super-centroid to the farthest merged centroid.
    - Summed available capacity.

    This is the second-level compression described in Section 4.5.2.

    Args:
        summary: Original subscription summary.
        max_clusters: Target number of super-clusters (must be >= 1).

    Returns:
        A new ``SubscriptionSummary`` with at most ``max_clusters`` entries.
    """
    if len(summary.clusters) <= max_clusters:
        return summary

    centroids = np.array([cs.centroid_embedding for cs in summary.clusters])
    km = KMeans(n_clusters=max_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(centroids)

    super_clusters: list[ClusterSummary] = []
    for label in range(max_clusters):
        mask = labels == label
        members = [summary.clusters[i] for i in range(len(summary.clusters)) if mask[i]]
        if not members:
            continue

        member_centroids = np.array([m.centroid_embedding for m in members])

        # Super-centroid: mean of member centroids, re-normalised
        super_centroid = member_centroids.mean(axis=0)
        norm = np.linalg.norm(super_centroid)
        if norm > 0:
            super_centroid = super_centroid / norm

        # Super-radius: max(member radius + distance from super-centroid to member centroid)
        sims = cosine_similarity(
            super_centroid.reshape(1, -1), member_centroids
        ).flatten()
        distances = 1.0 - sims
        super_radius = float(max(
            m.radius + d for m, d in zip(members, distances)
        ))

        # Summed capacity
        total_capacity = sum(m.available_capacity for m in members)

        super_clusters.append(
            ClusterSummary(
                cluster_id=f"super_{label}",
                centroid_embedding=super_centroid,
                radius=super_radius,
                available_capacity=total_capacity,
            )
        )

    return SubscriptionSummary(
        domain_id=summary.domain_id,
        clusters=super_clusters,
        timestamp=summary.timestamp,
    )
