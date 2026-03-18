"""Integrator encapsulation for federated capacity estimation (paper Section 4.2.4, Eq. 8).

The integrator computes the composite capacity of a domain for a given
pipeline type. Full computation requires solving a max-flow problem on
the internal resource graph (nodes = execution units, edges = data links,
capacities = per-stage throughput). For this experiment, we use a simplified
bound that is valid for small domains:

    kappa_composite = sum_{u in U_eligible} (capacity_u - load_u)

where U_eligible is the set of execution units that can execute at least one
stage of the requested pipeline type.
"""

from __future__ import annotations

from src.broker.placement import ExecutionUnit


# ---------------------------------------------------------------------------
# Pipeline stage registry (simplified)
# ---------------------------------------------------------------------------

# Maps pipeline type to its ordered list of stage types.
# In the full system this comes from PipelineDAG; here we hard-code the
# experiment's pipeline types to match src/pipeline/patterns.py.
_PIPELINE_STAGES: dict[str, list[str]] = {
    "cqi_prediction": ["collect", "feature_extract", "predict"],
    "anomaly_detection": ["ingest", "preprocess", "detect", "alert"],
    "sensor_fusion": ["sensor", "fuse", "decide"],
}


def register_pipeline_type(pipeline_type: str, stages: list[str]) -> None:
    """Register a pipeline type and its stage sequence.

    Allows the experiment harness to define custom pipeline types beyond
    the built-in defaults.

    Args:
        pipeline_type: Identifier for the pipeline (e.g., "my_pipeline").
        stages: Ordered list of stage type names.
    """
    _PIPELINE_STAGES[pipeline_type] = list(stages)


# ---------------------------------------------------------------------------
# Helpers for adapting ExecutionUnit to the capacity model
# ---------------------------------------------------------------------------


def _node_available(node: ExecutionUnit) -> float:
    """Remaining capacity (events/s) before overload."""
    return max(0.0, node.residual_capacity)


def _node_supports_stage(node: ExecutionUnit, stage_type: str) -> bool:
    """Check whether a node can execute a given stage type.

    The placement.ExecutionUnit does not carry a ``supported_stages`` set.
    For this simplified integrator we assume every node can execute every
    stage type. Override this function or extend ExecutionUnit if the
    experiment requires heterogeneous capabilities.
    """
    return True


# ---------------------------------------------------------------------------
# Composite capacity computation (Eq. 8, simplified bound)
# ---------------------------------------------------------------------------

def compute_composite_capacity(
    domain_nodes: list[ExecutionUnit],
    pipeline_type: str,
) -> float:
    """Compute the composite available capacity of a domain for a pipeline type.

    Implements a simplified version of Eq. 8 (Section 4.2.4). The full
    formulation solves max-flow on the resource graph; this simplified
    bound sums the available capacity of all nodes that can execute at
    least one stage of the pipeline, then takes the minimum across stages
    (bottleneck stage determines throughput).

    For each stage in the pipeline, we sum the available capacity of nodes
    that support that stage. The composite capacity is the minimum across
    all stages (the bottleneck):

        kappa_composite = min_{s in stages} sum_{u : s in u.supported_stages} u.available

    This is a valid upper bound on the true max-flow for small domains where
    each stage has limited parallelism.

    Args:
        domain_nodes: List of execution units in this domain.
        pipeline_type: Identifier of the pipeline type (must be registered
            in ``_PIPELINE_STAGES`` or via ``register_pipeline_type``).

    Returns:
        Estimated composite capacity (events/s). Returns 0.0 if no node
        can execute any stage of the pipeline.

    Raises:
        ValueError: If ``pipeline_type`` is not registered.
    """
    stages = _PIPELINE_STAGES.get(pipeline_type)
    if stages is None:
        raise ValueError(
            f"Unknown pipeline type '{pipeline_type}'. "
            f"Registered types: {list(_PIPELINE_STAGES.keys())}. "
            f"Use register_pipeline_type() to add new types."
        )

    if not stages:
        return 0.0

    stage_capacities: list[float] = []
    for stage in stages:
        cap = sum(
            _node_available(node)
            for node in domain_nodes
            if _node_supports_stage(node, stage)
        )
        stage_capacities.append(cap)

    # Bottleneck: minimum across stages
    composite = min(stage_capacities) if stage_capacities else 0.0
    return composite


# ---------------------------------------------------------------------------
# Integrator class (thin wrapper around the module-level functions)
# ---------------------------------------------------------------------------


class Integrator:
    """Federated capacity integrator for a single domain.

    Holds a set of execution units and exposes composite capacity queries
    for registered pipeline types. Used by the broker to answer federation
    capacity probes.

    Args:
        domain_id: Identifier of the domain this integrator represents.
    """

    def __init__(self, domain_id: str) -> None:
        self.domain_id = domain_id
        self._nodes: list[ExecutionUnit] = []

    def add_node(self, node: ExecutionUnit) -> None:
        """Register an execution unit in this domain."""
        self._nodes.append(node)

    def remove_node(self, node_id: str) -> None:
        """Remove an execution unit by node_id."""
        self._nodes = [n for n in self._nodes if n.node_id != node_id]

    @property
    def nodes(self) -> list[ExecutionUnit]:
        return list(self._nodes)

    def composite_capacity(self, pipeline_type: str) -> float:
        """Compute composite available capacity for a pipeline type (Eq. 8)."""
        return compute_composite_capacity(self._nodes, pipeline_type)
