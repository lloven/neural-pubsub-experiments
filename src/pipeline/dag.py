"""Service-dependency DAG representation for Neural Pub/Sub pipelines.

Implements the pipeline graph G_p = (V, E) from paper Section 4.1, where each
vertex v in V is a processing stage with computational demand rho_v and output
data rate omega_v, and each edge (v, v') in E carries a latency bound L_{v,v'}.

The DAG supports both tree-structured pipelines (enabling optimal DP placement)
and general DAGs (requiring greedy heuristics).
"""

from __future__ import annotations

import types
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Stage:
    """A processing stage (vertex) in the pipeline DAG.

    Corresponds to a single computational step, e.g. feature extraction,
    prediction, or aggregation. Each stage carries resource requirements
    and governance constraints used during placement (Section 4.3).

    Attributes:
        id: Unique identifier for this stage within the pipeline.
        stage_type: Semantic type (e.g. "collect", "feature_extract", "predict",
            "aggregate"). Used by governance policies to enforce locality rules
            (Eq. 5).
        computational_demand: rho_v from Eq. 1 -- the processing load this stage
            imposes on its assigned execution unit, in normalised units where
            1.0 saturates one unit of capacity.
        output_data_rate: omega_v -- the data rate (e.g. Mbps) emitted by this
            stage toward its successors. Relevant for bandwidth-aware placement.
        slice_requirement: Optional network-slice identifier (e.g. "eMBB",
            "URLLC"). When set, the stage may only be placed on execution units
            that belong to this slice (Eq. 9).
        data_sovereignty_domain: Optional domain identifier. When set, the stage
            must be placed on a node within this domain, enforcing data
            sovereignty constraints (Eq. 5).
    """

    id: str
    stage_type: str
    computational_demand: float  # rho_v
    output_data_rate: float  # omega_v
    slice_requirement: Optional[str] = None
    data_sovereignty_domain: Optional[str] = None


@dataclass
class Edge:
    """A directed dependency edge in the pipeline DAG.

    Represents a data flow from ``source_id`` to ``target_id`` with a maximum
    tolerable end-to-end latency ``latency_bound`` (L_{v,v'} from Eq. 2).

    Attributes:
        source_id: Stage that produces data.
        target_id: Stage that consumes data.
        latency_bound: Maximum acceptable latency (ms) for data transfer
            between source and target, per Eq. 2.
    """

    source_id: str
    target_id: str
    latency_bound: float  # L_{v,v'}


class PipelineDAG:
    """Directed acyclic graph of processing stages and data-flow edges.

    Stores the pipeline topology G_p = (V, E) and provides structural queries
    needed by the placement algorithm (Section 4.3):

    * Topological ordering for greedy stage assignment.
    * Tree detection to decide between DP (optimal, O(|V|*|N|)) and greedy
      placement.
    * Source/sink identification for publisher and subscriber endpoints.

    Example::

        dag = PipelineDAG()
        dag.add_stage(Stage("s1", "collect", 0.2, 10.0))
        dag.add_stage(Stage("s2", "predict", 0.8, 1.0))
        dag.add_edge(Edge("s1", "s2", latency_bound=5.0))
        order = dag.topological_sort()  # ["s1", "s2"]
    """

    def __init__(self) -> None:
        self._stages: dict[str, Stage] = {}
        self._edges: list[Edge] = []
        self._edge_index: dict[tuple[str, str], Edge] = {}
        self._successors: dict[str, list[str]] = defaultdict(list)
        self._predecessors: dict[str, list[str]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def add_stage(self, stage: Stage) -> None:
        """Register a processing stage in the DAG.

        Args:
            stage: The stage to add. Its ``id`` must be unique within this DAG.

        Raises:
            ValueError: If a stage with the same id already exists.
        """
        if stage.id in self._stages:
            raise ValueError(f"Stage '{stage.id}' already exists in the DAG.")
        self._stages[stage.id] = stage

    def add_edge(self, edge: Edge) -> None:
        """Add a directed data-flow edge between two stages.

        Both ``edge.source_id`` and ``edge.target_id`` must refer to stages
        already present in the DAG. Adding the edge must not create a cycle.

        Args:
            edge: The edge to add.

        Raises:
            ValueError: If either endpoint is missing or the edge would create
                a cycle.
        """
        if edge.source_id not in self._stages:
            raise ValueError(
                f"Source stage '{edge.source_id}' not found in the DAG."
            )
        if edge.target_id not in self._stages:
            raise ValueError(
                f"Target stage '{edge.target_id}' not found in the DAG."
            )
        # Cycle check: target must not be able to reach source.
        if self._can_reach(edge.target_id, edge.source_id):
            raise ValueError(
                f"Adding edge {edge.source_id} -> {edge.target_id} would "
                f"create a cycle."
            )
        self._edges.append(edge)
        self._edge_index[(edge.source_id, edge.target_id)] = edge
        self._successors[edge.source_id].append(edge.target_id)
        self._predecessors[edge.target_id].append(edge.source_id)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def stages(self) -> types.MappingProxyType[str, Stage]:
        """All stages in the DAG, keyed by stage id (read-only view)."""
        return types.MappingProxyType(self._stages)

    @property
    def edges(self) -> tuple[Edge, ...]:
        """All edges in the DAG (immutable tuple)."""
        return tuple(self._edges)

    def get_stage(self, stage_id: str) -> Stage:
        """Return a stage by id.

        Raises:
            KeyError: If the stage does not exist.
        """
        return self._stages[stage_id]

    def get_edge(self, source_id: str, target_id: str) -> Optional[Edge]:
        """Return the edge between two stages, or None if no such edge."""
        return self._edge_index.get((source_id, target_id))

    def predecessors(self, stage_id: str) -> list[str]:
        """Return immediate predecessor stage ids (data providers).

        Args:
            stage_id: The stage whose predecessors are queried.

        Returns:
            List of stage ids that feed data into ``stage_id``.
        """
        return list(self._predecessors.get(stage_id, []))

    def successors(self, stage_id: str) -> list[str]:
        """Return immediate successor stage ids (data consumers).

        Args:
            stage_id: The stage whose successors are queried.

        Returns:
            List of stage ids that consume data from ``stage_id``.
        """
        return list(self._successors.get(stage_id, []))

    def sources(self) -> list[str]:
        """Return leaf stages with no predecessors (publisher endpoints).

        These are the entry points of the pipeline where raw data is ingested,
        corresponding to the publisher side of the pub/sub model.
        """
        return [
            sid for sid in self._stages if not self._predecessors.get(sid)
        ]

    def sinks(self) -> list[str]:
        """Return root stages with no successors (subscriber-facing outputs).

        These are the terminal stages whose output is delivered to subscribers.
        """
        return [
            sid for sid in self._stages if not self._successors.get(sid)
        ]

    def topological_sort(self) -> list[str]:
        """Return a topological ordering of stage ids (Kahn's algorithm).

        The ordering guarantees that for every edge (u, v), u appears before v.
        This is the iteration order used by the greedy placement heuristic
        (Section 4.3).

        Returns:
            List of stage ids in topological order.

        Raises:
            RuntimeError: If the graph contains a cycle (should not happen if
                edges are added through ``add_edge``).
        """
        in_degree: dict[str, int] = {sid: 0 for sid in self._stages}
        for edge in self._edges:
            in_degree[edge.target_id] += 1

        queue: deque[str] = deque(
            sid for sid, deg in in_degree.items() if deg == 0
        )
        order: list[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for succ in self._successors.get(current, []):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    queue.append(succ)

        if len(order) != len(self._stages):
            raise RuntimeError(
                "Topological sort failed: the graph contains a cycle."
            )
        return order

    def is_tree(self) -> bool:
        """Check whether the DAG permits optimal DP placement.

        The DP placement algorithm (Section 4.3) is optimal when each stage's
        predecessors have independent subproblems. This holds when no stage
        has more than one successor (no fan-out), because fan-out means the
        same subproblem (placing the shared predecessor) appears in multiple
        recurrences and may receive inconsistent assignments.

        Fan-in (multiple predecessors for one stage, e.g. funnel patterns) is
        safe: the DP sums over independent predecessor subtrees.

        Returns:
            True if no stage has more than one successor and there is exactly
            one sink, False otherwise.
        """
        sink_count = 0
        for sid in self._stages:
            n_succ = len(self._successors.get(sid, []))
            if n_succ > 1:
                return False  # fan-out creates shared subproblems
            if n_succ == 0:
                sink_count += 1
        return sink_count == 1

    def __len__(self) -> int:
        """Return the number of stages in the DAG."""
        return len(self._stages)

    def __contains__(self, stage_id: str) -> bool:
        return stage_id in self._stages

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _can_reach(self, from_id: str, to_id: str) -> bool:
        """BFS reachability check from ``from_id`` to ``to_id``."""
        if from_id == to_id:
            return True
        visited: set[str] = set()
        queue: deque[str] = deque([from_id])
        while queue:
            current = queue.popleft()
            for succ in self._successors.get(current, []):
                if succ == to_id:
                    return True
                if succ not in visited:
                    visited.add(succ)
                    queue.append(succ)
        return False
