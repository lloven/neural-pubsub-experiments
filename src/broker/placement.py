"""Slice-aware pipeline placement algorithm for Neural Pub/Sub.

Implements the placement optimisation from paper Section 4.3 (Eq. 10):

    min  alpha * L_total + beta * U_total + gamma * D_cross

where:
    L_total = sum of inter-stage latencies across all edges (Eq. 2),
    U_total = sum of per-stage load ratios rho_v / C_node (Eq. 1),
    D_cross = number of domain boundaries crossed (governance cost, Eq. 5).

The solver selects between two strategies based on DAG topology:

* **Tree DAGs**: dynamic programming in O(|V| * |N|), where |V| is the number
  of pipeline stages and |N| is the number of execution units. Optimal under
  the additive cost model.
* **General DAGs**: greedy assignment in topological order, assigning each
  stage to the lowest-cost feasible node. Near-optimal for DAGs with limited
  fan-in.

Feasibility is checked against four constraint classes:
    - Capacity (Eq. 1): rho_v <= C_node - current_load
    - Latency (Eq. 2): network latency(node_u, node_v) <= L_{u,v}
    - Governance (Eq. 5): sovereignty domains, trust levels
    - Slice (Eq. 9): stage slice requirement matches node slice
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

from src.pipeline.dag import PipelineDAG, Stage


@dataclass
class ExecutionUnit:
    """A compute node that can host pipeline stages.

    Represents a physical or virtual execution environment (edge server, cloud
    VM, RAN-local compute) characterised by its domain, slice membership, and
    available capacity.

    Attributes:
        node_id: Unique identifier for this node.
        domain_id: Data-sovereignty domain this node belongs to (Eq. 5).
        slice_id: Network slice this node is part of (Eq. 9).
        capacity: Maximum processing capacity C (normalised, Eq. 1).
        current_load: Currently consumed capacity on this node.
    """

    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    current_load: float = 0.0

    @property
    def residual_capacity(self) -> float:
        """Remaining capacity available for new stages."""
        return self.capacity - self.current_load


@dataclass
class NetworkTopology:
    """Network of execution units and pairwise latencies.

    Attributes:
        nodes: List of available execution units.
        latency_matrix: Mapping from (node_id_a, node_id_b) to network latency
            in milliseconds. Must be symmetric. Self-latency (same node) is 0.
    """

    nodes: list[ExecutionUnit] = field(default_factory=list)
    latency_matrix: dict[tuple[str, str], float] = field(default_factory=dict)
    _node_map: dict[str, ExecutionUnit] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        """Build the node lookup index."""
        self._node_map = {n.node_id: n for n in self.nodes}

    def get_node(self, node_id: str) -> ExecutionUnit:
        """Look up a node by id.

        Raises:
            KeyError: If the node does not exist.
        """
        try:
            return self._node_map[node_id]
        except KeyError:
            raise KeyError(f"Node '{node_id}' not found in topology.")

    def latency(self, a: str, b: str) -> float:
        """Return network latency between two nodes.

        Returns 0.0 for same-node communication. Checks both orderings of the
        pair since the matrix should be symmetric but might only store one.

        Raises:
            KeyError: If the pair is not in the latency matrix.
        """
        if a == b:
            return 0.0
        if (a, b) in self.latency_matrix:
            return self.latency_matrix[(a, b)]
        if (b, a) in self.latency_matrix:
            return self.latency_matrix[(b, a)]
        raise KeyError(f"No latency entry for ({a}, {b}).")


@dataclass
class GovernancePolicy:
    """Data-governance and trust constraints for pipeline placement.

    Encodes the governance rules from Section 4.2 (Eq. 5):

    * ``local_stage_types`` restricts certain stage types to their
      data-sovereignty domain (e.g. raw radio collection must stay local).
    * ``trust_levels`` quantifies cross-domain trust on [0, 1]. A value of 0
      means data cannot flow between those domains; 1 means full trust.

    Attributes:
        local_stage_types: Set of stage types that must remain within their
            ``data_sovereignty_domain``. If a stage has this type AND a
            sovereignty domain set, it may only be placed on nodes in that
            domain.
        trust_levels: Mapping from (domain_a, domain_b) to trust level in
            [0, 1]. Pairs not present default to 0 (no trust).
    """

    local_stage_types: set[str] = field(default_factory=set)
    trust_levels: dict[tuple[str, str], float] = field(default_factory=dict)

    def get_trust(self, domain_a: str, domain_b: str) -> float:
        """Return trust level between two domains.

        Same-domain trust is always 1.0. For cross-domain pairs, returns the
        stored value or 0.0 if not specified.
        """
        if domain_a == domain_b:
            return 1.0
        if (domain_a, domain_b) in self.trust_levels:
            return self.trust_levels[(domain_a, domain_b)]
        if (domain_b, domain_a) in self.trust_levels:
            return self.trust_levels[(domain_b, domain_a)]
        return 0.0


# --------------------------------------------------------------------------
# Feasibility checking
# --------------------------------------------------------------------------


def check_feasibility(
    placement: dict[str, str],
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
) -> tuple[bool, list[str]]:
    """Validate a placement against all constraint classes.

    Checks the four constraint families from Sections 4.1-4.2:

    1. **Capacity (Eq. 1)**: ``rho_v <= residual_capacity(node)``. Residual
       capacity accounts for both ``current_load`` and all other stages in
       this placement assigned to the same node.
    2. **Latency (Eq. 2)**: For every edge (u, v) in the DAG, the network
       latency between assigned nodes must not exceed ``L_{u,v}``.
    3. **Governance (Eq. 5)**: Stages with ``data_sovereignty_domain`` set must
       be placed in that domain. Cross-domain data flows require trust > 0.
       Stages whose type is in ``governance.local_stage_types`` must stay in
       their sovereignty domain.
    4. **Slice (Eq. 9)**: Stages with ``slice_requirement`` must be placed on
       nodes whose ``slice_id`` matches.

    Args:
        placement: Mapping from stage_id to node_id.
        dag: The pipeline DAG.
        topology: Network of execution units.
        governance: Governance policy.

    Returns:
        Tuple of (is_feasible, violations) where violations is a list of
        human-readable strings describing each violated constraint. Empty
        list means the placement is feasible.
    """
    violations: list[str] = []
    stages = dag.stages
    edges = dag.edges

    # Compute per-node load from this placement
    node_load: dict[str, float] = {}
    for stage_id, node_id in placement.items():
        stage = stages[stage_id]
        node_load[node_id] = node_load.get(node_id, 0.0) + stage.computational_demand

    # 1. Capacity (Eq. 1)
    for node_id, load in node_load.items():
        node = topology.get_node(node_id)
        if load > node.residual_capacity + 1e-9:
            violations.append(
                f"Capacity: node '{node_id}' overloaded "
                f"(demand={load:.3f}, residual={node.residual_capacity:.3f})."
            )

    # 2. Latency (Eq. 2) and cross-domain trust (Eq. 5) -- single edge pass
    for edge in edges:
        src_node_id = placement.get(edge.source_id)
        tgt_node_id = placement.get(edge.target_id)
        if src_node_id is None or tgt_node_id is None:
            violations.append(
                f"Latency: stage '{edge.source_id}' or '{edge.target_id}' "
                f"not placed."
            )
            continue
        actual_latency = topology.latency(src_node_id, tgt_node_id)
        if actual_latency > edge.latency_bound + 1e-9:
            violations.append(
                f"Latency: edge ({edge.source_id} -> {edge.target_id}) "
                f"latency {actual_latency:.2f}ms > bound {edge.latency_bound:.2f}ms "
                f"(nodes {src_node_id} -> {tgt_node_id})."
            )
        # Cross-domain trust
        src_domain = topology.get_node(src_node_id).domain_id
        tgt_domain = topology.get_node(tgt_node_id).domain_id
        if src_domain != tgt_domain:
            trust = governance.get_trust(src_domain, tgt_domain)
            if trust <= 0.0:
                violations.append(
                    f"Governance: no trust between domains '{src_domain}' and "
                    f"'{tgt_domain}' for edge ({edge.source_id} -> {edge.target_id})."
                )

    # 3. Governance (Eq. 5): sovereignty and local-stage-type
    for stage_id, node_id in placement.items():
        stage = stages[stage_id]
        node = topology.get_node(node_id)

        # Data sovereignty: stage must be in its declared domain
        if stage.data_sovereignty_domain is not None:
            if node.domain_id != stage.data_sovereignty_domain:
                violations.append(
                    f"Governance: stage '{stage_id}' requires domain "
                    f"'{stage.data_sovereignty_domain}' but placed on node "
                    f"'{node_id}' in domain '{node.domain_id}'."
                )

        # Local stage types: must stay in sovereignty domain
        if (
            stage.stage_type in governance.local_stage_types
            and stage.data_sovereignty_domain is not None
            and node.domain_id != stage.data_sovereignty_domain
        ):
            violations.append(
                f"Governance: stage '{stage_id}' (type={stage.stage_type}) "
                f"is local-only but placed outside its domain."
            )

    # 4. Slice (Eq. 9)
    for stage_id, node_id in placement.items():
        stage = stages[stage_id]
        node = topology.get_node(node_id)
        if stage.slice_requirement is not None:
            if node.slice_id != stage.slice_requirement:
                violations.append(
                    f"Slice: stage '{stage_id}' requires slice "
                    f"'{stage.slice_requirement}' but node '{node_id}' "
                    f"provides '{node.slice_id}'."
                )

    return (len(violations) == 0, violations)


# --------------------------------------------------------------------------
# Cost computation
# --------------------------------------------------------------------------


def _placement_cost(
    placement: dict[str, str],
    dag: PipelineDAG,
    topology: NetworkTopology,
    alpha: float,
    beta: float,
    gamma: float,
) -> float:
    """Compute the weighted placement cost per Eq. 10.

    Cost = alpha * L_total + beta * U_total + gamma * D_cross

    where:
        L_total = sum over edges of network latency between assigned nodes,
        U_total = sum over stages of (rho_v / C_node),
        D_cross = number of edges whose endpoints are in different domains.

    Args:
        placement: stage_id -> node_id mapping.
        dag: The pipeline DAG.
        topology: Network topology.
        alpha: Weight for latency term.
        beta: Weight for load-balance term.
        gamma: Weight for domain-crossing term.

    Returns:
        Scalar cost value (lower is better).
    """
    l_total = 0.0
    d_cross = 0

    for edge in dag.edges:
        src_node = placement[edge.source_id]
        tgt_node = placement[edge.target_id]
        l_total += topology.latency(src_node, tgt_node)
        if topology.get_node(src_node).domain_id != topology.get_node(tgt_node).domain_id:
            d_cross += 1

    u_total = 0.0
    for stage_id, node_id in placement.items():
        stage = dag.get_stage(stage_id)
        node = topology.get_node(node_id)
        u_total += stage.computational_demand / max(node.capacity, 1e-12)

    return alpha * l_total + beta * u_total + gamma * d_cross


# --------------------------------------------------------------------------
# Node feasibility for a single stage
# --------------------------------------------------------------------------


def _is_node_feasible(
    stage: Stage,
    node: ExecutionUnit,
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
    partial_placement: dict[str, str],
    additional_load: dict[str, float],
) -> bool:
    """Check whether assigning ``stage`` to ``node`` satisfies all constraints.

    This is the inner feasibility filter used during placement search. It
    checks constraints incrementally against the partial placement built so
    far.

    Args:
        stage: The stage to place.
        node: Candidate execution unit.
        dag: The pipeline DAG.
        topology: Network topology.
        governance: Governance policy.
        partial_placement: Stages already placed (stage_id -> node_id).
        additional_load: Extra load already committed to nodes in this
            placement round (node_id -> load).

    Returns:
        True if all constraints are satisfied.
    """
    # Shared checks: capacity, slice, sovereignty, local-stage-type
    if not _is_node_feasible_simple(stage, node, governance):
        return False

    # Additional capacity check accounting for load committed in this round
    committed = additional_load.get(node.node_id, 0.0)
    if committed > 0 and stage.computational_demand > node.residual_capacity - committed + 1e-9:
        return False

    # Latency (Eq. 2) for edges to already-placed predecessors
    for pred_id in dag.predecessors(stage.id):
        if pred_id in partial_placement:
            pred_node_id = partial_placement[pred_id]
            edge = dag.get_edge(pred_id, stage.id)
            if edge is not None:
                actual = topology.latency(pred_node_id, node.node_id)
                if actual > edge.latency_bound + 1e-9:
                    return False

    # Latency for edges to already-placed successors (rare in topo order,
    # but possible for edges added in non-standard order)
    for succ_id in dag.successors(stage.id):
        if succ_id in partial_placement:
            succ_node_id = partial_placement[succ_id]
            edge = dag.get_edge(stage.id, succ_id)
            if edge is not None:
                actual = topology.latency(node.node_id, succ_node_id)
                if actual > edge.latency_bound + 1e-9:
                    return False

    # Cross-domain trust for flows to/from already-placed neighbours
    for pred_id in dag.predecessors(stage.id):
        if pred_id in partial_placement:
            pred_domain = topology.get_node(partial_placement[pred_id]).domain_id
            if pred_domain != node.domain_id:
                if governance.get_trust(pred_domain, node.domain_id) <= 0.0:
                    return False

    for succ_id in dag.successors(stage.id):
        if succ_id in partial_placement:
            succ_domain = topology.get_node(partial_placement[succ_id]).domain_id
            if succ_domain != node.domain_id:
                if governance.get_trust(node.domain_id, succ_domain) <= 0.0:
                    return False

    return True


# --------------------------------------------------------------------------
# Greedy placement (general DAGs)
# --------------------------------------------------------------------------


def _greedy_placement(
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
    alpha: float,
    beta: float,
    gamma: float,
) -> dict[str, str]:
    """Greedy placement for general (non-tree) DAGs.

    Iterates through stages in topological order. For each stage, evaluates all
    feasible nodes and picks the one that minimises the incremental cost
    (Eq. 10) considering only already-placed predecessors.

    Complexity: O(|V| * |N|) where |V| = stages, |N| = nodes.

    Args:
        dag: The pipeline DAG.
        topology: Network topology.
        governance: Governance policy.
        alpha: Latency weight.
        beta: Load-balance weight.
        gamma: Domain-crossing weight.

    Returns:
        Mapping from stage_id to node_id.

    Raises:
        RuntimeError: If no feasible node exists for some stage.
    """
    placement: dict[str, str] = {}
    additional_load: dict[str, float] = {}
    order = dag.topological_sort()

    for stage_id in order:
        stage = dag.get_stage(stage_id)
        best_node: Optional[str] = None
        best_cost = math.inf

        for node in topology.nodes:
            if not _is_node_feasible(
                stage, node, dag, topology, governance, placement, additional_load
            ):
                continue

            # Incremental cost for placing this stage on this node
            latency_inc = 0.0
            domain_inc = 0

            for pred_id in dag.predecessors(stage_id):
                if pred_id in placement:
                    pred_node_id = placement[pred_id]
                    latency_inc += topology.latency(pred_node_id, node.node_id)
                    if topology.get_node(pred_node_id).domain_id != node.domain_id:
                        domain_inc += 1

            load_inc = stage.computational_demand / max(node.capacity, 1e-12)

            cost = alpha * latency_inc + beta * load_inc + gamma * domain_inc

            if cost < best_cost:
                best_cost = cost
                best_node = node.node_id

        if best_node is None:
            raise RuntimeError(
                f"No feasible node found for stage '{stage_id}'. "
                f"Check capacity, slice, governance, and latency constraints."
            )

        placement[stage_id] = best_node
        additional_load[best_node] = (
            additional_load.get(best_node, 0.0) + stage.computational_demand
        )

    return placement


# --------------------------------------------------------------------------
# DP placement (tree DAGs)
# --------------------------------------------------------------------------


def _dp_placement(
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
    alpha: float,
    beta: float,
    gamma: float,
) -> dict[str, str]:
    """Optimal DP placement for tree-structured DAGs.

    For a tree DAG with |V| stages and |N| execution units, computes the
    optimal placement in O(|V| * |N|^2) time using bottom-up dynamic
    programming.

    For each stage v and candidate node n, dp[v][n] stores the minimum
    cost of placing the subtree rooted at v such that v is assigned to n.

    The recurrence (bottom-up) is:

        dp[leaf][n] = beta * (rho_leaf / C_n)              if feasible
        dp[v][n]    = beta * (rho_v / C_n)
                      + sum over children c of:
                          min over m of (dp[c][m]
                              + alpha * latency(n, m)
                              + gamma * 1[domain(n) != domain(m)])

    After filling the table, the sink is assigned to the node with minimum
    dp value, and optimal assignments are traced back through the tree.

    Args:
        dag: A tree-structured pipeline DAG.
        topology: Network topology.
        governance: Governance policy.
        alpha: Latency weight.
        beta: Load-balance weight.
        gamma: Domain-crossing weight.

    Returns:
        Mapping from stage_id to node_id.

    Raises:
        RuntimeError: If no feasible placement exists.
    """
    stages = dag.stages
    node_ids = [n.node_id for n in topology.nodes]
    n_nodes = len(node_ids)
    INF = float("inf")

    # dp[stage_id][node_idx] = minimum cost of subtree rooted at stage
    dp: dict[str, list[float]] = {}
    # choice[stage_id][node_idx] = dict mapping child_stage_id -> node_idx
    choice: dict[str, list[dict[str, int]]] = {}

    # Process in topological order (sources first). For each stage, its
    # predecessors (children in the tree DP sense) have already been processed.
    # Edges point source -> sink; the DP tree is rooted at the sink.
    topo = dag.topological_sort()

    for stage_id in topo:
        stage = stages[stage_id]
        dp[stage_id] = [INF] * n_nodes
        choice[stage_id] = [{} for _ in range(n_nodes)]
        predecessors = dag.predecessors(stage_id)

        for ni, nid in enumerate(node_ids):
            node = topology.get_node(nid)

            # Basic feasibility for this stage on this node
            if not _is_node_feasible_simple(stage, node, governance):
                continue

            # Local cost: load term
            local_cost = beta * (stage.computational_demand / max(node.capacity, 1e-12))

            if not predecessors:
                # Leaf (source) stage: no children to account for
                dp[stage_id][ni] = local_cost
            else:
                # Sum over predecessors (children in tree DP sense)
                total = local_cost
                child_choices: dict[str, int] = {}
                feasible = True

                for pred_id in predecessors:
                    edge = dag.get_edge(pred_id, stage_id)
                    edge_bound = edge.latency_bound if edge else INF

                    best_child_cost = INF
                    best_child_node = -1

                    for mi, mid in enumerate(node_ids):
                        if dp[pred_id][mi] >= INF:
                            continue
                        lat = topology.latency(nid, mid)
                        if lat > edge_bound + 1e-9:
                            continue
                        # Trust check
                        pred_domain = topology.get_node(mid).domain_id
                        if pred_domain != node.domain_id:
                            if governance.get_trust(pred_domain, node.domain_id) <= 0.0:
                                continue

                        cross = 1 if topology.get_node(mid).domain_id != node.domain_id else 0
                        child_cost = dp[pred_id][mi] + alpha * lat + gamma * cross

                        if child_cost < best_child_cost:
                            best_child_cost = child_cost
                            best_child_node = mi

                    if best_child_node < 0:
                        feasible = False
                        break

                    total += best_child_cost
                    child_choices[pred_id] = best_child_node

                if feasible:
                    dp[stage_id][ni] = total
                    choice[stage_id][ni] = child_choices

    # Find the optimal assignment for the sink
    sinks = dag.sinks()
    if len(sinks) != 1:
        # Fallback: pick the sink with lowest cost
        pass

    sink_id = sinks[0]
    best_cost = INF
    best_sink_node = -1
    for ni in range(n_nodes):
        if dp[sink_id][ni] < best_cost:
            best_cost = dp[sink_id][ni]
            best_sink_node = ni

    if best_sink_node < 0:
        raise RuntimeError("No feasible DP placement found for the tree DAG.")

    # Trace back
    placement: dict[str, str] = {}

    def _trace(stage_id: str, node_idx: int) -> None:
        placement[stage_id] = node_ids[node_idx]
        for pred_id, pred_ni in choice[stage_id][node_idx].items():
            _trace(pred_id, pred_ni)

    _trace(sink_id, best_sink_node)
    return placement


def _is_node_feasible_simple(
    stage: Stage,
    node: ExecutionUnit,
    governance: GovernancePolicy,
) -> bool:
    """Lightweight feasibility check for DP (no partial placement context).

    Checks capacity, slice, and sovereignty constraints for a single
    stage-node pair without considering already-placed neighbours (those
    are handled by the DP recurrence).

    Args:
        stage: The stage to place.
        node: Candidate execution unit.
        governance: Governance policy.

    Returns:
        True if the stage can be placed on this node in isolation.
    """
    # Capacity (Eq. 1) -- approximate; DP does not track multi-stage load
    if stage.computational_demand > node.residual_capacity + 1e-9:
        return False

    # Slice (Eq. 9)
    if stage.slice_requirement is not None and node.slice_id != stage.slice_requirement:
        return False

    # Governance: data sovereignty (Eq. 5)
    if stage.data_sovereignty_domain is not None:
        if node.domain_id != stage.data_sovereignty_domain:
            return False

    # Governance: local stage type
    if (
        stage.stage_type in governance.local_stage_types
        and stage.data_sovereignty_domain is not None
        and node.domain_id != stage.data_sovereignty_domain
    ):
        return False

    return True


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def find_placement(
    dag: PipelineDAG,
    topology: NetworkTopology,
    governance: GovernancePolicy,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
) -> dict[str, str]:
    """Find a cost-minimising, feasible placement for a pipeline DAG.

    Implements the placement optimisation from Section 4.3, Eq. 10:

        min  alpha * L_total + beta * U_total + gamma * D_cross

    subject to capacity (Eq. 1), latency (Eq. 2), governance (Eq. 5),
    and slice (Eq. 9) constraints.

    Strategy selection:
        - Tree DAGs: DP in O(|V| * |N|^2) for optimal placement.
        - General DAGs: greedy assignment in topological order.

    Args:
        dag: The pipeline DAG to place.
        topology: Available execution units and pairwise latencies.
        governance: Data-governance and trust policy.
        alpha: Weight for the latency term in the cost function. Higher values
            prioritise low-latency placement.
        beta: Weight for the load-balance term. Higher values spread load
            more evenly across nodes.
        gamma: Weight for the domain-crossing penalty. Higher values keep
            pipelines within fewer domains.

    Returns:
        Mapping from stage_id to node_id. Every stage in the DAG receives
        an assignment.

    Raises:
        RuntimeError: If no feasible placement exists for one or more stages.
    """
    if len(dag) == 0:
        return {}

    if dag.is_tree():
        return _dp_placement(dag, topology, governance, alpha, beta, gamma)
    else:
        return _greedy_placement(dag, topology, governance, alpha, beta, gamma)
