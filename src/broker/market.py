"""Market clearing engine for Neural Pub/Sub.

Implements price-based allocation for AI pipeline orchestration,
grounded in Paper 1's Walrasian equilibrium result. For tree/SP DAGs
with gross-substitutes valuations, clearing prices converge and the
market allocation matches the centralized oracle.

The clearing mechanism:
1. Workers submit bids (stage_type, cost_per_stage)
2. Broker computes per-domain clearing prices via marginal cost pricing
3. Pipelines accept/reject based on value budget vs total price
4. Cross-domain trade via price arbitrage (remote + WAN < local)

Federation summaries propagate PriceSignal (aggregate prices),
not worker-level bid details (privacy-preserving).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WorkerBid:
    """A worker's reported cost for processing one stage of a given type.

    Attributes:
        worker_id: Identifier of the bidding worker.
        domain_id: Domain the worker belongs to.
        stage_type: Type of stage this bid covers.
        compute_ms: Expected compute time in milliseconds.
        cost_per_stage: Opportunity cost of allocating one slot.
            Should increase with worker utilization (congestion pricing).
    """

    worker_id: str
    domain_id: str
    stage_type: str
    compute_ms: float
    cost_per_stage: float


@dataclass
class PriceSignal:
    """Aggregate price information for federation.

    Contains per-stage-type clearing prices for one domain.
    Does NOT contain worker-level details (privacy-preserving).

    Attributes:
        domain_id: Which domain this signal represents.
        prices: Mapping from stage_type to clearing price.
    """

    domain_id: str
    prices: dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_clearing_prices(
        cls, clearing: dict[str, dict[str, float]], domain_id: str
    ) -> "PriceSignal":
        """Create a PriceSignal from the output of compute_clearing_prices."""
        domain_prices = clearing.get(domain_id, {})
        return cls(domain_id=domain_id, prices=dict(domain_prices))


DomainPrice = dict[str, dict[str, float]]
"""Type alias: {domain_id: {stage_type: clearing_price}}"""


def compute_clearing_prices(
    bids: list[WorkerBid],
    demand: dict[str, int],
) -> DomainPrice:
    """Compute per-domain clearing prices using marginal cost pricing.

    For each (domain, stage_type) pair, the clearing price is the cost
    of the marginal worker needed to meet demand. Workers are sorted by
    cost; the clearing price is the cost of the k-th cheapest worker
    where k = min(demand, supply).

    This implements a simplified posted-price mechanism. For tree/SP
    DAGs with GS valuations, this converges to the Walrasian equilibrium
    (Kelso-Crawford 1982, Paper 1 Proposition 3).

    Args:
        bids: List of worker bids across all domains.
        demand: Total demand per stage_type (across all domains).

    Returns:
        DomainPrice: {domain_id: {stage_type: clearing_price}}.
        Empty dict if no bids.
    """
    if not bids:
        return {}

    # Group bids by (domain_id, stage_type)
    grouped: dict[tuple[str, str], list[WorkerBid]] = {}
    for bid in bids:
        key = (bid.domain_id, bid.stage_type)
        grouped.setdefault(key, []).append(bid)

    result: DomainPrice = {}

    for (domain_id, stage_type), domain_bids in grouped.items():
        # Sort by cost (cheapest first)
        domain_bids.sort(key=lambda b: b.cost_per_stage)

        stage_demand = demand.get(stage_type, 0)
        if stage_demand == 0:
            # No demand → zero price
            result.setdefault(domain_id, {})[stage_type] = 0.0
            continue

        # Clearing price = cost of the marginal worker
        # (the most expensive worker needed to meet demand)
        supply = len(domain_bids)
        marginal_index = min(stage_demand, supply) - 1
        clearing_price = domain_bids[marginal_index].cost_per_stage

        result.setdefault(domain_id, {})[stage_type] = clearing_price

    return result


def should_trade_cross_domain(
    local_price: float,
    remote_price: float,
    wan_cost: float,
) -> bool:
    """Decide whether to trade cross-domain based on price comparison.

    Trade happens when the remote price plus WAN cost is strictly less
    than the local price. Ties prefer local (no WAN risk).

    Args:
        local_price: Clearing price in the local domain.
        remote_price: Clearing price in the remote domain.
        wan_cost: Additional cost of cross-domain data transfer (WAN latency).

    Returns:
        True if cross-domain trade is beneficial.
    """
    return (remote_price + wan_cost) < local_price
