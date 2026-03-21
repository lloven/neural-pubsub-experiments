"""Static Broker: round-robin and random baseline placement.

A minimal broker that uses the same API surface as NeuralBroker but replaces
the neural placement engine with simple round-robin or random assignment.
No federation, no health monitoring, no governance constraints.

Usage:
    python -m src.broker.static_broker --domain d1 --port 8080
    PLACEMENT=random python -m src.broker.static_broker --domain d1 --port 8080
"""

from __future__ import annotations

import argparse
import enum
import itertools
import logging
import os
import random

import uvicorn

from src.broker.base import BaseBroker
from src.broker.models import WorkerInfo
from src.pipeline.dag import PipelineDAG

logger = logging.getLogger(__name__)


class PlacementStrategy(enum.Enum):
    """Supported static placement strategies."""

    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


class StaticBroker(BaseBroker):
    """Baseline broker with round-robin or random stage placement.

    Inherits dual-transport dispatch from BaseBroker (HTTP or Kafka).
    """

    def __init__(
        self,
        domain_id: str,
        broker_id: str,
        placement: str | PlacementStrategy = PlacementStrategy.ROUND_ROBIN,
        *,
        transport: str = "http",
        kafka_bootstrap: str | None = None,
    ) -> None:
        super().__init__(domain_id, broker_id, transport=transport, kafka_bootstrap=kafka_bootstrap)
        if isinstance(placement, str):
            placement = PlacementStrategy(placement)
        self.placement = placement
        self._worker_cycle: itertools.cycle | None = None

    # ------------------------------------------------------------------
    # Worker-change hook
    # ------------------------------------------------------------------

    def _on_worker_change(self) -> None:
        self._rebuild_cycle()

    def _rebuild_cycle(self) -> None:
        """Rebuild the round-robin iterator from current workers."""
        ids = sorted(self._workers.keys())
        self._worker_cycle = itertools.cycle(ids) if ids else None

    # ------------------------------------------------------------------
    # Placement
    # ------------------------------------------------------------------

    def _pick_worker(self) -> str:
        if not self._workers:
            raise RuntimeError("No workers registered.")
        if self.placement is PlacementStrategy.RANDOM:
            return random.choice(list(self._workers.keys()))
        if self._worker_cycle is None:
            self._rebuild_cycle()
        return next(self._worker_cycle)  # type: ignore[arg-type]

    def _compute_placement(self, dag: PipelineDAG) -> dict[str, str]:
        order = dag.topological_sort()
        return {stage_id: self._pick_worker() for stage_id in order}

    # Dispatch is inherited from BaseBroker (dual-transport: HTTP or Kafka).


# ---------------------------------------------------------------------------
# Module-level app (for uvicorn / Docker CMD)
# ---------------------------------------------------------------------------

_domain = os.environ.get("DOMAIN", "d1")
_broker_id = os.environ.get("BROKER_ID", f"static-{_domain}")
_placement = os.environ.get("PLACEMENT", "round_robin")
_transport = os.environ.get("TRANSPORT", "http")
_kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

_broker = StaticBroker(
    domain_id=_domain, broker_id=_broker_id, placement=_placement,
    transport=_transport,
    kafka_bootstrap=_kafka_bootstrap if _transport == "kafka" else None,
)
app = _broker.build_app()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args():
    parser = argparse.ArgumentParser(description="Static baseline broker.")
    parser.add_argument("--domain", default="d1", help="Domain ID.")
    parser.add_argument("--port", type=int, default=8080, help="Listen port.")
    parser.add_argument("--broker-id", default=None, help="Broker ID.")
    parser.add_argument(
        "--placement", default=os.environ.get("PLACEMENT", "round_robin"),
        choices=[s.value for s in PlacementStrategy],
        help="Placement strategy (default: round_robin, or PLACEMENT env var).",
    )
    parser.add_argument("--peers", default="", help="Ignored (for compose compatibility with neural_broker).")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    broker_id = args.broker_id or f"static-{args.domain}"
    broker = StaticBroker(
        domain_id=args.domain,
        broker_id=broker_id,
        placement=args.placement,
    )
    local_app = broker.build_app()
    uvicorn.run(local_app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
