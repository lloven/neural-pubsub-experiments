"""Kafka Broker: Kafka-based baseline for pipeline stage distribution (S1).

A minimal broker that uses the same API surface as NeuralBroker but distributes
pipeline stages via Kafka topics instead of direct HTTP dispatch. Each pipeline
type maps to a Kafka topic. A separate sidecar container (kafka_consumer.py)
reads from Kafka and HTTP-dispatches to workers in round-robin order.  Workers
execute stages and POST results back to the broker's /result endpoint.

This two-process design (broker + sidecar) avoids asyncio scheduling issues
when running a long-lived Kafka consumer inside uvicorn's event loop.

Usage:
    python -m src.broker.kafka_broker --domain d1 --port 8080 \
        --kafka-bootstrap kafka:9092
"""

from __future__ import annotations

import argparse
import json
import logging
import time

import uvicorn
from fastapi import FastAPI

from src.broker.base import BaseBroker
from src.broker.models import PipelineState
from src.measurement.harness import TimestampRecord
from src.pipeline.dag import PipelineDAG

logger = logging.getLogger(__name__)


class KafkaBroker(BaseBroker):
    """Baseline broker that distributes pipeline stages via Kafka topics.

    Publishes each stage assignment to a Kafka topic (one topic per pipeline
    type).  A separate sidecar container (``kafka_consumer.py``) consumes
    from these topics and HTTP-dispatches to workers in round-robin order.

    This design represents the state-of-practice for NWDAF data flows
    (S1 baseline in the manuscript).
    """

    def __init__(
        self,
        domain_id: str,
        broker_id: str,
        kafka_bootstrap: str = "kafka:9092",
    ) -> None:
        super().__init__(domain_id, broker_id)
        self.kafka_bootstrap = kafka_bootstrap
        self._producer = None  # aiokafka.AIOKafkaProducer (created at startup)

    # ------------------------------------------------------------------
    # Kafka producer lifecycle
    # ------------------------------------------------------------------

    async def _start_producer(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()
        logger.info("Kafka producer connected to %s.", self.kafka_bootstrap)

    async def _stop_producer(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    # ------------------------------------------------------------------
    # Override build_app to add Kafka lifecycle hooks
    # ------------------------------------------------------------------

    def build_app(self) -> FastAPI:
        app = super().build_app()
        broker = self

        @app.get("/workers")
        async def workers() -> dict:
            """Return registered workers with their URLs (for kafka-consumer sidecar)."""
            async with broker._workers_lock:
                return {
                    nid: {"url": w.url, "domain_id": w.domain_id, "slice_id": w.slice_id}
                    for nid, w in broker._workers.items()
                }

        @app.on_event("startup")
        async def _kafka_startup() -> None:
            await broker._start_producer()
            # Consumer runs as a separate sidecar container
            # (kafka-consumer service in docker-compose.kafka.yaml).

        @app.on_event("shutdown")
        async def _kafka_shutdown() -> None:
            await broker._stop_producer()

        return app

    # ------------------------------------------------------------------
    # Placement (Kafka sentinel)
    # ------------------------------------------------------------------

    def _compute_placement(self, dag: PipelineDAG) -> dict[str, str]:
        return {sid: "kafka" for sid in dag.stages}

    # ------------------------------------------------------------------
    # Dispatch via Kafka
    # ------------------------------------------------------------------

    async def _dispatch_stage(self, ps: PipelineState, stage_id: str) -> None:
        stage = ps.dag.get_stage(stage_id)
        topic = ps.pipeline_type

        await self._metrics.record(
            TimestampRecord(
                pipeline_id=ps.pipeline_id,
                stage_id=stage_id,
                event="dispatched",
                timestamp=time.time(),
                node_id=self.broker_id,
                metadata={"pipeline_type": ps.pipeline_type},
            )
        )

        message = {
            "pipeline_id": ps.pipeline_id,
            "stage_id": stage_id,
            "stage_type": stage.stage_type,
            "computational_demand": stage.computational_demand,
            "input_data": "",
            "metadata": {
                "broker_id": self.broker_id,
                "pipeline_type": ps.pipeline_type,
            },
        }

        try:
            await self._producer.send_and_wait(topic, value=message)
            logger.debug(
                "Published stage '%s' (pipeline=%s) to topic '%s'.",
                stage_id, ps.pipeline_id, topic,
            )
        except Exception as exc:
            logger.error(
                "Failed to publish stage '%s' to Kafka: %s", stage_id, exc
            )
            async with self._pipelines_lock:
                ps.failed = True
                ps.error = f"Kafka publish failed for stage '{stage_id}': {exc}"
                self._active_pipelines.pop(ps.pipeline_id, None)
            await self._metrics.complete_pipeline(
                ps.pipeline_id, success=False, error=ps.error
            )


# ---------------------------------------------------------------------------
# CLI (Docker entrypoint: python -m src.broker.kafka_broker ...)
# ---------------------------------------------------------------------------


def _parse_args():
    parser = argparse.ArgumentParser(description="Kafka baseline broker.")
    parser.add_argument("--domain", default="d1", help="Domain ID.")
    parser.add_argument("--port", type=int, default=8080, help="Listen port.")
    parser.add_argument("--broker-id", default=None, help="Broker ID.")
    parser.add_argument(
        "--kafka-bootstrap", default="kafka:9092",
        help="Kafka bootstrap servers (default: kafka:9092).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    broker_id = args.broker_id or f"kafka-{args.domain}"
    broker = KafkaBroker(
        domain_id=args.domain,
        broker_id=broker_id,
        kafka_bootstrap=args.kafka_bootstrap,
    )
    local_app = broker.build_app()
    uvicorn.run(local_app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
