"""Kafka Consumer Sidecar: consumes from Kafka topics and HTTP-dispatches to workers.

Runs as a standalone process alongside KafkaBroker. Reads stage assignments
from Kafka topics and POSTs them to workers registered with the broker.
Workers execute stages and report results back to the broker via HTTP.

This sidecar design avoids asyncio task scheduling issues when running
the consumer inside the broker's uvicorn event loop.

Usage (Docker Compose):
    python -m src.broker.kafka_consumer \
        --kafka-bootstrap kafka:9092 \
        --broker-url http://broker-d1:8080
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import logging
import time

import httpx

logger = logging.getLogger(__name__)

# Pipeline types used in experiments (must match workload generator).
EXPERIMENT_TOPICS = ("cqi_prediction", "anomaly_detection", "sensor_fusion")


async def _pre_create_topics(
    bootstrap: str, topics: tuple[str, ...], timeout: float = 10.0
) -> None:
    """Send sentinel messages to pre-create Kafka topics."""
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        for topic in topics:
            for attempt in range(3):
                try:
                    await asyncio.wait_for(
                        producer.send_and_wait(topic, value={"_sentinel": True}),
                        timeout=timeout,
                    )
                    logger.info("Pre-created topic: %s", topic)
                    break
                except asyncio.TimeoutError:
                    logger.warning("Timeout creating topic '%s' (attempt %d/3)", topic, attempt + 1)
                except Exception as exc:
                    logger.warning("Failed to create topic '%s': %s", topic, exc)
                    break
        await asyncio.sleep(2.0)  # metadata propagation
    finally:
        await producer.stop()


async def _fetch_workers(broker_url: str, client: httpx.AsyncClient) -> dict[str, str]:
    """Fetch registered worker URLs from the broker's /workers endpoint."""
    try:
        resp = await client.get(f"{broker_url.rstrip('/')}/workers", timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        return {nid: w.get("url", "") for nid, w in data.items() if w.get("url")}
    except Exception as exc:
        logger.warning("Failed to fetch workers from broker: %s", exc)
    return {}


async def run_consumer(
    kafka_bootstrap: str,
    broker_url: str,
) -> None:
    """Main consumer loop: subscribe to Kafka topics, dispatch to workers."""
    from aiokafka import AIOKafkaConsumer

    logger.info("Kafka consumer sidecar starting (bootstrap=%s, broker=%s)", kafka_bootstrap, broker_url)

    # Pre-create topics
    await _pre_create_topics(kafka_bootstrap, EXPERIMENT_TOPICS)
    logger.info("Topics pre-created: %s", EXPERIMENT_TOPICS)

    # Wait for broker to be ready and have workers
    async with httpx.AsyncClient() as client:
        worker_urls: dict[str, str] = {}
        worker_cycle: itertools.cycle | None = None

        # Poll broker until workers are registered
        for _ in range(60):  # up to 60s
            worker_urls = await _fetch_workers(broker_url, client)
            if worker_urls:
                break
            await asyncio.sleep(1.0)

        if not worker_urls:
            logger.error("No workers registered after 60s; exiting.")
            return

        worker_cycle = itertools.cycle(sorted(worker_urls.keys()))
        logger.info("Found %d workers: %s", len(worker_urls), sorted(worker_urls.keys()))

        # Start consumer
        consumer = AIOKafkaConsumer(
            *EXPERIMENT_TOPICS,
            bootstrap_servers=kafka_bootstrap,
            group_id="kafka-consumer-sidecar",
            value_deserializer=lambda v: v,
            auto_offset_reset="latest",
            retry_backoff_ms=500,
            metadata_max_age_ms=5000,
        )
        await consumer.start()
        logger.info("Kafka consumer started, subscribed to: %s", EXPERIMENT_TOPICS)

        try:
            async for msg in consumer:
                try:
                    payload = json.loads(msg.value)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                # Skip sentinels
                if payload.get("_sentinel"):
                    continue

                # Round-robin pick
                node_id = next(worker_cycle)
                url = worker_urls.get(node_id)
                if not url:
                    continue

                execute_url = f"{url.rstrip('/')}/execute"
                try:
                    resp = await client.post(execute_url, json=payload, timeout=30.0)
                    resp.raise_for_status()
                except Exception as exc:
                    logger.error("Dispatch to %s failed: %s", node_id, exc)

        except asyncio.CancelledError:
            logger.info("Consumer cancelled.")
        finally:
            await consumer.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Kafka consumer sidecar.")
    parser.add_argument(
        "--kafka-bootstrap", default="kafka:9092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--broker-url", default="http://broker-d1:8080",
        help="URL of the KafkaBroker to fetch worker registry from.",
    )
    args = parser.parse_args()
    asyncio.run(run_consumer(args.kafka_bootstrap, args.broker_url))


if __name__ == "__main__":
    main()
