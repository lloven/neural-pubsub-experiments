"""Kafka Consumer Sidecar: consumes from Kafka topics and HTTP-dispatches to workers.

Runs as a standalone process alongside any broker using Kafka transport.
Reads stage assignments from Kafka topics, extracts the target_url from
each message (embedded by the broker's placement decision), and POSTs to
that specific worker. Workers execute stages and report results back to
the broker via HTTP.

Dispatch is concurrent (asyncio.create_task per message) with a bounded
semaphore to avoid overwhelming workers.

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

import httpx

logger = logging.getLogger(__name__)

# Pipeline types used in experiments (must match workload generator).
EXPERIMENT_TOPICS = ("cqi_prediction", "anomaly_detection", "sensor_fusion")

# Default max concurrent HTTP dispatches.
DEFAULT_MAX_CONCURRENT = 20


# ---------------------------------------------------------------------------
# Dispatch primitives (testable without Kafka)
# ---------------------------------------------------------------------------


async def dispatch_message(
    payload: dict,
    client: httpx.AsyncClient,
    *,
    semaphore: asyncio.Semaphore | None = None,
) -> None:
    """Dispatch a single stage assignment to the target worker.

    Reads ``target_url`` from the payload and POSTs the stage execution
    request there.  If ``target_url`` is absent, logs a warning and skips.

    Args:
        payload: Deserialized Kafka message containing stage assignment.
        client: Shared async HTTP client.
        semaphore: Optional concurrency limiter.
    """
    target_url = payload.get("target_url")
    if not target_url:
        logger.warning("Message missing target_url; skipping: %s", payload.get("pipeline_id"))
        return

    execute_url = f"{target_url.rstrip('/')}/execute"

    async def _do_post():
        try:
            resp = await client.post(execute_url, json=payload, timeout=30.0)
            resp.raise_for_status()
        except Exception as exc:
            logger.error(
                "Dispatch to %s failed (pipeline=%s, stage=%s): %s",
                target_url, payload.get("pipeline_id"), payload.get("stage_id"), exc,
            )

    if semaphore is not None:
        async with semaphore:
            await _do_post()
    else:
        await _do_post()


async def dispatch_batch(
    messages: list[dict],
    client: httpx.AsyncClient,
    *,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
) -> None:
    """Dispatch multiple stage assignments concurrently.

    Uses ``asyncio.create_task`` per message with a bounded semaphore.

    Args:
        messages: List of deserialized Kafka message payloads.
        client: Shared async HTTP client.
        max_concurrent: Maximum number of concurrent HTTP dispatches.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        asyncio.create_task(dispatch_message(msg, client, semaphore=semaphore))
        for msg in messages
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Kafka topic pre-creation
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Broker worker registry polling
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------


async def run_consumer(
    kafka_bootstrap: str,
    broker_url: str,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
) -> None:
    """Main consumer loop: subscribe to Kafka topics, dispatch to workers."""
    from aiokafka import AIOKafkaConsumer

    logger.info(
        "Kafka consumer sidecar starting (bootstrap=%s, broker=%s, max_concurrent=%d)",
        kafka_bootstrap, broker_url, max_concurrent,
    )

    # Pre-create topics
    await _pre_create_topics(kafka_bootstrap, EXPERIMENT_TOPICS)
    logger.info("Topics pre-created: %s", EXPERIMENT_TOPICS)

    semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient() as client:
        # Poll broker until workers are registered (for fallback round-robin)
        worker_urls: dict[str, str] = {}
        worker_cycle: itertools.cycle | None = None

        for _ in range(60):
            worker_urls = await _fetch_workers(broker_url, client)
            if worker_urls:
                break
            await asyncio.sleep(1.0)

        if worker_urls:
            worker_cycle = itertools.cycle(sorted(worker_urls.keys()))
            logger.info("Found %d workers: %s", len(worker_urls), sorted(worker_urls.keys()))
        else:
            logger.warning("No workers registered after 60s; proceeding without fallback.")

        # Start Kafka consumer
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

                # If message has target_url (placed by broker), dispatch directly.
                # Otherwise fall back to round-robin (legacy compatibility).
                if not payload.get("target_url") and worker_cycle:
                    node_id = next(worker_cycle)
                    url = worker_urls.get(node_id)
                    if url:
                        payload["target_url"] = url

                # Concurrent dispatch via create_task
                asyncio.create_task(
                    dispatch_message(payload, client, semaphore=semaphore)
                )

        except asyncio.CancelledError:
            logger.info("Consumer cancelled.")
        finally:
            await consumer.stop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Kafka consumer sidecar.")
    parser.add_argument(
        "--kafka-bootstrap", default="kafka:9092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--broker-url", default="http://broker-d1:8080",
        help="URL of the broker to fetch worker registry from.",
    )
    parser.add_argument(
        "--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT,
        help=f"Max concurrent HTTP dispatches (default: {DEFAULT_MAX_CONCURRENT}).",
    )
    args = parser.parse_args()
    asyncio.run(run_consumer(args.kafka_bootstrap, args.broker_url, args.max_concurrent))


if __name__ == "__main__":
    main()
