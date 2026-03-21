"""Tests for Kafka transport layer: concurrent consumer and dual-transport dispatch.

TDD cycles:
  - Task 1: Concurrent consumer with placement-from-message
  - Task 2: Dual-transport dispatch in BaseBroker
  - Task 3: NeuralBroker dual-transport
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ===========================================================================
# Task 1: Concurrent Kafka Consumer with Placement-from-Message
# ===========================================================================


class TestKafkaConsumerPlacement:
    """Consumer must read target_url from Kafka message and dispatch there,
    not round-robin."""

    def test_dispatches_to_target_url_from_message(self):
        """Consumer reads target_url from message payload and POSTs to that URL."""
        from src.broker.kafka_consumer import dispatch_message

        message_payload = {
            "pipeline_id": "p1",
            "stage_id": "s1",
            "stage_type": "predict",
            "target_worker": "d1-urllc-1",
            "target_url": "http://worker-d1-urllc-1:8081",
            "computational_demand": 1.0,
            "metadata": {"broker_id": "broker-d1", "pipeline_type": "cqi_prediction"},
        }

        # Mock HTTP client
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response

        asyncio.run(dispatch_message(message_payload, mock_client))

        # Must dispatch to the target_url from the message, not round-robin
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "http://worker-d1-urllc-1:8081/execute", (
            f"Expected dispatch to target_url/execute, got {call_args[0][0]}"
        )

    def test_payload_forwarded_to_worker(self):
        """The full stage payload is forwarded to the worker."""
        from src.broker.kafka_consumer import dispatch_message

        message_payload = {
            "pipeline_id": "p1",
            "stage_id": "s1",
            "stage_type": "predict",
            "target_worker": "d1-urllc-1",
            "target_url": "http://worker:8081",
            "computational_demand": 1.0,
            "metadata": {"broker_id": "broker-d1", "pipeline_type": "cqi_prediction"},
        }

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response

        asyncio.run(dispatch_message(message_payload, mock_client))

        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs[0][1]
        assert sent_json["pipeline_id"] == "p1"
        assert sent_json["stage_id"] == "s1"
        assert sent_json["stage_type"] == "predict"


class TestKafkaConsumerConcurrency:
    """Consumer must dispatch concurrently, not sequentially."""

    def test_dispatches_concurrently_not_sequentially(self):
        """10 messages × 0.5s each should complete in <2s (concurrent), not ~5s (sequential)."""
        from src.broker.kafka_consumer import dispatch_batch

        messages = [
            {
                "pipeline_id": f"p{i}",
                "stage_id": "s1",
                "stage_type": "predict",
                "target_worker": f"w{i}",
                "target_url": f"http://worker-{i}:8081",
                "computational_demand": 1.0,
                "metadata": {"broker_id": "b1", "pipeline_type": "cqi_prediction"},
            }
            for i in range(10)
        ]

        async def slow_post(*args, **kwargs):
            await asyncio.sleep(0.5)
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            return resp

        mock_client = AsyncMock()
        mock_client.post.side_effect = slow_post

        start = time.monotonic()
        asyncio.run(dispatch_batch(messages, mock_client))
        elapsed = time.monotonic() - start

        # Concurrent: ~0.5s. Sequential: ~5s. Allow generous margin.
        assert elapsed < 2.0, (
            f"dispatch_batch took {elapsed:.1f}s for 10×0.5s messages. "
            f"Should be <2s (concurrent), not ~5s (sequential)."
        )

    def test_bounds_concurrent_dispatches_with_semaphore(self):
        """Max concurrent dispatches must be bounded."""
        from src.broker.kafka_consumer import dispatch_batch

        max_concurrent_seen = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        async def tracking_post(*args, **kwargs):
            nonlocal max_concurrent_seen, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent_seen:
                    max_concurrent_seen = current_concurrent
            await asyncio.sleep(0.1)
            async with lock:
                current_concurrent -= 1
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            return resp

        messages = [
            {
                "pipeline_id": f"p{i}",
                "stage_id": "s1",
                "stage_type": "predict",
                "target_worker": f"w{i}",
                "target_url": f"http://worker-{i}:8081",
                "computational_demand": 1.0,
                "metadata": {"broker_id": "b1", "pipeline_type": "cqi_prediction"},
            }
            for i in range(50)
        ]

        mock_client = AsyncMock()
        mock_client.post.side_effect = tracking_post

        asyncio.run(dispatch_batch(messages, mock_client, max_concurrent=10))

        assert max_concurrent_seen <= 10, (
            f"Max concurrent dispatches was {max_concurrent_seen}, expected ≤10. "
            f"Semaphore not enforced."
        )
