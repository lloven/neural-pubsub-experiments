"""Tests for Kafka transport layer: concurrent consumer and dual-transport dispatch.

TDD cycles:
  - Task 1: Concurrent consumer with placement-from-message
  - Task 2: Dual-transport dispatch in BaseBroker
  - Task 3: NeuralBroker dual-transport
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


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


# ===========================================================================
# Task 2: Dual-Transport Dispatch in BaseBroker
# ===========================================================================


class TestDualTransportBaseBroker:
    """BaseBroker must support transport='http' (direct) and transport='kafka'."""

    def test_base_broker_accepts_transport_param(self):
        """BaseBroker.__init__ must accept a 'transport' parameter."""
        from src.broker.static_broker import StaticBroker

        # HTTP transport (default)
        broker_http = StaticBroker(domain_id="d1", broker_id="test-http", transport="http")
        assert broker_http.transport == "http"

        # Kafka transport
        broker_kafka = StaticBroker(
            domain_id="d1", broker_id="test-kafka",
            transport="kafka", kafka_bootstrap="kafka:9092",
        )
        assert broker_kafka.transport == "kafka"

    def test_base_broker_default_transport_is_http(self):
        """If transport not specified, default to 'http'."""
        from src.broker.static_broker import StaticBroker

        broker = StaticBroker(domain_id="d1", broker_id="test")
        assert broker.transport == "http"

    def test_dispatch_stage_http_posts_directly(self):
        """With transport='http', _dispatch_stage POSTs directly to worker URL."""
        from src.broker.static_broker import StaticBroker
        from src.broker.models import PipelineState, WorkerInfo
        from src.pipeline.dag import PipelineDAG, Stage

        broker = StaticBroker(domain_id="d1", broker_id="test", transport="http")

        # Register a worker
        broker._workers["w1"] = WorkerInfo(
            node_id="w1", domain_id="d1", slice_id="embb",
            capacity=10.0, url="http://worker1:8081",
        )

        # Create a simple pipeline
        dag = PipelineDAG()
        dag.add_stage(Stage(id="s1", stage_type="predict", computational_demand=1.0, output_data_rate=1.0))
        ps = PipelineState(
            pipeline_id="p1", pipeline_type="cqi_prediction",
            dag=dag, placement={"s1": "w1"},
        )

        # Mock the HTTP client
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_resp
        broker._http_client = mock_client

        asyncio.run(broker._dispatch_stage(ps, "s1"))

        mock_client.post.assert_called_once()
        url_called = mock_client.post.call_args[0][0]
        assert "worker1:8081/execute" in url_called

    def test_dispatch_stage_kafka_publishes_with_target_url(self):
        """With transport='kafka', _dispatch_stage publishes to Kafka with target_url."""
        from src.broker.static_broker import StaticBroker
        from src.broker.models import PipelineState, WorkerInfo
        from src.pipeline.dag import PipelineDAG, Stage

        broker = StaticBroker(
            domain_id="d1", broker_id="test",
            transport="kafka", kafka_bootstrap="kafka:9092",
        )

        broker._workers["w1"] = WorkerInfo(
            node_id="w1", domain_id="d1", slice_id="embb",
            capacity=10.0, url="http://worker1:8081",
        )

        dag = PipelineDAG()
        dag.add_stage(Stage(id="s1", stage_type="predict", computational_demand=1.0, output_data_rate=1.0))
        ps = PipelineState(
            pipeline_id="p1", pipeline_type="cqi_prediction",
            dag=dag, placement={"s1": "w1"},
        )

        # Mock the Kafka producer
        mock_producer = AsyncMock()
        broker._producer = mock_producer

        asyncio.run(broker._dispatch_stage(ps, "s1"))

        mock_producer.send_and_wait.assert_called_once()
        call_args = mock_producer.send_and_wait.call_args
        topic = call_args[0][0]
        message = call_args[1].get("value") or call_args[0][1]

        assert topic == "cqi_prediction", f"Expected topic 'cqi_prediction', got '{topic}'"
        assert message["target_url"] == "http://worker1:8081", (
            f"Kafka message must contain target_url from placement"
        )
        assert message["target_worker"] == "w1"
        assert message["pipeline_id"] == "p1"
        assert message["stage_id"] == "s1"


class TestStaticBrokerNoDispatchOverride:
    """StaticBroker must NOT override _dispatch_stage (inherits from BaseBroker)."""

    def test_static_broker_uses_base_dispatch(self):
        """StaticBroker._dispatch_stage should be inherited, not overridden."""
        from src.broker.static_broker import StaticBroker
        from src.broker.base import BaseBroker

        # If StaticBroker defines its own _dispatch_stage, it shadows BaseBroker's.
        # After refactoring, it should NOT define one.
        assert StaticBroker._dispatch_stage is BaseBroker._dispatch_stage, (
            "StaticBroker should inherit _dispatch_stage from BaseBroker, not override it."
        )
