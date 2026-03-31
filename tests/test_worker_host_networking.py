"""Tests for worker host-networking support (unique ports + URL registration).

With host networking (network_mode: host), multiple workers on the same VM
share the network namespace. Each worker needs:
1. A unique port (--port flag or auto-assigned from base + offset)
2. To include its routable URL in the registration payload
3. To include its bid_cost_ms in the registration payload (for market mode)

These tests verify the registration payload without requiring a running broker.
"""

import pytest

from src.worker.worker import Worker, WorkerConfig


class TestWorkerPortAssignment:
    """Workers can be configured with unique ports."""

    def test_default_port(self):
        """Default port is 8081 for backward compatibility."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
        )
        assert config.port == 8081

    def test_custom_port(self):
        """Workers accept a custom port."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            port=8095,
        )
        assert config.port == 8095

    def test_twelve_workers_unique_ports(self):
        """12 workers on the same VM get unique ports from base + offset."""
        base_port = 8081
        configs = [
            WorkerConfig(
                node_id=f"w{i}", domain_id="d1", slice_id="URLLC",
                capacity=1.0, broker_url="http://localhost:8080",
                port=base_port + i,
            )
            for i in range(12)
        ]
        ports = [c.port for c in configs]
        assert len(set(ports)) == 12  # all unique
        assert min(ports) == 8081
        assert max(ports) == 8092


class TestWorkerURLConfig:
    """Workers have a configurable callback URL."""

    def test_default_url_is_empty(self):
        """Default callback_url is empty (broker infers from request IP)."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
        )
        assert config.callback_url == ""

    def test_custom_url(self):
        """Workers can set an explicit callback URL for host networking."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            callback_url="http://10.0.0.1:8081",
        )
        assert config.callback_url == "http://10.0.0.1:8081"

    def test_auto_url_from_port(self):
        """When callback_url is not set, worker can generate it from port."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            port=8095,
        )
        worker = Worker(config)
        # The worker should be able to generate its local URL
        assert worker.local_url == "http://localhost:8095"


class TestRegistrationPayload:
    """The registration payload includes URL and bid cost."""

    def test_payload_includes_url(self):
        """Registration payload contains the callback URL."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            callback_url="http://10.0.0.1:8081",
        )
        worker = Worker(config)
        payload = worker.registration_payload()
        assert payload["url"] == "http://10.0.0.1:8081"

    def test_payload_includes_bid_cost(self):
        """Registration payload contains bid_cost_ms for market mode."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            bid_cost_ms=150.0,
        )
        worker = Worker(config)
        payload = worker.registration_payload()
        assert payload["bid_cost_ms"] == 150.0

    def test_payload_includes_all_fields(self):
        """Registration payload has all required fields."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            callback_url="http://10.0.0.1:8095",
            bid_cost_ms=200.0,
            port=8095,
        )
        worker = Worker(config)
        payload = worker.registration_payload()
        assert payload["node_id"] == "w1"
        assert payload["domain_id"] == "d1"
        assert payload["slice_id"] == "URLLC"
        assert payload["capacity"] == 1.0
        assert payload["url"] == "http://10.0.0.1:8095"
        assert payload["bid_cost_ms"] == 200.0

    def test_payload_fallback_url_from_port(self):
        """When no explicit URL, payload uses localhost + port."""
        config = WorkerConfig(
            node_id="w1", domain_id="d1", slice_id="URLLC",
            capacity=1.0, broker_url="http://localhost:8080",
            port=8095,
        )
        worker = Worker(config)
        payload = worker.registration_payload()
        assert payload["url"] == "http://localhost:8095"
