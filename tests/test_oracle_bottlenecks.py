"""Tests for oracle bottleneck fixes.

Bug 4: HTTP connection pool exhaustion with 48 workers.
"""

from __future__ import annotations

import pytest

from src.broker.neural_broker import BrokerConfig, NeuralBroker


class TestHTTPConnectionPoolConfig:
    """HTTP client must have sufficient connection limits for 48 workers."""

    @pytest.mark.asyncio
    async def test_http_client_limits_sufficient(self):
        """Connection limits must handle 48+ concurrent worker connections."""
        broker = NeuralBroker(BrokerConfig(
            domain_id="d1", broker_id="b1",
        ))
        # Trigger startup by building the app and calling startup
        app = broker.build_app()
        # Manually fire startup
        for handler in app.router.on_startup:
            await handler()

        client = broker._http_client
        assert client is not None

        # Check via the transport's connection pool
        transport = client._transport
        pool = transport._pool
        assert pool._max_connections >= 96, (
            f"max_connections={pool._max_connections} too low for 48 workers. "
            f"Need >= 96 (2x workers for dispatch + health checks)"
        )
        assert pool._max_keepalive_connections >= 48, (
            f"max_keepalive_connections={pool._max_keepalive_connections} "
            f"too low for 48 workers"
        )

        # Cleanup
        for handler in app.router.on_shutdown:
            await handler()
