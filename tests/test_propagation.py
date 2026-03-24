"""Unit tests for SummaryPropagator (federation module, paper Section 4.2.5).

Covers: lifecycle, peer health tracking, push/receive summaries, edge cases,
and error handling. Written per TDD skill (tests first, verify behaviour).

Lessons applied:
- L37: no shortcuts; test real behaviour through the public API
- L38: verify treatments (assert the HTTP call was actually made, not just outcome)
- L39: failures must not be swallowed silently
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch, MagicMock

import httpx
import numpy as np
import pytest

from src.federation.propagation import SummaryPropagator
from src.federation.summary import (
    ClusterSummary,
    SubscriptionSummary,
    serialize,
    deserialize,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unit_vec(dim: int, idx: int) -> np.ndarray:
    """Return a unit vector with 1.0 at position idx."""
    v = np.zeros(dim, dtype=np.float32)
    v[idx] = 1.0
    return v


def _make_summary(
    domain_id: str = "domain-A",
    n_clusters: int = 1,
    timestamp: float = 1000.0,
) -> SubscriptionSummary:
    """Create a minimal SubscriptionSummary for testing."""
    clusters = [
        ClusterSummary(
            cluster_id=f"c{i}",
            centroid_embedding=_unit_vec(4, i % 4),
            radius=0.1,
            available_capacity=100.0,
        )
        for i in range(n_clusters)
    ]
    return SubscriptionSummary(
        domain_id=domain_id,
        clusters=clusters,
        timestamp=timestamp,
    )


def _make_propagator(
    peers: list[str] | None = None,
    interval: float = 10.0,
    timeout: float = 5.0,
    max_failures: int = 3,
) -> SummaryPropagator:
    """Create a SummaryPropagator with sensible test defaults."""
    if peers is None:
        peers = ["http://peer-b:8000", "http://peer-c:8000"]
    return SummaryPropagator(
        domain_id="domain-A",
        peers=peers,
        interval_seconds=interval,
        timeout_seconds=timeout,
        max_peer_failures=max_failures,
    )


# ---------------------------------------------------------------------------
# 1. Lifecycle tests
# ---------------------------------------------------------------------------

class TestLifecycle:
    """SummaryPropagator creation, start, stop."""

    def test_creation_stores_peers(self):
        peers = ["http://a:8000", "http://b:8000"]
        p = SummaryPropagator(
            domain_id="d1", peers=peers, interval_seconds=5.0
        )
        assert p.domain_id == "d1"
        assert p.peers == peers
        assert p.interval_seconds == 5.0

    def test_creation_copies_peer_list(self):
        """Mutating the original list must not affect the propagator."""
        peers = ["http://a:8000"]
        p = SummaryPropagator(domain_id="d1", peers=peers)
        peers.append("http://b:8000")
        assert len(p.peers) == 1, "Propagator should have its own copy of peers"

    def test_creation_defaults(self):
        p = SummaryPropagator(domain_id="d1", peers=[])
        assert p.interval_seconds == 10.0
        assert p.timeout_seconds == 5.0
        assert p.max_peer_failures == 3

    def test_initial_state_no_local_summary(self):
        p = _make_propagator()
        assert p.local_summary is None
        assert p.get_peer_summaries() == {}
        assert p.propagation_latencies_ms() == []

    @pytest.mark.asyncio
    async def test_start_sets_running(self):
        p = _make_propagator()
        await p.start()
        assert p._running is True
        assert p._task is not None
        await p.stop()

    @pytest.mark.asyncio
    async def test_stop_clears_task(self):
        p = _make_propagator()
        await p.start()
        await p.stop()
        assert p._running is False
        assert p._task is None

    @pytest.mark.asyncio
    async def test_double_start_is_idempotent(self):
        """Calling start() twice should not create a second task."""
        p = _make_propagator()
        await p.start()
        first_task = p._task
        await p.start()
        assert p._task is first_task, "Second start() should not create a new task"
        await p.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_safe(self):
        """Calling stop() before start() should not raise."""
        p = _make_propagator()
        await p.stop()  # Should not raise
        assert p._running is False


# ---------------------------------------------------------------------------
# 2. Peer health tracking tests
# ---------------------------------------------------------------------------

class TestPeerHealth:
    """Peer health state transitions: healthy -> unhealthy -> recovery."""

    def test_new_peer_is_healthy_by_default(self):
        p = _make_propagator(peers=["http://peer:8000"])
        assert p.is_peer_healthy("http://peer:8000") is True

    def test_unknown_peer_is_healthy(self):
        """A peer URL never seen before should be considered healthy."""
        p = _make_propagator(peers=[])
        assert p.is_peer_healthy("http://never-seen:8000") is True

    def test_peer_becomes_unhealthy_after_max_failures(self):
        p = _make_propagator(max_failures=3)
        peer = "http://peer-b:8000"
        for _ in range(3):
            p._record_peer_failure(peer)
        assert p.is_peer_healthy(peer) is False

    def test_peer_stays_healthy_below_threshold(self):
        p = _make_propagator(max_failures=3)
        peer = "http://peer-b:8000"
        for _ in range(2):
            p._record_peer_failure(peer)
        assert p.is_peer_healthy(peer) is True

    def test_failure_count_accumulates(self):
        p = _make_propagator(max_failures=5)
        peer = "http://peer-b:8000"
        for _ in range(4):
            p._record_peer_failure(peer)
        assert p.is_peer_healthy(peer) is True
        p._record_peer_failure(peer)
        assert p.is_peer_healthy(peer) is False

    def test_peer_health_is_per_peer(self):
        """Failures on one peer must not affect another."""
        p = _make_propagator(peers=["http://a:8000", "http://b:8000"], max_failures=2)
        for _ in range(2):
            p._record_peer_failure("http://a:8000")
        assert p.is_peer_healthy("http://a:8000") is False
        assert p.is_peer_healthy("http://b:8000") is True


# ---------------------------------------------------------------------------
# 3. Push summary tests
# ---------------------------------------------------------------------------

class TestPushSummary:
    """push_summary sends HTTP POST to all peers."""

    @pytest.mark.asyncio
    async def test_push_sends_to_all_peers(self):
        """Verify each peer receives a POST request (L38: verify treatment)."""
        p = _make_propagator(peers=["http://a:8000", "http://b:8000"])
        summary = _make_summary("domain-A")

        posted_urls = []

        async def mock_post(url, *, content, headers):
            posted_urls.append(url)
            resp = httpx.Response(200, request=httpx.Request("POST", url))
            return resp

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert "http://a:8000/federation/summary" in posted_urls
        assert "http://b:8000/federation/summary" in posted_urls
        assert len(posted_urls) == 2

    @pytest.mark.asyncio
    async def test_push_sends_msgpack_content_type(self):
        """Wire format must be application/x-msgpack (Section 4.5.1)."""
        p = _make_propagator(peers=["http://a:8000"])
        summary = _make_summary("domain-A")

        captured_headers = {}

        async def mock_post(url, *, content, headers):
            captured_headers.update(headers)
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert captured_headers.get("Content-Type") == "application/x-msgpack"

    @pytest.mark.asyncio
    async def test_push_sends_serialized_summary(self):
        """Payload must be deserializable back to a valid summary."""
        p = _make_propagator(peers=["http://a:8000"])
        summary = _make_summary("domain-A", n_clusters=2)

        captured_content = None

        async def mock_post(url, *, content, headers):
            nonlocal captured_content
            captured_content = content
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert captured_content is not None
        recovered = deserialize(captured_content)
        assert recovered.domain_id == "domain-A"
        assert len(recovered.clusters) == 2

    @pytest.mark.asyncio
    async def test_push_records_latency(self):
        p = _make_propagator(peers=["http://a:8000"])
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            assert len(p.propagation_latencies_ms()) == 0
            await p.push_summary(summary)
            latencies = p.propagation_latencies_ms()
            assert len(latencies) == 1
            assert latencies[0] >= 0.0

    @pytest.mark.asyncio
    async def test_push_resets_failure_count_on_success(self):
        """Successful push must reset consecutive failure count (recovery)."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=3)
        peer = "http://a:8000"

        # Accumulate 2 failures (below threshold)
        p._record_peer_failure(peer)
        p._record_peer_failure(peer)
        assert p._peer_failures.get(peer, 0) == 2

        async def mock_post(url, *, content, headers):
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(_make_summary("domain-A"))

        # Failure count should be cleared
        assert p._peer_failures.get(peer, 0) == 0
        assert p.is_peer_healthy(peer) is True

    @pytest.mark.asyncio
    async def test_push_recovers_unhealthy_peer_on_success(self):
        """An unhealthy peer that responds 200 must be marked healthy again."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=2)
        peer = "http://a:8000"

        # Make peer unhealthy
        for _ in range(2):
            p._record_peer_failure(peer)
        assert p.is_peer_healthy(peer) is False

        async def mock_post(url, *, content, headers):
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(_make_summary("domain-A"))

        assert p.is_peer_healthy(peer) is True

    @pytest.mark.asyncio
    async def test_push_strips_trailing_slash_from_peer_url(self):
        """Peer URL http://a:8000/ should produce http://a:8000/federation/summary."""
        p = _make_propagator(peers=["http://a:8000/"])
        summary = _make_summary("domain-A")

        posted_urls = []

        async def mock_post(url, *, content, headers):
            posted_urls.append(url)
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert posted_urls == ["http://a:8000/federation/summary"]


# ---------------------------------------------------------------------------
# 4. Receive summary tests
# ---------------------------------------------------------------------------

class TestReceiveSummary:
    """receive_summary stores incoming peer summaries."""

    @pytest.mark.asyncio
    async def test_receive_stores_summary(self):
        p = _make_propagator()
        summary = _make_summary("domain-B")
        await p.receive_summary(summary)

        peer_summaries = p.get_peer_summaries()
        assert "domain-B" in peer_summaries
        assert peer_summaries["domain-B"] is summary

    @pytest.mark.asyncio
    async def test_receive_replaces_older_summary_from_same_domain(self):
        p = _make_propagator()
        old = _make_summary("domain-B", timestamp=1000.0)
        new = _make_summary("domain-B", timestamp=2000.0)

        await p.receive_summary(old)
        await p.receive_summary(new)

        peer_summaries = p.get_peer_summaries()
        assert peer_summaries["domain-B"].timestamp == 2000.0

    @pytest.mark.asyncio
    async def test_receive_from_multiple_peers(self):
        p = _make_propagator()
        await p.receive_summary(_make_summary("domain-B"))
        await p.receive_summary(_make_summary("domain-C"))

        peer_summaries = p.get_peer_summaries()
        assert len(peer_summaries) == 2
        assert "domain-B" in peer_summaries
        assert "domain-C" in peer_summaries

    @pytest.mark.asyncio
    async def test_get_peer_summaries_returns_copy(self):
        """Mutating the returned dict must not affect internal state."""
        p = _make_propagator()
        await p.receive_summary(_make_summary("domain-B"))

        summaries = p.get_peer_summaries()
        summaries.pop("domain-B")
        # Internal state should still have it
        assert "domain-B" in p.get_peer_summaries()


# ---------------------------------------------------------------------------
# 5. update_local_summary tests
# ---------------------------------------------------------------------------

class TestUpdateLocalSummary:
    """update_local_summary sets the summary for the next push round."""

    def test_update_local_summary(self):
        p = _make_propagator()
        summary = _make_summary("domain-A")
        p.update_local_summary(summary)
        assert p.local_summary is summary

    def test_update_local_summary_replaces_previous(self):
        p = _make_propagator()
        s1 = _make_summary("domain-A", timestamp=1.0)
        s2 = _make_summary("domain-A", timestamp=2.0)
        p.update_local_summary(s1)
        p.update_local_summary(s2)
        assert p.local_summary is s2


# ---------------------------------------------------------------------------
# 6. Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge cases: empty peers, all peers down, stale summaries."""

    @pytest.mark.asyncio
    async def test_push_with_empty_peer_list(self):
        """push_summary with no peers should complete without error."""
        p = _make_propagator(peers=[])
        summary = _make_summary("domain-A")
        await p.push_summary(summary)
        # Should record a latency (the round happened, just with 0 peers)
        assert len(p.propagation_latencies_ms()) == 1

    @pytest.mark.asyncio
    async def test_push_continues_when_one_peer_fails(self):
        """Failure on one peer must not prevent pushing to others (L39 context)."""
        p = _make_propagator(peers=["http://fail:8000", "http://ok:8000"])
        summary = _make_summary("domain-A")

        posted_urls = []

        async def mock_post(url, *, content, headers):
            posted_urls.append(url)
            if "fail" in url:
                raise httpx.ConnectError("Connection refused")
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        # Both peers were attempted
        assert len(posted_urls) == 2
        # The OK peer should be healthy, the failed peer should have a failure recorded
        assert p.is_peer_healthy("http://ok:8000") is True
        assert p._peer_failures.get("http://fail:8000", 0) == 1

    @pytest.mark.asyncio
    async def test_all_peers_down_completes_without_raising(self):
        """When every peer fails, push_summary should still complete."""
        p = _make_propagator(peers=["http://a:8000", "http://b:8000"])
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            raise httpx.ConnectError("Connection refused")

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            # Should not raise
            await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 1
        assert p._peer_failures.get("http://b:8000", 0) == 1

    @pytest.mark.asyncio
    async def test_receive_stale_summary_overwrites_newer(self):
        """BUG DOCUMENTATION: receive_summary does NOT check timestamps.
        A stale summary (lower timestamp) will overwrite a newer one.

        This is a potential bug: if summaries arrive out of order due to
        network delays, the propagator will use stale data. The paper
        (Section 4.2.5) mentions freshness checks but the implementation
        does not enforce them.
        """
        p = _make_propagator()
        newer = _make_summary("domain-B", timestamp=2000.0)
        stale = _make_summary("domain-B", timestamp=1000.0)

        await p.receive_summary(newer)
        assert p.get_peer_summaries()["domain-B"].timestamp == 2000.0

        # Stale summary arrives after newer one
        await p.receive_summary(stale)
        # BUG: stale summary overwrites newer one because there's no timestamp check
        assert p.get_peer_summaries()["domain-B"].timestamp == 1000.0, (
            "Expected stale overwrite (current behaviour). "
            "If this fails, the bug has been fixed -- remove this test."
        )

    @pytest.mark.asyncio
    async def test_receive_duplicate_summary_is_idempotent(self):
        """Receiving the same summary twice should be harmless."""
        p = _make_propagator()
        summary = _make_summary("domain-B", timestamp=1000.0)

        await p.receive_summary(summary)
        await p.receive_summary(summary)

        peer_summaries = p.get_peer_summaries()
        assert len(peer_summaries) == 1
        assert peer_summaries["domain-B"].timestamp == 1000.0


# ---------------------------------------------------------------------------
# 7. Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    """Network errors, HTTP 4xx/5xx, malformed responses."""

    @pytest.mark.asyncio
    async def test_http_500_records_failure(self):
        """Peer returning 500 should increment failure count."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=3)
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            return httpx.Response(500, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 1
        assert p.is_peer_healthy("http://a:8000") is True  # only 1 failure

    @pytest.mark.asyncio
    async def test_http_400_records_failure(self):
        """Peer returning 400 should also be treated as a failure."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=2)
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            return httpx.Response(400, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 1

    @pytest.mark.asyncio
    async def test_http_success_codes_do_not_record_failure(self):
        """HTTP 2xx responses should not record failures."""
        p = _make_propagator(peers=["http://a:8000"])
        summary = _make_summary("domain-A")

        for status_code in [200, 201, 204]:
            async def mock_post(url, *, content, headers, _sc=status_code):
                return httpx.Response(_sc, request=httpx.Request("POST", url))

            with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
                client_instance = AsyncMock()
                client_instance.post = mock_post
                client_instance.__aenter__ = AsyncMock(return_value=client_instance)
                client_instance.__aexit__ = AsyncMock(return_value=False)
                MockClient.return_value = client_instance

                await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 0

    @pytest.mark.asyncio
    async def test_network_timeout_records_failure(self):
        """httpx.TimeoutException should be caught and recorded."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=2)
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            raise httpx.TimeoutException("timed out")

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 1

    @pytest.mark.asyncio
    async def test_connect_error_records_failure(self):
        """httpx.ConnectError should be caught and recorded."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=3)
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            raise httpx.ConnectError("Connection refused")

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 1

    @pytest.mark.asyncio
    async def test_consecutive_failures_mark_unhealthy(self):
        """max_peer_failures consecutive HTTP errors mark peer unhealthy."""
        p = _make_propagator(peers=["http://a:8000"], max_failures=3)
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            return httpx.Response(503, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            for _ in range(3):
                await p.push_summary(summary)

        assert p.is_peer_healthy("http://a:8000") is False

    @pytest.mark.asyncio
    async def test_unhealthy_peer_still_receives_push_attempts(self):
        """BUG DOCUMENTATION: Unhealthy peers are still contacted on every push.

        The propagator does NOT skip unhealthy peers in push_summary().
        It always sends to all peers in self.peers regardless of health status.
        This means bandwidth is wasted on peers known to be down.

        Whether this is intentional (to detect recovery) or a bug is unclear.
        The paper does not specify the expected behaviour for unhealthy peers.
        """
        p = _make_propagator(peers=["http://a:8000"], max_failures=2)
        summary = _make_summary("domain-A")

        call_count = 0

        async def mock_post(url, *, content, headers):
            nonlocal call_count
            call_count += 1
            return httpx.Response(503, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            # First 2 pushes: make peer unhealthy
            await p.push_summary(summary)
            await p.push_summary(summary)
            assert p.is_peer_healthy("http://a:8000") is False

            # Third push: peer is unhealthy but still contacted
            await p.push_summary(summary)

        assert call_count == 3, (
            "Unhealthy peer was still contacted (current behaviour). "
            "If this fails, behaviour has changed."
        )

    @pytest.mark.asyncio
    async def test_http_3xx_does_not_record_failure(self):
        """HTTP 3xx redirects should not be treated as failures
        (status_code < 400)."""
        p = _make_propagator(peers=["http://a:8000"])
        summary = _make_summary("domain-A")

        async def mock_post(url, *, content, headers):
            return httpx.Response(302, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.push_summary(summary)

        assert p._peer_failures.get("http://a:8000", 0) == 0


# ---------------------------------------------------------------------------
# 8. Propagation loop tests
# ---------------------------------------------------------------------------

class TestPropagationLoop:
    """The internal _loop pushes local_summary periodically."""

    @pytest.mark.asyncio
    async def test_loop_does_not_push_when_no_local_summary(self):
        """If local_summary is None, the loop should skip pushing."""
        p = _make_propagator(peers=["http://a:8000"], interval=0.05)
        assert p.local_summary is None

        call_count = 0

        async def mock_post(url, *, content, headers):
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.start()
            await asyncio.sleep(0.15)  # ~3 intervals
            await p.stop()

        assert call_count == 0, "Should not push when local_summary is None"

    @pytest.mark.asyncio
    async def test_loop_pushes_when_local_summary_set(self):
        """When local_summary is set, the loop should push it."""
        p = _make_propagator(peers=["http://a:8000"], interval=0.05)
        p.update_local_summary(_make_summary("domain-A"))

        call_count = 0

        async def mock_post(url, *, content, headers):
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.start()
            await asyncio.sleep(0.15)
            await p.stop()

        assert call_count >= 1, "Loop should have pushed at least once"

    @pytest.mark.asyncio
    async def test_loop_survives_exception_in_push(self):
        """An exception during push should not kill the loop (L39 context)."""
        p = _make_propagator(peers=["http://a:8000"], interval=0.05)
        p.update_local_summary(_make_summary("domain-A"))

        call_count = 0

        async def mock_post(url, *, content, headers):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Unexpected error in first push")
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("src.federation.propagation.httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = mock_post
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            await p.start()
            await asyncio.sleep(0.2)
            await p.stop()

        assert call_count >= 2, (
            "Loop should have retried after first push failure"
        )
