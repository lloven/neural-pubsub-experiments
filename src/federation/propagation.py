"""Summary propagation service for federated Neural Pub/Sub (paper Section 4.2.5).

Each broker runs a ``SummaryPropagator`` that periodically pushes its local
subscription summary to all known federation peers. Incoming summaries from
peers are stored and made available to the routing protocol (Section 4.2.3).

The propagation loop runs as an asyncio task and uses ``httpx`` for async
HTTP POST requests. Each peer is expected to expose an endpoint that accepts
msgpack-encoded ``SubscriptionSummary`` payloads (see ``summary.serialize``).

Wire format: POST /federation/summary with Content-Type application/x-msgpack.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from .summary import SubscriptionSummary, serialize, deserialize

logger = logging.getLogger(__name__)


class SummaryPropagator:
    """Periodic summary propagation service.

    Maintains a cache of the latest summary received from each peer and
    pushes the local summary to all peers on a configurable interval.

    Args:
        domain_id: Identifier of the local domain (broker).
        peers: List of peer base URLs (e.g., ``["http://broker-b:8000"]``).
        interval_seconds: Seconds between propagation rounds.
        timeout_seconds: HTTP request timeout per peer.
    """

    def __init__(
        self,
        domain_id: str,
        peers: list[str],
        interval_seconds: float = 10.0,
        timeout_seconds: float = 5.0,
        max_peer_failures: int = 3,
    ):
        self.domain_id = domain_id
        self.peers = list(peers)
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self.max_peer_failures = max_peer_failures
        self.recovery_probe_interval: int = 5  # probe unhealthy peers every N rounds

        # Latest summary from each peer, keyed by domain_id
        self._peer_summaries: dict[str, SubscriptionSummary] = {}
        # The local summary to push
        self._local_summary: SubscriptionSummary | None = None
        # Async task handle
        self._task: asyncio.Task[None] | None = None
        self._running = False

        # Peer health tracking: consecutive push failures and health status
        self._peer_failures: dict[str, int] = {}   # peer_url -> consecutive failure count
        self._peer_healthy: dict[str, bool] = {}    # peer_url -> is_healthy
        self._peer_skip_counter: dict[str, int] = {}  # peer_url -> rounds skipped since unhealthy

        # Propagation latency tracking (ms per push round)
        self._propagation_latencies: list[float] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the periodic propagation loop.

        The loop runs until ``stop()`` is called. Each iteration pushes
        the current local summary to every peer.
        """
        if self._running:
            logger.warning("Propagator already running for domain %s", self.domain_id)
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Propagator started for domain %s (interval=%.1fs, peers=%d)",
            self.domain_id,
            self.interval_seconds,
            len(self.peers),
        )

    async def stop(self) -> None:
        """Stop the propagation loop gracefully."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Propagator stopped for domain %s", self.domain_id)

    # ------------------------------------------------------------------
    # Summary exchange
    # ------------------------------------------------------------------

    async def push_summary(self, summary: SubscriptionSummary) -> None:
        """Push a local summary to healthy federation peers.

        Sends the serialised summary via HTTP POST to each healthy peer's
        ``/federation/summary`` endpoint. Unhealthy peers are skipped but
        receive periodic recovery probes (every ``recovery_probe_interval``
        rounds) to detect when they come back online.

        Failures are logged but do not interrupt propagation to other peers.

        Args:
            summary: The local domain's current subscription summary.
        """
        t_start = time.time()
        data = serialize(summary)
        eligible_peers = self._select_eligible_peers()
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            tasks = [
                self._send_to_peer(client, peer_url, data)
                for peer_url in eligible_peers
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        t_end = time.time()
        self._propagation_latencies.append((t_end - t_start) * 1000.0)

    def _select_eligible_peers(self) -> list[str]:
        """Return peers eligible for this push round.

        Healthy peers are always included. Unhealthy peers are included
        only on recovery probe rounds (every ``recovery_probe_interval``
        skipped rounds).
        """
        eligible: list[str] = []
        for peer_url in self.peers:
            if self.is_peer_healthy(peer_url):
                eligible.append(peer_url)
            else:
                # Increment skip counter; probe when it reaches the interval
                count = self._peer_skip_counter.get(peer_url, 0) + 1
                if count >= self.recovery_probe_interval:
                    eligible.append(peer_url)
                    self._peer_skip_counter[peer_url] = 0
                    logger.debug(
                        "Recovery probe for unhealthy peer %s", peer_url
                    )
                else:
                    self._peer_skip_counter[peer_url] = count
        return eligible

    async def receive_summary(self, summary: SubscriptionSummary) -> None:
        """Process an incoming summary from a federation peer.

        Stores the summary in the peer cache, replacing any previous
        summary from the same domain. Enforces timestamp-based freshness
        checks (Section 4.2.5): a summary is only accepted if its
        timestamp >= the stored timestamp. Summaries with timestamp == 0.0
        are treated as legacy (no freshness data) and always accepted.

        Args:
            summary: A subscription summary received from a peer.
        """
        existing = self._peer_summaries.get(summary.domain_id)
        if existing is not None:
            # Freshness check: reject strictly older summaries.
            # timestamp == 0.0 means legacy/missing -- accept unconditionally.
            if (
                summary.timestamp != 0.0
                and existing.timestamp != 0.0
                and summary.timestamp < existing.timestamp
            ):
                logger.debug(
                    "Rejected stale summary from domain %s "
                    "(incoming ts=%.1f < stored ts=%.1f)",
                    summary.domain_id,
                    summary.timestamp,
                    existing.timestamp,
                )
                return

        self._peer_summaries[summary.domain_id] = summary
        logger.debug(
            "Received summary from domain %s (%d clusters, ts=%.1f)",
            summary.domain_id,
            len(summary.clusters),
            summary.timestamp,
        )

    def get_peer_summaries(self) -> dict[str, SubscriptionSummary]:
        """Return the latest summary from each known peer.

        Returns:
            Mapping from peer domain_id to its most recent
            ``SubscriptionSummary``.
        """
        return dict(self._peer_summaries)

    @property
    def local_summary(self) -> SubscriptionSummary | None:
        """The current local subscription summary, or None if not yet set."""
        return self._local_summary

    def propagation_latencies_ms(self) -> list[float]:
        """Return the recorded per-round propagation latencies in milliseconds."""
        return list(self._propagation_latencies)

    def update_local_summary(self, summary: SubscriptionSummary) -> None:
        """Set the local summary that will be pushed in the next propagation round.

        Args:
            summary: The updated local subscription summary.
        """
        self._local_summary = summary

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Propagation loop: push local summary to peers on each tick."""
        while self._running:
            try:
                if self._local_summary is not None:
                    await self.push_summary(self._local_summary)
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "Error in propagation loop for domain %s", self.domain_id
                )
                await asyncio.sleep(self.interval_seconds)

    def is_peer_healthy(self, peer_url: str) -> bool:
        """Return whether a peer is considered healthy.

        Peers are healthy by default; they become unhealthy after
        ``max_peer_failures`` consecutive push failures and recover
        when a push succeeds.
        """
        return self._peer_healthy.get(peer_url, True)

    async def _send_to_peer(
        self,
        client: httpx.AsyncClient,
        peer_url: str,
        data: bytes,
    ) -> None:
        """Send serialised summary to a single peer, tracking health."""
        url = f"{peer_url.rstrip('/')}/federation/summary"
        try:
            resp = await client.post(
                url,
                content=data,
                headers={"Content-Type": "application/x-msgpack"},
            )
            if resp.status_code >= 400:
                logger.warning(
                    "Peer %s returned HTTP %d", peer_url, resp.status_code
                )
                self._record_peer_failure(peer_url)
            else:
                # Success: reset failure count, skip counter, and mark healthy
                self._peer_failures.pop(peer_url, None)
                self._peer_skip_counter.pop(peer_url, None)
                if not self._peer_healthy.get(peer_url, True):
                    logger.info("Peer %s recovered; marking healthy.", peer_url)
                self._peer_healthy[peer_url] = True
        except httpx.HTTPError as exc:
            logger.warning("Failed to push summary to %s: %s", peer_url, exc)
            self._record_peer_failure(peer_url)

    def _record_peer_failure(self, peer_url: str) -> None:
        """Increment failure count for a peer; mark unhealthy if threshold reached."""
        count = self._peer_failures.get(peer_url, 0) + 1
        self._peer_failures[peer_url] = count
        if count >= self.max_peer_failures:
            if self._peer_healthy.get(peer_url, True):
                logger.warning(
                    "Peer %s marked unhealthy after %d consecutive failures. "
                    "Using last cached summary (stale).",
                    peer_url,
                    count,
                )
            self._peer_healthy[peer_url] = False
