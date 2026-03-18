"""Failure injection for Neural Pub/Sub resilience experiments.

Implements controlled failure scenarios for Phase D experiments (Section 4.4):

- **Worker failure** (D1): Kill a worker container to trigger stage re-placement.
- **Broker failure** (D2): Kill a domain broker to trigger proxy recovery.
- **Network partition** (D3): Disconnect a Docker network to simulate link failure.
- **Partial input failure** (D4): Kill a subset of funnel inputs to test
  configurable wait/proceed/abort policies.

All operations use the Docker Engine API via the ``docker`` Python SDK.
Each injector records failure and recovery events through an
:class:`~src.measurement.harness.AdaptationTracker` for post-hoc analysis.

Usage::

    tracker = AdaptationTracker()
    injector = FailureInjector.from_compose(
        compose_project="neural-pubsub",
        tracker=tracker,
    )
    await injector.kill_worker("d1-nearrt-1")
    await asyncio.sleep(30)
    await injector.restart_worker("d1-nearrt-1")
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

try:
    import docker
    from docker.errors import APIError, NotFound
except ImportError:
    docker = None  # type: ignore[assignment]

from src.measurement.harness import AdaptationTracker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class FailureConfig:
    """Configuration for a failure injection scenario.

    Attributes:
        failure_type: One of 'worker_kill', 'broker_kill', 'network_partition',
            'partial_input'.
        target_id: Container name or network name to target.
        delay_s: Seconds to wait before injecting the failure (from scenario
            start). Allows scheduling failures mid-experiment.
        duration_s: Seconds the failure lasts before automatic recovery.
            If None, recovery must be triggered manually.
        extra: Additional parameters for specific failure types. For
            'network_partition': {'container': '<name>', 'network': '<name>'}.
            For 'partial_input': {'targets': ['sensor_0', 'sensor_1']}.
    """

    failure_type: str
    target_id: str
    delay_s: float = 0.0
    duration_s: Optional[float] = None
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# FailureInjector
# ---------------------------------------------------------------------------


class FailureInjector:
    """Controls Docker container and network operations for failure injection.

    Wraps the Docker Engine API to provide high-level failure primitives.
    All operations are async-compatible (run blocking Docker calls in a
    thread executor) and integrate with
    :class:`~src.measurement.harness.AdaptationTracker`.

    Args:
        client: A ``docker.DockerClient`` instance.
        tracker: An :class:`AdaptationTracker` for recording failure/recovery
            events with precise timestamps.
        compose_project: Docker Compose project name prefix for container
            lookups. Defaults to ``"neural-pubsub"``.
    """

    def __init__(
        self,
        client: docker.DockerClient,  # type: ignore[name-defined]
        tracker: AdaptationTracker,
        compose_project: str = "neural-pubsub",
    ) -> None:
        self._client = client
        self._tracker = tracker
        self._project = compose_project
        # Track partitioned (container, network) pairs for cleanup
        self._partitions: list[tuple[str, str]] = []
        # Track killed containers for restart
        self._killed: dict[str, str] = {}  # container_name -> container_id

    @classmethod
    def from_compose(
        cls,
        compose_project: str = "neural-pubsub",
        tracker: Optional[AdaptationTracker] = None,
    ) -> "FailureInjector":
        """Create a FailureInjector connected to the local Docker daemon.

        Args:
            compose_project: Docker Compose project name for container lookups.
            tracker: Optional AdaptationTracker. If None, a new one is created.

        Returns:
            A configured FailureInjector.

        Raises:
            ImportError: If the ``docker`` package is not installed.
            docker.errors.DockerException: If the Docker daemon is unreachable.
        """
        if docker is None:
            raise ImportError(
                "The 'docker' package is required for failure injection. "
                "Install it with: pip install docker"
            )
        client = docker.from_env()
        if tracker is None:
            tracker = AdaptationTracker()
        return cls(client=client, tracker=tracker, compose_project=compose_project)

    # ------------------------------------------------------------------
    # Container lookup
    # ------------------------------------------------------------------

    def _find_container(self, name: str) -> docker.models.containers.Container:  # type: ignore[name-defined]
        """Find a Docker container by name, with Compose project prefix.

        Tries exact name first, then with the Compose project prefix.

        Args:
            name: Container name (e.g. 'broker-d1' or 'neural-pubsub-broker-d1-1').

        Returns:
            The Docker container object.

        Raises:
            NotFound: If no matching container exists.
        """
        # Try exact name first
        try:
            return self._client.containers.get(name)
        except NotFound:
            pass

        # Try with Compose project prefix (e.g. "neural-pubsub-broker-d1-1")
        prefixed = f"{self._project}-{name}-1"
        try:
            return self._client.containers.get(prefixed)
        except NotFound:
            pass

        # Try listing with label filter
        containers = self._client.containers.list(
            filters={
                "label": f"com.docker.compose.project={self._project}",
                "name": name,
            }
        )
        if containers:
            return containers[0]

        raise NotFound(f"Container '{name}' not found (project: {self._project})")

    def _find_network(self, name: str) -> docker.models.networks.Network:  # type: ignore[name-defined]
        """Find a Docker network by name, with Compose project prefix.

        Args:
            name: Network name (e.g. 'federation' or 'neural-pubsub_federation').

        Returns:
            The Docker network object.

        Raises:
            NotFound: If no matching network exists.
        """
        # Try exact name
        try:
            return self._client.networks.get(name)
        except NotFound:
            pass

        # Try with Compose project prefix
        prefixed = f"{self._project}_{name}"
        try:
            return self._client.networks.get(prefixed)
        except NotFound:
            pass

        # Try listing
        networks = self._client.networks.list(names=[name])
        if networks:
            return networks[0]
        networks = self._client.networks.list(names=[prefixed])
        if networks:
            return networks[0]

        raise NotFound(f"Network '{name}' not found (project: {self._project})")

    # ------------------------------------------------------------------
    # Worker failure (Phase D, Test D1)
    # ------------------------------------------------------------------

    async def kill_worker(self, worker_name: str) -> None:
        """Kill a worker container to simulate execution unit failure.

        Sends SIGKILL to the worker container. The broker should detect the
        health check failure and re-place affected pipeline stages on surviving
        workers (Section 4.4.2).

        Args:
            worker_name: Name of the worker container to kill.
        """
        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None, self._find_container, worker_name
        )
        container_id = container.id

        logger.info("Killing worker container: %s (%s)", worker_name, container_id[:12])
        await loop.run_in_executor(None, container.kill)

        self._killed[worker_name] = container_id
        self._tracker.record_failure("worker_kill", worker_name, time.time())

    async def restart_worker(self, worker_name: str) -> None:
        """Restart a previously killed worker container.

        Args:
            worker_name: Name of the worker container to restart.

        Raises:
            KeyError: If the worker was not previously killed via this injector.
        """
        if worker_name not in self._killed:
            raise KeyError(
                f"Worker '{worker_name}' was not killed by this injector. "
                f"Known killed workers: {list(self._killed.keys())}"
            )

        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None, self._find_container, worker_name
        )

        logger.info("Restarting worker container: %s", worker_name)
        await loop.run_in_executor(None, container.restart)

        del self._killed[worker_name]
        self._tracker.record_recovery("worker_kill", worker_name, time.time())

    # ------------------------------------------------------------------
    # Broker failure (Phase D, Test D2)
    # ------------------------------------------------------------------

    async def kill_broker(self, broker_name: str) -> None:
        """Kill a broker container to simulate domain broker failure.

        The peer broker should detect the failure (via missing heartbeats)
        and activate proxy recovery (Section 4.4.1), serving cached
        subscription summaries for the failed domain.

        Args:
            broker_name: Name of the broker container to kill.
        """
        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None, self._find_container, broker_name
        )
        container_id = container.id

        logger.info("Killing broker container: %s (%s)", broker_name, container_id[:12])
        await loop.run_in_executor(None, container.kill)

        self._killed[broker_name] = container_id
        self._tracker.record_failure("broker_kill", broker_name, time.time())

    async def restart_broker(self, broker_name: str) -> None:
        """Restart a previously killed broker container.

        Args:
            broker_name: Name of the broker container to restart.
        """
        if broker_name not in self._killed:
            raise KeyError(
                f"Broker '{broker_name}' was not killed by this injector. "
                f"Known killed: {list(self._killed.keys())}"
            )

        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None, self._find_container, broker_name
        )

        logger.info("Restarting broker container: %s", broker_name)
        await loop.run_in_executor(None, container.restart)

        del self._killed[broker_name]
        self._tracker.record_recovery("broker_kill", broker_name, time.time())

    # ------------------------------------------------------------------
    # Network partition (Phase D, Test D3)
    # ------------------------------------------------------------------

    async def partition_network(
        self, container_name: str, network_name: str
    ) -> None:
        """Disconnect a container from a network to simulate a link failure.

        Removes the container from the specified Docker network, breaking
        connectivity to all other containers on that network. Used to
        simulate inter-site link failures (Tokyo-Oulu WAN link down).

        Args:
            container_name: Container to disconnect.
            network_name: Network to disconnect from.
        """
        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None, self._find_container, container_name
        )
        network = await loop.run_in_executor(
            None, self._find_network, network_name
        )

        logger.info(
            "Partitioning: disconnecting %s from network %s",
            container_name,
            network_name,
        )
        await loop.run_in_executor(None, network.disconnect, container)

        self._partitions.append((container_name, network_name))
        target_id = f"{container_name}:{network_name}"
        self._tracker.record_failure("network_partition", target_id, time.time())

    async def heal_partition(
        self, container_name: str, network_name: str
    ) -> None:
        """Reconnect a container to a network after a simulated partition.

        Args:
            container_name: Container to reconnect.
            network_name: Network to reconnect to.
        """
        loop = asyncio.get_event_loop()
        container = await loop.run_in_executor(
            None, self._find_container, container_name
        )
        network = await loop.run_in_executor(
            None, self._find_network, network_name
        )

        logger.info(
            "Healing partition: reconnecting %s to network %s",
            container_name,
            network_name,
        )
        await loop.run_in_executor(None, network.connect, container)

        pair = (container_name, network_name)
        if pair in self._partitions:
            self._partitions.remove(pair)

        target_id = f"{container_name}:{network_name}"
        self._tracker.record_recovery("network_partition", target_id, time.time())

    # ------------------------------------------------------------------
    # Partial input failure (Phase D, Test D4)
    # ------------------------------------------------------------------

    async def kill_partial_inputs(
        self, worker_names: list[str]
    ) -> None:
        """Kill a subset of worker containers feeding a funnel stage.

        Simulates partial sensor/input failure in a sensor-fusion or funnel
        pipeline. The broker's funnel stage should apply its configurable
        policy: wait (with timeout), proceed with partial data, or abort
        the pipeline (Section 4.4.3).

        Args:
            worker_names: List of worker container names to kill.
        """
        for name in worker_names:
            await self.kill_worker(name)
            # Re-tag the failure type for partial-input tracking
            # (kill_worker already recorded it as worker_kill)
        # Also record a composite partial_input failure event
        target_id = ",".join(sorted(worker_names))
        self._tracker.record_failure("partial_input", target_id, time.time())

    async def restart_partial_inputs(
        self, worker_names: list[str]
    ) -> None:
        """Restart workers that were killed as partial inputs.

        Args:
            worker_names: List of worker container names to restart.
        """
        for name in worker_names:
            await self.restart_worker(name)
        target_id = ",".join(sorted(worker_names))
        self._tracker.record_recovery("partial_input", target_id, time.time())

    # ------------------------------------------------------------------
    # Scenario execution
    # ------------------------------------------------------------------

    async def run_scenario(self, config: FailureConfig) -> None:
        """Execute a single failure scenario according to a FailureConfig.

        Waits ``config.delay_s`` seconds, injects the failure, then
        optionally waits ``config.duration_s`` seconds and triggers
        automatic recovery.

        Args:
            config: The failure scenario configuration.

        Raises:
            ValueError: If the failure_type is not recognised.
        """
        if config.delay_s > 0:
            logger.info(
                "Scheduling %s on %s in %.1fs",
                config.failure_type,
                config.target_id,
                config.delay_s,
            )
            await asyncio.sleep(config.delay_s)

        logger.info(
            "Injecting %s on %s", config.failure_type, config.target_id
        )

        if config.failure_type == "worker_kill":
            await self.kill_worker(config.target_id)
        elif config.failure_type == "broker_kill":
            await self.kill_broker(config.target_id)
        elif config.failure_type == "network_partition":
            container = config.extra.get("container", config.target_id)
            network = config.extra.get("network", "federation")
            await self.partition_network(container, network)
        elif config.failure_type == "partial_input":
            targets = config.extra.get("targets", [config.target_id])
            await self.kill_partial_inputs(targets)
        else:
            raise ValueError(f"Unknown failure type: {config.failure_type}")

        if config.duration_s is not None:
            logger.info(
                "Failure active for %.1fs, then auto-recovering", config.duration_s
            )
            await asyncio.sleep(config.duration_s)
            await self._auto_recover(config)

    async def _auto_recover(self, config: FailureConfig) -> None:
        """Automatically recover from a failure after the configured duration."""
        logger.info(
            "Auto-recovering %s on %s", config.failure_type, config.target_id
        )

        if config.failure_type == "worker_kill":
            await self.restart_worker(config.target_id)
        elif config.failure_type == "broker_kill":
            await self.restart_broker(config.target_id)
        elif config.failure_type == "network_partition":
            container = config.extra.get("container", config.target_id)
            network = config.extra.get("network", "federation")
            await self.heal_partition(container, network)
        elif config.failure_type == "partial_input":
            targets = config.extra.get("targets", [config.target_id])
            await self.restart_partial_inputs(targets)

    async def run_scenarios(self, configs: list[FailureConfig]) -> None:
        """Execute multiple failure scenarios concurrently.

        Each scenario runs as an independent asyncio task, respecting its
        own ``delay_s`` and ``duration_s`` timing.

        Args:
            configs: List of failure configurations to execute.
        """
        tasks = [asyncio.create_task(self.run_scenario(c)) for c in configs]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def cleanup(self) -> None:
        """Restore all injected failures: restart killed containers and
        heal all network partitions.

        Safe to call even if some resources have already been restored.
        """
        loop = asyncio.get_event_loop()

        # Heal all network partitions
        for container_name, network_name in list(self._partitions):
            try:
                await self.heal_partition(container_name, network_name)
            except (NotFound, APIError) as e:
                logger.warning(
                    "Could not heal partition %s:%s: %s",
                    container_name,
                    network_name,
                    e,
                )

        # Restart all killed containers
        for name in list(self._killed):
            try:
                container = await loop.run_in_executor(
                    None, self._find_container, name
                )
                await loop.run_in_executor(None, container.restart)
                logger.info("Cleaned up: restarted %s", name)
                del self._killed[name]
            except (NotFound, APIError) as e:
                logger.warning("Could not restart %s: %s", name, e)

    @property
    def active_failures(self) -> dict:
        """Return a summary of currently active (unrecovered) failures.

        Returns:
            Dictionary with 'killed_containers' and 'partitions' keys.
        """
        return {
            "killed_containers": list(self._killed.keys()),
            "partitions": [
                {"container": c, "network": n} for c, n in self._partitions
            ],
        }
