"""Unit tests for failure injection (src/measurement/failure.py).

Uses mock Docker client to test injection logic without requiring a running
Docker daemon.
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from src.measurement.failure import FailureConfig, FailureInjector
from src.measurement.harness import AdaptationTracker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tracker():
    return AdaptationTracker()


@pytest.fixture
def mock_docker_client():
    """Create a mock Docker client with containers and networks."""
    client = MagicMock()

    # Mock containers
    containers = {}
    for name in [
        "worker-d1-nearrt-1", "worker-d1-nearrt-2", "worker-d1-edge-1",
        "worker-d2-nearrt-1", "worker-d2-edge-1",
        "broker-d1", "broker-d2",
    ]:
        container = MagicMock()
        container.id = f"sha256-{name}-fake-id"
        container.name = name
        container.kill = MagicMock()
        container.restart = MagicMock()
        containers[name] = container

    def get_container(name):
        if name in containers:
            return containers[name]
        # Try Compose-prefixed name
        from docker.errors import NotFound
        raise NotFound(f"Container not found: {name}")

    client.containers.get = MagicMock(side_effect=get_container)
    client.containers.list = MagicMock(return_value=[])

    # Mock networks
    networks = {}
    for name in [
        "federation", "slice-nearrt-d1", "slice-edge-d1",
        "slice-nearrt-d2", "slice-edge-d2",
    ]:
        network = MagicMock()
        network.name = name
        network.disconnect = MagicMock()
        network.connect = MagicMock()
        networks[name] = network

    def get_network(name):
        if name in networks:
            return networks[name]
        from docker.errors import NotFound
        raise NotFound(f"Network not found: {name}")

    client.networks.get = MagicMock(side_effect=get_network)
    client.networks.list = MagicMock(return_value=[])

    # Attach containers and networks dicts for assertions
    client._mock_containers = containers
    client._mock_networks = networks

    return client


@pytest.fixture
def injector(mock_docker_client, tracker):
    return FailureInjector(
        client=mock_docker_client,
        tracker=tracker,
        compose_project="neural-pubsub",
    )


# ---------------------------------------------------------------------------
# test_kill_worker
# ---------------------------------------------------------------------------

def test_kill_worker(injector, mock_docker_client, tracker):
    """Killing a worker calls Container.kill() and records a worker_kill event."""
    asyncio.get_event_loop().run_until_complete(
        injector.kill_worker("worker-d1-nearrt-1")
    )
    # Container.kill() was called
    mock_docker_client._mock_containers["worker-d1-nearrt-1"].kill.assert_called_once()
    # Tracker recorded failure
    events = tracker.all_events()
    assert len(events) == 1
    assert events[0].failure_type == "worker_kill"
    assert events[0].target_id == "worker-d1-nearrt-1"
    assert events[0].is_recovery is False


# ---------------------------------------------------------------------------
# test_restart_worker
# ---------------------------------------------------------------------------

def test_restart_worker(injector, mock_docker_client, tracker):
    """Restarting a previously killed worker records a recovery event with positive adaptation time."""
    loop = asyncio.get_event_loop()
    # Must kill first
    loop.run_until_complete(injector.kill_worker("worker-d1-nearrt-1"))
    loop.run_until_complete(injector.restart_worker("worker-d1-nearrt-1"))

    # Container.restart() was called
    mock_docker_client._mock_containers["worker-d1-nearrt-1"].restart.assert_called_once()
    # Tracker has both failure and recovery
    events = tracker.all_events()
    assert len(events) == 2
    assert events[1].is_recovery is True
    # Adaptation time should be positive
    times = tracker.adaptation_times_ms()
    assert len(times) == 1
    assert times[0] >= 0


# ---------------------------------------------------------------------------
# test_restart_worker_not_killed_raises
# ---------------------------------------------------------------------------

def test_restart_worker_not_killed_raises(injector):
    """Restarting a worker that was never killed raises KeyError."""
    with pytest.raises(KeyError, match="not killed"):
        asyncio.get_event_loop().run_until_complete(
            injector.restart_worker("worker-d1-nearrt-1")
        )


# ---------------------------------------------------------------------------
# test_kill_broker
# ---------------------------------------------------------------------------

def test_kill_broker(injector, mock_docker_client, tracker):
    """Killing a broker records a broker_kill event (distinct from worker_kill)."""
    asyncio.get_event_loop().run_until_complete(
        injector.kill_broker("broker-d1")
    )
    mock_docker_client._mock_containers["broker-d1"].kill.assert_called_once()
    events = tracker.all_events()
    assert len(events) == 1
    assert events[0].failure_type == "broker_kill"
    assert events[0].target_id == "broker-d1"


# ---------------------------------------------------------------------------
# test_network_partition
# ---------------------------------------------------------------------------

def test_network_partition(injector, mock_docker_client, tracker):
    """Partitioning a network calls Network.disconnect() and records a network_partition event."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        injector.partition_network("broker-d2", "federation")
    )
    mock_docker_client._mock_networks["federation"].disconnect.assert_called_once()
    events = tracker.all_events()
    assert len(events) == 1
    assert events[0].failure_type == "network_partition"
    assert events[0].target_id == "broker-d2:federation"


# ---------------------------------------------------------------------------
# test_heal_partition
# ---------------------------------------------------------------------------

def test_heal_partition(injector, mock_docker_client, tracker):
    """Healing a partition calls Network.connect() and records a recovery event with adaptation time."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        injector.partition_network("broker-d2", "federation")
    )
    loop.run_until_complete(
        injector.heal_partition("broker-d2", "federation")
    )

    mock_docker_client._mock_networks["federation"].connect.assert_called_once()
    events = tracker.all_events()
    assert len(events) == 2
    assert events[1].failure_type == "network_partition"
    assert events[1].is_recovery is True
    times = tracker.adaptation_times_ms()
    assert len(times) == 1


# ---------------------------------------------------------------------------
# test_partial_input_failure
# ---------------------------------------------------------------------------

def test_partial_input_failure(injector, mock_docker_client, tracker):
    """Killing multiple workers records individual kills plus a composite partial_input event."""
    loop = asyncio.get_event_loop()
    targets = ["worker-d1-nearrt-1", "worker-d1-nearrt-2"]
    loop.run_until_complete(injector.kill_partial_inputs(targets))

    # Both containers killed
    for name in targets:
        mock_docker_client._mock_containers[name].kill.assert_called_once()

    # Tracker has worker_kill events + a composite partial_input event
    events = tracker.all_events()
    partial_events = [e for e in events if e.failure_type == "partial_input"]
    assert len(partial_events) == 1
    assert "worker-d1-nearrt-1" in partial_events[0].target_id
    assert "worker-d1-nearrt-2" in partial_events[0].target_id


# ---------------------------------------------------------------------------
# test_run_scenario_worker_kill
# ---------------------------------------------------------------------------

def test_run_scenario_worker_kill(injector, mock_docker_client, tracker):
    """Full worker_kill scenario: kill then auto-restart, producing both failure and recovery events."""
    config = FailureConfig(
        failure_type="worker_kill",
        target_id="worker-d1-edge-1",
        delay_s=0,
        duration_s=0.01,  # Very short for testing
    )
    asyncio.get_event_loop().run_until_complete(injector.run_scenario(config))

    # Worker should have been killed and then restarted
    mock_docker_client._mock_containers["worker-d1-edge-1"].kill.assert_called_once()
    mock_docker_client._mock_containers["worker-d1-edge-1"].restart.assert_called_once()

    events = tracker.all_events()
    assert any(e.failure_type == "worker_kill" and not e.is_recovery for e in events)
    assert any(e.failure_type == "worker_kill" and e.is_recovery for e in events)


# ---------------------------------------------------------------------------
# test_run_scenario_network_partition
# ---------------------------------------------------------------------------

def test_run_scenario_network_partition(injector, mock_docker_client, tracker):
    """Full network_partition scenario: disconnect then reconnect the federation network."""
    config = FailureConfig(
        failure_type="network_partition",
        target_id="broker-d2",
        delay_s=0,
        duration_s=0.01,
        extra={"container": "broker-d2", "network": "federation"},
    )
    asyncio.get_event_loop().run_until_complete(injector.run_scenario(config))

    mock_docker_client._mock_networks["federation"].disconnect.assert_called_once()
    mock_docker_client._mock_networks["federation"].connect.assert_called_once()


# ---------------------------------------------------------------------------
# test_run_scenario_unknown_type_raises
# ---------------------------------------------------------------------------

def test_run_scenario_unknown_type_raises(injector):
    """An unrecognised failure_type in FailureConfig raises ValueError."""
    config = FailureConfig(failure_type="unknown_type", target_id="x")
    with pytest.raises(ValueError, match="Unknown failure type"):
        asyncio.get_event_loop().run_until_complete(
            injector.run_scenario(config)
        )


# ---------------------------------------------------------------------------
# test_active_failures
# ---------------------------------------------------------------------------

def test_active_failures(injector, mock_docker_client):
    """active_failures dict tracks killed containers and partitions, clearing them on recovery."""
    loop = asyncio.get_event_loop()
    assert injector.active_failures == {
        "killed_containers": [],
        "partitions": [],
    }

    # Kill a worker
    loop.run_until_complete(injector.kill_worker("worker-d1-nearrt-1"))
    assert "worker-d1-nearrt-1" in injector.active_failures["killed_containers"]

    # Partition a network
    loop.run_until_complete(
        injector.partition_network("broker-d2", "federation")
    )
    assert len(injector.active_failures["partitions"]) == 1

    # Restart worker
    loop.run_until_complete(injector.restart_worker("worker-d1-nearrt-1"))
    assert "worker-d1-nearrt-1" not in injector.active_failures["killed_containers"]


# ---------------------------------------------------------------------------
# test_cleanup
# ---------------------------------------------------------------------------

def test_cleanup(injector, mock_docker_client):
    """cleanup() restores all killed containers and healed partitions, leaving active_failures empty."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(injector.kill_worker("worker-d1-nearrt-1"))
    loop.run_until_complete(injector.kill_broker("broker-d2"))
    loop.run_until_complete(
        injector.partition_network("broker-d1", "federation")
    )

    # Cleanup should restore everything
    loop.run_until_complete(injector.cleanup())

    assert injector.active_failures == {
        "killed_containers": [],
        "partitions": [],
    }
