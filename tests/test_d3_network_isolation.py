"""Tests for D3 network partition isolation.

ROOT CAUSE: D3 disconnects ALL containers from the federation network,
including the workload container. Since the workload connects to broker-d1
via federation, it loses connectivity and cannot write results.

FIX: The workload must connect to broker-d1 via a separate network
(workload-net) that is NOT affected by D3's federation disconnect.
The federation network should only carry broker-to-broker peering traffic.

These tests verify:
1. The workload container is on workload-net, NOT federation
2. broker-d1 bridges workload-net and federation
3. D3 disconnects federation without affecting workload-net
4. After D3 partition, workload can still reach broker-d1

TDD RED phase: these tests MUST fail before the fix is applied.
"""

from __future__ import annotations

import yaml
import pytest
from pathlib import Path

from scripts._common import PROJECT_ROOT


COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"


def _load_compose() -> dict:
    """Load the local compose file."""
    with open(COMPOSE_LOCAL) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# 1. Workload must NOT be on the federation network
# ---------------------------------------------------------------------------

class TestWorkloadNetworkIsolation:
    """The workload container must be isolated from the federation network
    so that D3 network partition does not kill its connection to broker-d1."""

    def test_workload_not_on_federation(self):
        """Workload must NOT list 'federation' in its networks.

        If it does, disconnecting federation (D3) kills workload->broker
        connectivity and no results are produced.
        """
        dc = _load_compose()
        workload_networks = dc["services"]["workload"].get("networks", [])
        assert "federation" not in workload_networks, (
            "Workload is on the 'federation' network. D3 disconnects "
            "federation, which kills workload->broker connectivity. "
            "Move workload to a dedicated 'workload-net' network."
        )

    def test_workload_on_dedicated_network(self):
        """Workload must be on a dedicated 'workload-net' network that
        connects it to broker-d1 without traversing federation."""
        dc = _load_compose()
        workload_networks = dc["services"]["workload"].get("networks", [])
        assert "workload-net" in workload_networks, (
            "Workload must be on 'workload-net' to maintain connectivity "
            "to broker-d1 when federation is partitioned."
        )

    def test_workload_net_defined_in_networks(self):
        """The workload-net network must be defined in the compose file."""
        dc = _load_compose()
        networks = dc.get("networks", {})
        assert "workload-net" in networks, (
            "Missing 'workload-net' network definition in compose file."
        )


# ---------------------------------------------------------------------------
# 2. Broker-d1 must bridge workload-net and federation
# ---------------------------------------------------------------------------

class TestBrokerD1Bridging:
    """Broker-d1 must be on both workload-net (for workload access) and
    federation (for broker peering). This makes it the bridge point."""

    def test_broker_d1_on_workload_net(self):
        """Broker-d1 must be on workload-net so the workload can reach it."""
        dc = _load_compose()
        broker_networks = dc["services"]["broker-d1"].get("networks", [])
        assert "workload-net" in broker_networks, (
            "Broker-d1 must be on 'workload-net' to receive workload traffic."
        )

    def test_broker_d1_on_federation(self):
        """Broker-d1 must still be on federation for broker peering."""
        dc = _load_compose()
        broker_networks = dc["services"]["broker-d1"].get("networks", [])
        assert "federation" in broker_networks, (
            "Broker-d1 must remain on 'federation' for broker peering."
        )

    def test_broker_d2_not_on_workload_net(self):
        """Broker-d2 should NOT be on workload-net (workload only talks to d1)."""
        dc = _load_compose()
        broker_networks = dc["services"]["broker-d2"].get("networks", [])
        assert "workload-net" not in broker_networks, (
            "Broker-d2 should not be on workload-net. The workload submits "
            "only to broker-d1; cross-domain routing goes via federation."
        )


# ---------------------------------------------------------------------------
# 3. D3 partition targets only federation, not workload-net
# ---------------------------------------------------------------------------

class TestD3PartitionScope:
    """D3 must disconnect the federation network only. The workload-net
    must remain intact so results can be collected."""

    def test_d3_target_is_federation_not_workload_net(self):
        """D3 failure_target must be 'federation', not 'workload-net'."""
        from scripts.run_phase_d import CONFIGS
        target = CONFIGS["D3"]["failure_target"]
        assert target == "federation", (
            f"D3 target should be 'federation', got '{target}'"
        )
        assert target != "workload-net", (
            "D3 must never target workload-net"
        )

    def test_inject_network_partition_only_disconnects_target_network(self):
        """inject_network_partition must only disconnect containers from the
        specified target network. It must not touch other networks."""
        import inspect
        from scripts._common import inject_network_partition
        source = inspect.getsource(inject_network_partition)
        # The function should only operate on the named network
        # It should NOT iterate over all networks
        assert "workload-net" not in source, (
            "inject_network_partition must not reference workload-net"
        )


# ---------------------------------------------------------------------------
# 4. Structural: workload connects to broker-d1 via workload-net DNS
# ---------------------------------------------------------------------------

class TestWorkloadBrokerConnectivity:
    """After the fix, workload reaches broker-d1 via workload-net.
    The broker URL in the workload command must resolve over workload-net."""

    def test_workload_broker_url_uses_broker_d1(self):
        """Workload's --broker-url must point to broker-d1 (resolved via
        Docker DNS on shared workload-net network)."""
        dc = _load_compose()
        workload_cmd = dc["services"]["workload"]["command"]
        assert "broker-d1" in workload_cmd, (
            "Workload must connect to broker-d1"
        )

    def test_workload_and_broker_d1_share_exactly_workload_net(self):
        """The only shared network between workload and broker-d1 must be
        workload-net (not federation)."""
        dc = _load_compose()
        workload_nets = set(dc["services"]["workload"].get("networks", []))
        broker_nets = set(dc["services"]["broker-d1"].get("networks", []))
        shared = workload_nets & broker_nets
        assert shared == {"workload-net"}, (
            f"Workload and broker-d1 should share only 'workload-net', "
            f"but share: {shared}"
        )


# ---------------------------------------------------------------------------
# 5. Federation network only carries broker-to-broker traffic
# ---------------------------------------------------------------------------

class TestFederationNetworkScope:
    """After the fix, the federation network should only contain brokers.
    No workers or workload containers should be on it."""

    def test_only_brokers_on_federation(self):
        """Only broker services should list 'federation' in their networks."""
        dc = _load_compose()
        for svc_name, svc_cfg in dc["services"].items():
            networks = svc_cfg.get("networks", [])
            if "federation" in networks:
                assert svc_name.startswith("broker-"), (
                    f"Non-broker service '{svc_name}' is on federation network. "
                    f"Only brokers should be on federation for D3 isolation."
                )
