"""Tests for oracle single-broker deployment mode.

Oracle-global is a centralised upper bound: one broker on VM1 sees all 48
workers across 4 domains. VM2-4 run workers only (no broker), registering
with VM1's broker via WORKER_BROKER_URL.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Config-level: oracle_mode flag in MARKET_CONFIGS
# ---------------------------------------------------------------------------


class TestOracleConfigFlag:
    """oracle-global config carries oracle_mode=True."""

    def test_oracle_config_has_oracle_mode(self):
        from scripts.run_market import MARKET_CONFIGS
        assert MARKET_CONFIGS["oracle-global"].get("oracle_mode") is True

    def test_non_oracle_configs_no_oracle_mode(self):
        from scripts.run_market import MARKET_CONFIGS
        # oracle_mode is used by configs that need single-broker deployment
        oracle_mode_configs = {"oracle-global", "rr-global"}
        for name, cfg in MARKET_CONFIGS.items():
            if name not in oracle_mode_configs:
                assert not cfg.get("oracle_mode"), (
                    f"{name} should not have oracle_mode"
                )

    def test_rr_global_has_oracle_mode(self):
        from scripts.run_market import MARKET_CONFIGS
        assert MARKET_CONFIGS["rr-global"].get("oracle_mode") is True

    def test_rr_global_uses_static_broker(self):
        from scripts.run_market import MARKET_CONFIGS
        assert MARKET_CONFIGS["rr-global"]["broker_module"] == "src.broker.static_broker"

    def test_oracle_config_uses_neural_placement(self):
        from scripts.run_market import MARKET_CONFIGS
        assert MARKET_CONFIGS["oracle-global"]["placement_mode"] == "neural"


# ---------------------------------------------------------------------------
# start_cluster oracle mode: VM1 = full, VM2-4 = workers only
# ---------------------------------------------------------------------------


class TestOracleStartCluster:
    """start_cluster with oracle_mode dispatches correctly per VM."""

    @patch("scripts.multi_vm_runner._exec")
    def test_oracle_mode_vm1_starts_full_compose(self, mock_exec):
        from scripts.multi_vm_runner import start_cluster, VMS
        start_cluster(placement_mode="neural", oracle_mode=True, dry_run=True)
        vm1_calls = [c for c in mock_exec.call_args_list
                     if c[0][0] == VMS[0]]
        assert len(vm1_calls) == 1
        cmd = vm1_calls[0][0][1]
        assert "up -d" in cmd
        assert "PLACEMENT_MODE=neural" in cmd
        # VM1 starts all services (no worker-0 worker-1 ... suffix)
        assert "worker-0 worker-1" not in cmd

    @patch("scripts.multi_vm_runner._exec")
    def test_oracle_mode_vm234_workers_only(self, mock_exec):
        from scripts.multi_vm_runner import start_cluster, VMS
        start_cluster(placement_mode="neural", oracle_mode=True, dry_run=True)
        for i in [1, 2, 3]:
            vm_calls = [c for c in mock_exec.call_args_list
                        if c[0][0] == VMS[i]]
            assert len(vm_calls) == 1, f"VM{i+1} should get exactly 1 exec call"
            cmd = vm_calls[0][0][1]
            # Should list individual worker services (not bare "up -d")
            assert "worker-0" in cmd, f"VM{i+1} should start worker-0"
            assert "worker-11" in cmd, f"VM{i+1} should start worker-11"

    @patch("scripts.multi_vm_runner._exec")
    def test_oracle_mode_workers_point_at_vm1(self, mock_exec):
        from scripts.multi_vm_runner import start_cluster, VMS
        start_cluster(placement_mode="neural", oracle_mode=True, dry_run=True)
        for i in [1, 2, 3]:
            vm_calls = [c for c in mock_exec.call_args_list
                        if c[0][0] == VMS[i]]
            cmd = vm_calls[0][0][1]
            assert f"WORKER_BROKER_URL=http://{VMS[0].ip}:8080" in cmd, (
                f"VM{i+1} workers should register with VM1's broker"
            )

    @patch("scripts.multi_vm_runner._exec")
    def test_oracle_mode_vm1_workers_stay_local(self, mock_exec):
        from scripts.multi_vm_runner import start_cluster, VMS
        start_cluster(placement_mode="neural", oracle_mode=True, dry_run=True)
        vm1_calls = [c for c in mock_exec.call_args_list
                     if c[0][0] == VMS[0]]
        cmd = vm1_calls[0][0][1]
        assert "WORKER_BROKER_URL=http://localhost:8080" in cmd

    @patch("scripts.multi_vm_runner._exec")
    def test_non_oracle_mode_all_vms_start_full(self, mock_exec):
        from scripts.multi_vm_runner import start_cluster, VMS
        start_cluster(placement_mode="market", oracle_mode=False, dry_run=True)
        for i in range(4):
            vm_calls = [c for c in mock_exec.call_args_list
                        if c[0][0] == VMS[i]]
            cmd = vm_calls[0][0][1]
            # Non-oracle: all VMs start full compose (no worker-0 ... suffix)
            assert "worker-0 worker-1" not in cmd


# ---------------------------------------------------------------------------
# run_single passes oracle_mode through
# ---------------------------------------------------------------------------


class TestRunSingleOracleMode:
    """run_single forwards oracle_mode to start_cluster."""

    @patch("scripts.multi_vm_runner.collect_results")
    @patch("scripts.multi_vm_runner.stop_cluster")
    @patch("scripts.multi_vm_runner.teardown_wan_emulation")
    @patch("scripts.multi_vm_runner.setup_wan_emulation")
    @patch("scripts.multi_vm_runner.wait_for_federation", return_value=True)
    @patch("scripts.multi_vm_runner.start_cluster")
    @patch("scripts.multi_vm_runner._exec")
    def test_oracle_mode_forwarded(self, mock_exec, mock_start,
                                   mock_wait, mock_wan_setup,
                                   mock_wan_tear, mock_stop, mock_collect):
        from scripts.multi_vm_runner import run_single
        run_single(
            config="oracle-global", seed=42,
            placement_mode="neural", governance_config="all",
            workload_env={"ARRIVAL_RATE": "5.0"},
            oracle_mode=True, dry_run=True,
        )
        mock_start.assert_called_once()
        _, kwargs = mock_start.call_args
        assert kwargs.get("oracle_mode") is True


# ---------------------------------------------------------------------------
# Compose file uses WORKER_BROKER_URL
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# wait_for_federation in oracle mode
# ---------------------------------------------------------------------------


class TestOracleWaitForFederation:
    """In oracle mode, only VM1's broker is health-checked."""

    @patch("scripts.multi_vm_runner._exec")
    def test_oracle_mode_checks_only_vm1(self, mock_exec):
        from scripts.multi_vm_runner import wait_for_federation, VMS
        mock_exec.return_value = '{"status": "ok"}'
        result = wait_for_federation(oracle_mode=True, timeout_s=5)
        assert result is True
        # Should only check VM1
        checked_vms = [c[0][0] for c in mock_exec.call_args_list]
        assert all(vm == VMS[0] for vm in checked_vms)

    @patch("scripts.multi_vm_runner._exec")
    def test_normal_mode_checks_all_vms(self, mock_exec):
        from scripts.multi_vm_runner import wait_for_federation, VMS
        mock_exec.return_value = '{"status": "ok"}'
        result = wait_for_federation(oracle_mode=False, timeout_s=5)
        assert result is True
        checked_vm_names = [c[0][0].name for c in mock_exec.call_args_list]
        assert len(set(checked_vm_names)) == 4


# ---------------------------------------------------------------------------
# Compose file uses WORKER_BROKER_URL
# ---------------------------------------------------------------------------


class TestComposeWorkerBrokerUrl:
    """docker-compose.vm.yaml workers reference WORKER_BROKER_URL."""

    def test_worker_0_uses_worker_broker_url(self):
        from pathlib import Path
        compose_path = Path("deploy/docker-compose.vm.yaml")
        content = compose_path.read_text()
        assert "WORKER_BROKER_URL" in content, (
            "docker-compose.vm.yaml should reference WORKER_BROKER_URL"
        )

    def test_all_12_workers_use_worker_broker_url(self):
        from pathlib import Path
        compose_path = Path("deploy/docker-compose.vm.yaml")
        content = compose_path.read_text()
        count = content.count("WORKER_BROKER_URL")
        assert count >= 12, (
            f"Expected >=12 WORKER_BROKER_URL references (one per worker), "
            f"got {count}"
        )
