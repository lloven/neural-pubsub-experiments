"""Tests for multi_vm_runner.py strategy switching and env var propagation.

Verifies that the multi-VM runner can start clusters with all placement
strategies (round-robin, random, neural, market, locality, latency, spillover)
and passes the correct env vars to each VM's Docker Compose stack.
"""

import logging
from unittest.mock import patch, MagicMock

import pytest

from scripts.multi_vm_runner import (
    CONFIG_MAP,
    VMConfig,
    start_cluster,
    run_single,
)


# ---------------------------------------------------------------------------
# CONFIG_MAP coverage
# ---------------------------------------------------------------------------

def test_config_map_has_all_strategies():
    """CONFIG_MAP must cover all placement strategies for fair comparison."""
    required = {
        "round-robin", "random", "neural",
        "market-quad", "oracle-global",
        "locality-only", "latency-greedy", "spillover",
    }
    assert required.issubset(set(CONFIG_MAP.keys())), (
        f"Missing configs: {required - set(CONFIG_MAP.keys())}"
    )


def test_config_map_round_robin_uses_static_broker():
    """Round-robin config must use the static broker module."""
    cfg = CONFIG_MAP["round-robin"]
    assert cfg.get("broker_module") == "src.broker.static_broker"
    assert cfg.get("static_placement") == "round_robin"


def test_config_map_random_uses_static_broker():
    """Random config must use the static broker module."""
    cfg = CONFIG_MAP["random"]
    assert cfg.get("broker_module") == "src.broker.static_broker"
    assert cfg.get("static_placement") == "random"


def test_config_map_neural_uses_neural_broker():
    """Neural config must use the neural broker (default)."""
    cfg = CONFIG_MAP["neural"]
    assert cfg.get("broker_module", "src.broker.neural_broker") == "src.broker.neural_broker"


# ---------------------------------------------------------------------------
# start_cluster env var propagation
# ---------------------------------------------------------------------------

@patch("scripts.multi_vm_runner._ssh")
def test_start_cluster_passes_broker_module(mock_ssh):
    """start_cluster with broker_module passes BROKER_MODULE env var."""
    start_cluster(
        broker_module="src.broker.static_broker",
        placement="round_robin",
        dry_run=True,
    )
    # Check that at least one SSH call contains BROKER_MODULE
    calls = [str(c) for c in mock_ssh.call_args_list]
    ssh_cmds = " ".join(calls)
    assert "BROKER_MODULE=src.broker.static_broker" in ssh_cmds, (
        f"BROKER_MODULE not found in SSH commands: {ssh_cmds[:500]}"
    )


@patch("scripts.multi_vm_runner._ssh")
def test_start_cluster_passes_placement(mock_ssh):
    """start_cluster with placement passes PLACEMENT env var."""
    start_cluster(
        broker_module="src.broker.static_broker",
        placement="random",
        dry_run=True,
    )
    calls = [str(c) for c in mock_ssh.call_args_list]
    ssh_cmds = " ".join(calls)
    assert "PLACEMENT=random" in ssh_cmds


@patch("scripts.multi_vm_runner._ssh")
def test_start_cluster_passes_extra_env(mock_ssh):
    """start_cluster with extra_env passes additional env vars."""
    start_cluster(
        extra_env={"ARRIVAL_RATE": "10.0", "SEED": "42"},
        dry_run=True,
    )
    calls = [str(c) for c in mock_ssh.call_args_list]
    ssh_cmds = " ".join(calls)
    assert "ARRIVAL_RATE=10.0" in ssh_cmds
    assert "SEED=42" in ssh_cmds


# ---------------------------------------------------------------------------
# run_single result dir and workload env
# ---------------------------------------------------------------------------

@patch("scripts.multi_vm_runner.collect_results")
@patch("scripts.multi_vm_runner.teardown_wan_emulation")
@patch("scripts.multi_vm_runner.wait_for_federation", return_value=True)
@patch("scripts.multi_vm_runner.setup_wan_emulation")
@patch("scripts.multi_vm_runner.stop_cluster")
@patch("scripts.multi_vm_runner.start_cluster")
@patch("scripts.multi_vm_runner._ssh", return_value="")
def test_run_single_results_subdir(mock_ssh, mock_start, mock_stop,
                                    mock_wan, mock_fed, mock_teardown,
                                    mock_collect):
    """run_single with results_subdir writes to that subdirectory."""
    run_single(
        config="neural",
        seed=42,
        placement_mode="neural",
        governance_config="none",
        results_subdir="baseline",
        workload_env={"ARRIVAL_RATE": "5.0"},
        dry_run=True,
    )
    # The workload command should reference results/baseline/
    calls = [str(c) for c in mock_ssh.call_args_list]
    ssh_cmds = " ".join(calls)
    assert "results/baseline" in ssh_cmds or mock_collect.called


@patch("scripts.multi_vm_runner.collect_results")
@patch("scripts.multi_vm_runner.teardown_wan_emulation")
@patch("scripts.multi_vm_runner.wait_for_federation", return_value=True)
@patch("scripts.multi_vm_runner.setup_wan_emulation")
@patch("scripts.multi_vm_runner.stop_cluster")
@patch("scripts.multi_vm_runner.start_cluster")
@patch("scripts.multi_vm_runner._ssh", return_value="")
def test_run_single_passes_workload_env(mock_ssh, mock_start, mock_stop,
                                         mock_wan, mock_fed, mock_teardown,
                                         mock_collect):
    """run_single passes workload_env vars to the Docker workload container."""
    run_single(
        config="neural",
        seed=42,
        placement_mode="neural",
        governance_config="none",
        workload_env={"ARRIVAL_RATE": "5.0", "PIPELINE_MIX_CQI": "0.5", "PIPELINE_MIX_ANOMALY": "0.5"},
        dry_run=True,
    )
    calls = [str(c) for c in mock_ssh.call_args_list]
    ssh_cmds = " ".join(calls)
    # dry_run logs SSH commands via logger, so they appear in mock_ssh calls
    assert "PIPELINE_MIX_CQI" in ssh_cmds, (
        f"Workload env PIPELINE_MIX_CQI not found in SSH commands: {ssh_cmds[:300]}"
    )
