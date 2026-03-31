"""Tests for SSH-based failure injection in multi_vm_runner.py."""

from unittest.mock import patch, call

import pytest

from scripts.multi_vm_runner import (
    VMConfig,
    inject_remote_kill,
    inject_remote_partition,
)


VM1 = VMConfig("vm1", "10.0.0.1", "test-vm1", "vm1.env", "edge", "d1")
VM2 = VMConfig("vm2", "10.0.0.2", "test-vm2", "vm2.env", "edge", "d2")


@patch("scripts.multi_vm_runner._ssh")
def test_inject_remote_kill_dry_run(mock_ssh):
    """inject_remote_kill with dry_run produces SSH docker kill command."""
    inject_remote_kill(VM1, "worker-3", delay_s=0, dry_run=True)
    calls_str = " ".join(str(c) for c in mock_ssh.call_args_list)
    assert "docker" in calls_str
    assert "kill" in calls_str or "stop" in calls_str
    assert "worker-3" in calls_str


@patch("scripts.multi_vm_runner._ssh")
def test_inject_remote_kill_targets_correct_vm(mock_ssh):
    """inject_remote_kill targets the specified VM's SSH host."""
    inject_remote_kill(VM2, "broker", delay_s=0, dry_run=True)
    assert mock_ssh.call_args_list[0][0][0] == "test-vm2"


@patch("scripts.multi_vm_runner._ssh")
def test_inject_remote_partition_dry_run(mock_ssh):
    """inject_remote_partition with dry_run produces SSH command to stop broker."""
    inject_remote_partition(VM1, VM2, delay_s=0, dry_run=True)
    calls_str = " ".join(str(c) for c in mock_ssh.call_args_list)
    # Should target one of the VMs to stop the broker
    assert "docker" in calls_str
    assert "stop" in calls_str or "kill" in calls_str


@patch("scripts.multi_vm_runner._ssh")
def test_inject_remote_partition_targets_correct_vm(mock_ssh):
    """inject_remote_partition targets the destination VM."""
    inject_remote_partition(VM1, VM2, delay_s=0, dry_run=True)
    hosts_called = [c[0][0] for c in mock_ssh.call_args_list]
    assert "test-vm2" in hosts_called
