"""Tests for WAN emulation via tc qdisc netem.

Verifies that:
- setup_wan_emulation generates correct tc commands for both directions
- Correct delay (50ms) and jitter (5ms) are applied
- Correct target IPs are used in tc filters
- teardown_wan_emulation cleans up correctly
- sudo failures produce clear error messages
"""

from __future__ import annotations

import subprocess
from unittest.mock import patch, call

import pytest

from scripts.multi_vm_runner import (
    VMS,
    WAN_DELAY_MS,
    WAN_INTERFACE,
    WAN_JITTER_MS,
    setup_wan_emulation,
    teardown_wan_emulation,
)


class TestSetupWanEmulation:
    """setup_wan_emulation must apply tc netem bidirectionally."""

    @patch("scripts.multi_vm_runner._ssh")
    def test_calls_ssh_for_both_directions(self, mock_ssh):
        setup_wan_emulation(VMS[1], VMS[2], dry_run=False)
        # 4 tc commands per direction x 2 directions = 8 SSH calls
        assert mock_ssh.call_count == 8

    @patch("scripts.multi_vm_runner._ssh")
    def test_uses_correct_delay(self, mock_ssh):
        setup_wan_emulation(VMS[1], VMS[2], dry_run=False)
        all_cmds = " ".join(str(c) for c in mock_ssh.call_args_list)
        assert f"{WAN_DELAY_MS}ms" in all_cmds
        assert f"{WAN_JITTER_MS}ms" in all_cmds

    @patch("scripts.multi_vm_runner._ssh")
    def test_targets_correct_ips(self, mock_ssh):
        setup_wan_emulation(VMS[1], VMS[2], dry_run=False)
        all_cmds = " ".join(str(c) for c in mock_ssh.call_args_list)
        # VM2 filter targets VM3 IP, and vice versa
        assert VMS[2].ip in all_cmds, f"VM3 IP {VMS[2].ip} not in tc commands"
        assert VMS[1].ip in all_cmds, f"VM2 IP {VMS[1].ip} not in tc commands"

    @patch("scripts.multi_vm_runner._ssh")
    def test_ssh_hosts_are_vm2_and_vm3(self, mock_ssh):
        setup_wan_emulation(VMS[1], VMS[2], dry_run=False)
        hosts = {c.args[0] for c in mock_ssh.call_args_list}
        assert VMS[1].ssh_host in hosts
        assert VMS[2].ssh_host in hosts
        assert len(hosts) == 2

    @patch("scripts.multi_vm_runner._ssh")
    def test_uses_correct_interface(self, mock_ssh):
        setup_wan_emulation(VMS[1], VMS[2], dry_run=False)
        all_cmds = " ".join(str(c) for c in mock_ssh.call_args_list)
        assert WAN_INTERFACE in all_cmds
        assert "eth0" not in all_cmds, "Should use WAN_INTERFACE, not hardcoded eth0"

    @patch("scripts.multi_vm_runner._ssh")
    def test_dry_run_passes_through(self, mock_ssh):
        setup_wan_emulation(VMS[1], VMS[2], dry_run=True)
        for c in mock_ssh.call_args_list:
            assert c.kwargs.get("dry_run") is True or c[1].get("dry_run") is True


class TestTeardownWanEmulation:
    """teardown_wan_emulation must remove tc rules on both VMs."""

    @patch("scripts.multi_vm_runner._ssh")
    def test_calls_ssh_for_both_vms(self, mock_ssh):
        teardown_wan_emulation(VMS[1], VMS[2], dry_run=False)
        assert mock_ssh.call_count == 2

    @patch("scripts.multi_vm_runner._ssh")
    def test_uses_del_command(self, mock_ssh):
        teardown_wan_emulation(VMS[1], VMS[2], dry_run=False)
        all_cmds = " ".join(str(c) for c in mock_ssh.call_args_list)
        assert "del" in all_cmds


class TestWanEmulationErrorHandling:
    """sudo failures must produce clear diagnostics."""

    @patch("scripts.multi_vm_runner._ssh")
    def test_sudo_failure_does_not_crash(self, mock_ssh):
        mock_ssh.side_effect = subprocess.CalledProcessError(
            1, "ssh", stderr="sudo: a password is required"
        )
        # Should not raise — tc failure is non-fatal (best-effort)
        # but should log a clear message. The current implementation
        # does not wrap in try/except, so this test verifies the
        # DESIRED behavior after adding error handling.
        try:
            setup_wan_emulation(VMS[1], VMS[2], dry_run=False)
        except subprocess.CalledProcessError:
            pytest.fail(
                "setup_wan_emulation should catch sudo failures and log "
                "a clear hint about NOPASSWD configuration."
            )
