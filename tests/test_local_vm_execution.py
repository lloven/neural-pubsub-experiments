"""Tests for local VM execution and SSH resilience.

Verifies that:
- is_local_vm detects when running on VM1
- _exec dispatches to _local_run for local VM, _ssh for remote
- _ssh retries on transient connection failures
- SSH does NOT retry on command-level failures
"""

from __future__ import annotations

import subprocess
import time
from unittest.mock import patch, MagicMock, call

import pytest

from scripts.multi_vm_runner import VMS, REMOTE_PROJECT_DIR


# ---------------------------------------------------------------------------
# Task 1.1: Local VM detection
# ---------------------------------------------------------------------------


class TestLocalVmDetection:
    """is_local_vm must detect the local machine."""

    def test_local_when_hostname_matches_name(self):
        from scripts.multi_vm_runner import is_local_vm
        with patch("socket.gethostname", return_value=VMS[0].name):
            assert is_local_vm(VMS[0]) is True

    def test_local_when_hostname_matches_ssh_host(self):
        from scripts.multi_vm_runner import is_local_vm
        with patch("socket.gethostname", return_value=VMS[0].ssh_host):
            assert is_local_vm(VMS[0]) is True

    def test_not_local_for_other_vm(self):
        from scripts.multi_vm_runner import is_local_vm
        with patch("socket.gethostname", return_value=VMS[0].name):
            assert is_local_vm(VMS[1]) is False

    def test_override_flag(self):
        from scripts.multi_vm_runner import is_local_vm
        assert is_local_vm(VMS[0], local_vm_override=VMS[0].name) is True
        assert is_local_vm(VMS[1], local_vm_override=VMS[0].name) is False

    def test_override_none_falls_back_to_hostname(self):
        from scripts.multi_vm_runner import is_local_vm
        with patch("socket.gethostname", return_value=VMS[0].name):
            assert is_local_vm(VMS[0], local_vm_override=None) is True


# ---------------------------------------------------------------------------
# Task 1.2: _exec dispatch
# ---------------------------------------------------------------------------


class TestExecDispatch:
    """_exec must use _local_run for local VM, _ssh for remote."""

    @patch("scripts.multi_vm_runner._local_run", return_value="local_ok")
    @patch("scripts.multi_vm_runner._ssh", return_value="ssh_ok")
    @patch("socket.gethostname", return_value=VMS[0].name)
    def test_exec_uses_local_for_local_vm(self, _hostname, mock_ssh, mock_local):
        from scripts.multi_vm_runner import _exec
        result = _exec(VMS[0], "echo test")
        mock_local.assert_called_once()
        mock_ssh.assert_not_called()
        assert result == "local_ok"

    @patch("scripts.multi_vm_runner._local_run", return_value="local_ok")
    @patch("scripts.multi_vm_runner._ssh", return_value="ssh_ok")
    @patch("socket.gethostname", return_value=VMS[0].name)
    def test_exec_uses_ssh_for_remote_vm(self, _hostname, mock_ssh, mock_local):
        from scripts.multi_vm_runner import _exec
        result = _exec(VMS[1], "echo test")
        mock_ssh.assert_called_once()
        mock_local.assert_not_called()
        assert result == "ssh_ok"

    @patch("scripts.multi_vm_runner._local_run", return_value="ok")
    @patch("scripts.multi_vm_runner._ssh")
    def test_exec_with_override(self, mock_ssh, mock_local):
        from scripts.multi_vm_runner import _exec
        # Override says VM1 is local
        _exec(VMS[0], "cmd", local_vm_override=VMS[0].name)
        mock_local.assert_called_once()
        mock_ssh.assert_not_called()


# ---------------------------------------------------------------------------
# Task 1.3: SSH retry
# ---------------------------------------------------------------------------


class TestSshRetry:
    """_ssh must retry on connection failures but not command failures."""

    @patch("subprocess.run")
    def test_retries_on_timeout(self, mock_run):
        from scripts.multi_vm_runner import _ssh
        mock_run.side_effect = [
            subprocess.TimeoutExpired("ssh", 10),
            subprocess.CompletedProcess("ssh", 0, stdout="ok", stderr=""),
        ]
        result = _ssh("host", "cmd", retries=1, retry_delay=0.01)
        assert mock_run.call_count == 2
        assert result == "ok"

    @patch("subprocess.run")
    def test_retries_on_os_error(self, mock_run):
        from scripts.multi_vm_runner import _ssh
        mock_run.side_effect = [
            OSError("Connection refused"),
            subprocess.CompletedProcess("ssh", 0, stdout="ok", stderr=""),
        ]
        result = _ssh("host", "cmd", retries=1, retry_delay=0.01)
        assert mock_run.call_count == 2

    @patch("subprocess.run")
    def test_no_retry_on_command_failure(self, mock_run):
        from scripts.multi_vm_runner import _ssh
        mock_run.return_value = subprocess.CompletedProcess(
            "ssh", 1, stdout="", stderr="command not found"
        )
        _ssh("host", "cmd", retries=3, check=False)
        assert mock_run.call_count == 1  # No retry for non-zero exit

    @patch("subprocess.run")
    def test_raises_after_max_retries(self, mock_run):
        from scripts.multi_vm_runner import _ssh
        mock_run.side_effect = subprocess.TimeoutExpired("ssh", 10)
        with pytest.raises(subprocess.TimeoutExpired):
            _ssh("host", "cmd", retries=2, retry_delay=0.01)
        assert mock_run.call_count == 3  # initial + 2 retries

    @patch("subprocess.run")
    def test_zero_retries_is_default_behavior(self, mock_run):
        from scripts.multi_vm_runner import _ssh
        mock_run.side_effect = subprocess.TimeoutExpired("ssh", 10)
        with pytest.raises(subprocess.TimeoutExpired):
            _ssh("host", "cmd")  # default retries=0
        assert mock_run.call_count == 1


# ---------------------------------------------------------------------------
# Task: deploy_code rsync
# ---------------------------------------------------------------------------


class TestDeployCode:
    """deploy_code must rsync codebase to remote VMs, skip local."""

    @patch("scripts.multi_vm_runner._ssh")
    @patch("subprocess.run")
    @patch("socket.gethostname", return_value=VMS[0].name)
    def test_rsyncs_to_remote_vms_only(self, _hostname, mock_run, mock_ssh):
        from scripts.multi_vm_runner import deploy_code
        mock_run.return_value = subprocess.CompletedProcess("rsync", 0)
        deploy_code(dry_run=False)
        # Should rsync to VMs 2-4 (3 calls), skip VM1 (local)
        rsync_calls = [
            c for c in mock_run.call_args_list
            if "rsync" in str(c)
        ]
        assert len(rsync_calls) == 3
        # Verify targets are the remote VMs
        all_args = " ".join(str(c) for c in rsync_calls)
        assert VMS[1].ssh_host in all_args
        assert VMS[2].ssh_host in all_args
        assert VMS[3].ssh_host in all_args
        assert VMS[0].ssh_host not in all_args

    @patch("scripts.multi_vm_runner._ssh")
    @patch("subprocess.run")
    @patch("socket.gethostname", return_value=VMS[0].name)
    def test_excludes_git_and_results(self, _hostname, mock_run, mock_ssh):
        from scripts.multi_vm_runner import deploy_code
        mock_run.return_value = subprocess.CompletedProcess("rsync", 0)
        deploy_code(dry_run=False)
        rsync_calls = [
            c for c in mock_run.call_args_list
            if "rsync" in str(c)
        ]
        for c in rsync_calls:
            args_str = str(c)
            assert ".git" in args_str, "Should exclude .git"
            assert "results" in args_str, "Should exclude results"

    @patch("subprocess.run")
    @patch("socket.gethostname", return_value=VMS[0].name)
    def test_dry_run_no_subprocess(self, _hostname, mock_run):
        from scripts.multi_vm_runner import deploy_code
        deploy_code(dry_run=True)
        # No actual rsync calls in dry-run
        rsync_calls = [
            c for c in mock_run.call_args_list
            if "rsync" in str(c)
        ]
        assert len(rsync_calls) == 0

    @patch("subprocess.run")
    @patch("socket.gethostname", return_value=VMS[0].name)
    def test_target_path_is_remote_project_dir(self, _hostname, mock_run):
        from scripts.multi_vm_runner import deploy_code
        mock_run.return_value = subprocess.CompletedProcess("rsync", 0)
        deploy_code(dry_run=False)
        rsync_calls = [
            c for c in mock_run.call_args_list
            if "rsync" in str(c)
        ]
        for c in rsync_calls:
            args_str = str(c)
            assert REMOTE_PROJECT_DIR.lstrip("~") in args_str or "neural-pubsub" in args_str
