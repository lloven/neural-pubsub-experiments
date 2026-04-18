"""Tests for workload container cleanup on graceful shutdown.

The workload runs as a `docker run` container (not compose-managed).
On SIGINT, `stop_cluster()` only stops compose containers. The workload
container must also be stopped, otherwise it orphans and continues
consuming resources for up to 14 minutes.

Fix: name the workload container and kill it in the cleanup path.
"""

from __future__ import annotations

from unittest.mock import patch


class TestWorkloadContainerNamed:
    """The workload docker run command should use --name for cleanup."""

    @patch("scripts.multi_vm_runner.collect_results")
    @patch("scripts.multi_vm_runner.stop_cluster")
    @patch("scripts.multi_vm_runner.teardown_wan_emulation")
    @patch("scripts.multi_vm_runner.setup_wan_emulation")
    @patch("scripts.multi_vm_runner.wait_for_federation", return_value=True)
    @patch("scripts.multi_vm_runner.start_cluster")
    @patch("scripts.multi_vm_runner._exec")
    def test_workload_container_has_name(
        self, mock_exec, mock_start, mock_wait,
        mock_wan_setup, mock_wan_tear, mock_stop, mock_collect,
    ):
        from scripts.multi_vm_runner import run_single
        run_single(
            config="market-quad", seed=42,
            placement_mode="market", governance_config="all",
            workload_env={"ARRIVAL_RATE": "5.0"},
            dry_run=True,
        )
        # Find the docker run call in dry-run output
        docker_run_calls = [
            c for c in mock_exec.call_args_list
            if "docker run" in str(c) and "workload" not in str(c).lower()
            or "docker run" in str(c)
        ]
        # At least one call should contain --name
        workload_calls = [
            c for c in mock_exec.call_args_list
            if "docker run" in str(c) and "src.workload.generator" in str(c)
        ]
        assert len(workload_calls) >= 1, "Should have a docker run workload call"
        cmd = str(workload_calls[0])
        assert "--name" in cmd, (
            "Workload docker run must use --name for cleanup. "
            f"Got: {cmd[:200]}"
        )


class TestStopClusterKillsWorkload:
    """stop_cluster should also kill the named workload container."""

    @patch("scripts.multi_vm_runner._exec")
    def test_stop_cluster_kills_workload_container(self, mock_exec):
        from scripts.multi_vm_runner import stop_cluster, VMS
        stop_cluster()
        # Should call docker kill on the workload container
        all_cmds = " ".join(str(c) for c in mock_exec.call_args_list)
        assert "npubsub-workload" in all_cmds, (
            "stop_cluster should kill the npubsub-workload container"
        )
