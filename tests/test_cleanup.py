"""Tests for Docker container cleanup gaps (orphaned container prevention).

Covers:
  - Cycle 1: Pre-run stale container cleanup (compose_down before compose_up)
  - Cycle 2: Signal handler registration in phase_main
  - Cycle 3: compose_up includes --remove-orphans flag
  - Cycle 4: Regression — compose_down called in finally block of run_single
  - Cycle 5: SIGTERM handler calls compose_down for current project
"""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from scripts._common import COMPOSE_FILE, PROJECT_ROOT


# ============================================================================
# Cycle 1: Pre-run stale container cleanup
# ============================================================================

class TestPreRunCleanup:
    """run_single calls compose_down before compose_up to remove stale containers."""

    @patch("scripts._common._fix_result_permissions")
    @patch("scripts._common.compose_down")
    @patch("scripts._common.compose_up")
    def test_run_single_calls_compose_down_before_compose_up(
        self, mock_up, mock_down, mock_fix
    ):
        """compose_down is called before compose_up in run_single."""
        from scripts._common import run_single

        results_dir = PROJECT_ROOT / "results" / "test_cleanup"
        results_dir.mkdir(parents=True, exist_ok=True)

        call_order = []
        mock_down.side_effect = lambda *a, **kw: call_order.append("down")
        mock_up.side_effect = lambda *a, **kw: call_order.append("up")

        run_single(
            run_id="test-pre-cleanup",
            env={"SEED": "42"},
            results_dir=results_dir,
            total_duration=10,
            dry_run=False,
        )

        # compose_down must appear before compose_up in call order
        assert call_order[0] == "down", (
            f"Expected compose_down before compose_up, got: {call_order}"
        )
        assert "up" in call_order, "compose_up was never called"

    @patch("scripts._common._fix_result_permissions")
    @patch("scripts._common.compose_down")
    @patch("scripts._common.compose_up")
    def test_pre_run_cleanup_uses_correct_project_name(
        self, mock_up, mock_down, mock_fix
    ):
        """Pre-run compose_down uses the same project name as compose_up."""
        from scripts._common import run_single

        results_dir = PROJECT_ROOT / "results" / "test_cleanup"
        results_dir.mkdir(parents=True, exist_ok=True)

        run_single(
            run_id="test-proj-name",
            env={"SEED": "42"},
            results_dir=results_dir,
            total_duration=10,
            dry_run=False,
        )

        # First compose_down call is the pre-run cleanup
        pre_run_call = mock_down.call_args_list[0]
        project_name = pre_run_call[1].get("project_name") or pre_run_call[0][0]
        assert project_name == "npubsub-test-proj-name"


# ============================================================================
# Cycle 2: Signal handler registration in phase_main
# ============================================================================

class TestSignalHandler:
    """phase_main registers SIGTERM and SIGINT handlers."""

    @patch("scripts._common.argparse.ArgumentParser.parse_args")
    def test_phase_main_registers_sigterm_handler(self, mock_parse):
        """phase_main registers a SIGTERM handler."""
        import scripts._common as mod

        mock_parse.return_value = MagicMock(
            configs="A1",
            seeds="42",
            dry_run=True,
            resume=False,
            log_level="WARNING",
        )

        old_handler = signal.getsignal(signal.SIGTERM)
        try:
            mod.phase_main(
                phase_name="Test",
                description="test",
                configs={"A1": {}},
                build_matrix_fn=lambda c, s: [],
                run_fn=lambda r, d: {"run_id": "x", "status": "dry_run", "result_file": ""},
                results_dir=PROJECT_ROOT / "results" / "test_signal",
            )

            current_handler = signal.getsignal(signal.SIGTERM)
            assert current_handler is not old_handler, (
                "SIGTERM handler was not changed by phase_main"
            )
            assert callable(current_handler), "SIGTERM handler is not callable"
        finally:
            signal.signal(signal.SIGTERM, old_handler)

    @patch("scripts._common.argparse.ArgumentParser.parse_args")
    def test_phase_main_registers_sigint_handler(self, mock_parse):
        """phase_main registers a SIGINT handler."""
        import scripts._common as mod

        mock_parse.return_value = MagicMock(
            configs="A1",
            seeds="42",
            dry_run=True,
            resume=False,
            log_level="WARNING",
        )

        # Reset SIGINT to default so we can detect the change
        old_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            mod.phase_main(
                phase_name="Test",
                description="test",
                configs={"A1": {}},
                build_matrix_fn=lambda c, s: [],
                run_fn=lambda r, d: {"run_id": "x", "status": "dry_run", "result_file": ""},
                results_dir=PROJECT_ROOT / "results" / "test_signal",
            )

            current_handler = signal.getsignal(signal.SIGINT)
            assert current_handler is mod._cleanup_current_project, (
                f"SIGINT handler should be _cleanup_current_project, got {current_handler}"
            )
        finally:
            signal.signal(signal.SIGINT, old_handler)


# ============================================================================
# Cycle 3: compose_up includes --remove-orphans
# ============================================================================

class TestRemoveOrphans:
    """compose_up passes --remove-orphans to docker compose."""

    @patch("scripts._common.subprocess.run")
    def test_compose_up_includes_remove_orphans(self, mock_run):
        """compose_up command includes --remove-orphans flag."""
        from scripts._common import compose_up

        mock_run.return_value = MagicMock(returncode=0)

        compose_up(
            project_name="test-proj",
            compose_file=COMPOSE_FILE,
            env={},
            timeout_s=60,
        )

        cmd = mock_run.call_args[0][0]
        assert "--remove-orphans" in cmd, (
            f"--remove-orphans not in compose_up command: {cmd}"
        )

    @patch("scripts._common.subprocess.Popen")
    def test_compose_up_with_failure_fn_includes_remove_orphans(self, mock_popen):
        """compose_up with failure_fn also includes --remove-orphans."""
        from scripts._common import compose_up

        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        compose_up(
            project_name="test-proj",
            compose_file=COMPOSE_FILE,
            env={},
            timeout_s=60,
            failure_fn=lambda: None,
        )

        cmd = mock_popen.call_args[0][0]
        assert "--remove-orphans" in cmd, (
            f"--remove-orphans not in compose_up (Popen) command: {cmd}"
        )


# ============================================================================
# Cycle 4: Regression — compose_down in finally block
# ============================================================================

class TestFinallyCleanup:
    """compose_down is called even when compose_up raises."""

    @patch("scripts._common._fix_result_permissions")
    @patch("scripts._common.compose_down")
    @patch("scripts._common.compose_up", side_effect=Exception("boom"))
    def test_compose_down_called_on_compose_up_failure(
        self, mock_up, mock_down, mock_fix
    ):
        """compose_down runs in finally block even if compose_up raises."""
        from scripts._common import run_single

        results_dir = PROJECT_ROOT / "results" / "test_cleanup"
        results_dir.mkdir(parents=True, exist_ok=True)

        with pytest.raises(Exception, match="boom"):
            run_single(
                run_id="test-finally",
                env={"SEED": "42"},
                results_dir=results_dir,
                total_duration=10,
            )

        # compose_down must have been called (at least the finally-block call)
        assert mock_down.call_count >= 1, "compose_down not called after compose_up failure"


# ============================================================================
# Cycle 5: SIGTERM handler calls compose_down for current project
# ============================================================================

class TestSignalHandlerBehavior:
    """Signal handler calls compose_down for the currently running project."""

    @patch("scripts._common.compose_down")
    def test_signal_handler_calls_compose_down(self, mock_down):
        """The signal handler function calls compose_down with the current project."""
        from scripts._common import _cleanup_current_project, _current_project

        # Simulate a project being tracked
        import scripts._common as mod
        mod._current_project = {
            "project_name": "npubsub-test-signal",
            "compose_file": COMPOSE_FILE,
            "env": {"SEED": "42"},
            "compose_files": None,
        }

        try:
            _cleanup_current_project(signal.SIGTERM, None)
        except SystemExit:
            pass  # Handler should exit

        mock_down.assert_called_once_with(
            "npubsub-test-signal",
            COMPOSE_FILE,
            {"SEED": "42"},
            compose_files=None,
        )
