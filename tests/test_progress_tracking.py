"""Tests for progress tracking across local and distributed experiments.

Covers:
  T1: Run ID duplication fix (multi_vm_runner.run_single accepts explicit run_id)
  T2: Distributed result_file path correctness
  T3: Merge progress sources (progress.json + CSV + distributed dirs)
  T4: Resume from lost .progress.json (CSV-based fallback)
  T5: Stale "running" entries reset on resume
  T6: Stalled run display (OVERDUE indicator)
  T7: Robust ETA computation (stale timestamp filtering, actual run duration)
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# T1: Run ID duplication
# ---------------------------------------------------------------------------

class TestRunIdDuplication:
    """multi_vm_runner.run_single must not double the seed suffix."""

    @patch("scripts.multi_vm_runner.collect_results")
    @patch("scripts.multi_vm_runner.teardown_wan_emulation")
    @patch("scripts.multi_vm_runner.wait_for_federation", return_value=True)
    @patch("scripts.multi_vm_runner.setup_wan_emulation")
    @patch("scripts.multi_vm_runner.stop_cluster")
    @patch("scripts.multi_vm_runner.start_cluster")
    @patch("scripts.multi_vm_runner._ssh", return_value="")
    def test_explicit_run_id_no_duplication(self, mock_ssh, *_mocks):
        """When run_id is passed explicitly, it should be used as-is."""
        from scripts.multi_vm_runner import run_single

        run_single(
            config="neural",
            seed=42,
            placement_mode="neural",
            governance_config="none",
            run_id="neural_http_rate-medium_seed-42",
            results_subdir="baseline",
            dry_run=True,
        )
        calls = " ".join(str(c) for c in mock_ssh.call_args_list)
        # The run_id in the workload command should NOT have _seed-42_seed-42
        assert "_seed-42_seed-42" not in calls, (
            f"Doubled seed suffix found in SSH commands: {calls[:500]}"
        )

    @patch("scripts.multi_vm_runner.collect_results")
    @patch("scripts.multi_vm_runner.teardown_wan_emulation")
    @patch("scripts.multi_vm_runner.wait_for_federation", return_value=True)
    @patch("scripts.multi_vm_runner.setup_wan_emulation")
    @patch("scripts.multi_vm_runner.stop_cluster")
    @patch("scripts.multi_vm_runner.start_cluster")
    @patch("scripts.multi_vm_runner._ssh", return_value="")
    def test_default_run_id_backward_compat(self, mock_ssh, *_mocks):
        """Without explicit run_id, config_seed-{seed} is constructed (backward compat)."""
        from scripts.multi_vm_runner import run_single

        run_single(
            config="market-quad",
            seed=42,
            placement_mode="market",
            governance_config="all",
            results_subdir="market",
            dry_run=True,
        )
        calls = " ".join(str(c) for c in mock_ssh.call_args_list)
        assert "market-quad_seed-42" in calls


# ---------------------------------------------------------------------------
# T2: Distributed result_file path
# ---------------------------------------------------------------------------

class TestDistributedResultPath:
    """_run_distributed must return result_file pointing to actual location."""

    def test_baseline_distributed_result_is_directory(self):
        """Baseline _run_distributed result_file should be a directory path, not .csv."""
        from scripts.run_baseline import _run_distributed, RunConfig

        run = RunConfig(
            config_name="neural",
            rate_label="medium",
            arrival_rate=5.0,
            pipeline_complexity=3,
            seed=42,
            transport="http",
            placement_strategy="neural",
        )
        with patch("scripts.multi_vm_runner.run_single"):
            result = _run_distributed(run, dry_run=True)

        # Should NOT end in .csv (it's a directory with vm1/, vm2/, etc.)
        assert not result["result_file"].endswith(".csv"), (
            f"Distributed result_file should be a directory, got: {result['result_file']}"
        )
        assert run.run_id in result["result_file"]


# ---------------------------------------------------------------------------
# T3: Merge progress sources
# ---------------------------------------------------------------------------

class TestMergeProgress:
    """Monitor must merge .progress.json, CSV discovery, and distributed dirs."""

    def test_merge_progress_combines_sources(self):
        from scripts.monitor import merge_progress

        primary = {"run_a": {"status": "done"}, "run_b": {"status": "running"}}
        secondary = {"run_b": {"status": "done"}, "run_c": {"status": "done"}}

        merged = merge_progress(primary, secondary)
        assert merged["run_a"]["status"] == "done"
        assert merged["run_b"]["status"] == "running"  # primary wins
        assert merged["run_c"]["status"] == "done"

    def test_merge_progress_primary_precedence(self):
        from scripts.monitor import merge_progress

        primary = {"run_x": {"status": "running", "timestamp": "2026-04-01T10:00:00"}}
        secondary = {"run_x": {"status": "done", "timestamp": "2026-04-01T09:00:00"}}

        merged = merge_progress(primary, secondary)
        assert merged["run_x"]["status"] == "running"

    def test_discover_distributed_runs(self, tmp_path):
        from scripts.monitor import _discover_distributed_runs

        phase_dir = tmp_path / "baseline"
        phase_dir.mkdir()

        # Distributed run dirs (contain vm1/) WITH sibling .csv
        (phase_dir / "run_a" / "vm1").mkdir(parents=True)
        (phase_dir / "run_a.csv").write_text("header\nrow1\n")
        (phase_dir / "run_b" / "vm1").mkdir(parents=True)
        (phase_dir / "run_b" / "vm2").mkdir(parents=True)
        (phase_dir / "run_b.csv").write_text("header\nrow1\n")

        # Phantom dir (vm1/ but no sibling .csv)
        (phase_dir / "phantom" / "vm1").mkdir(parents=True)

        # Non-distributed dir (no vm* child)
        (phase_dir / "some_other_dir").mkdir()

        # Regular file
        (phase_dir / "summary.csv").write_text("header\n")

        result = _discover_distributed_runs(phase_dir)
        assert "run_a" in result
        assert "run_b" in result
        assert "phantom" not in result, "Phantom dir (no .csv) must not be found"
        assert "some_other_dir" not in result
        assert result["run_a"]["status"] == "done"

    def test_phase_summary_merges_all_sources(self, tmp_path):
        """phase_summary should find runs from .progress.json AND distributed dirs
        (but only distributed dirs with a sibling .csv).
        """
        from scripts.monitor import phase_summary

        phase_dir = tmp_path / "baseline"
        phase_dir.mkdir()

        # 1 run tracked in .progress.json
        progress = {"run_tracked": {"status": "running", "timestamp": datetime.now().isoformat()}}
        (phase_dir / ".progress.json").write_text(json.dumps(progress))

        # 2 completed distributed run dirs (vm1/ + sibling .csv)
        (phase_dir / "run_dist_a" / "vm1").mkdir(parents=True)
        (phase_dir / "run_dist_a.csv").write_text("header\nrow1\n")
        (phase_dir / "run_dist_b" / "vm1").mkdir(parents=True)
        (phase_dir / "run_dist_b.csv").write_text("header\nrow1\n")

        summary = phase_summary(phase_dir)
        assert summary["total"] == 3
        assert summary["done"] == 2
        assert summary["running"] == 1


# ---------------------------------------------------------------------------
# T4: Resume from lost .progress.json
# ---------------------------------------------------------------------------

class TestResumeRecovery:
    """--resume must recover completed runs from disk when .progress.json is missing."""

    def test_discover_completed_runs_finds_csvs(self, tmp_path):
        from scripts._common import _discover_completed_runs

        results_dir = tmp_path / "baseline"
        results_dir.mkdir()
        (results_dir / "run_a.csv").write_text("header\nrow1\n")
        (results_dir / "run_b.csv").write_text("header\nrow1\n")
        (results_dir / "run_c.partial.csv").write_text("header\n")  # partial, not done
        (results_dir / "summary.csv").write_text("header\n")  # skip

        completed = _discover_completed_runs(results_dir)
        assert "run_a" in completed
        assert "run_b" in completed
        assert "run_c" not in completed  # partial
        assert "summary" not in completed  # skip

    def test_discover_completed_runs_finds_distributed_dirs(self, tmp_path):
        """A distributed run is complete iff it has BOTH a vm*/ subdir
        (post-run rsync from worker VMs) AND a sibling .csv (workload's
        primary output). The .csv is the canonical "did this run finish"
        marker; the vm*/ dirs are auxiliary broker/federation logs.
        """
        from scripts._common import _discover_completed_runs

        results_dir = tmp_path / "baseline"
        results_dir.mkdir()
        (results_dir / "run_dist" / "vm1").mkdir(parents=True)
        (results_dir / "run_dist.csv").write_text("header\nrow1\n")

        completed = _discover_completed_runs(results_dir)
        assert "run_dist" in completed

    def test_discover_completed_runs_skips_phantom_dirs(self, tmp_path):
        """A directory with vm*/ children but NO sibling .csv is a phantom
        (an aborted run where rsync of broker/federation state from worker
        VMs partially succeeded but the workload never wrote its .csv).
        Such directories must NOT be marked as done, otherwise --resume
        will skip the affected runs forever.

        This was the root cause of the oracle-global anomaly-sp re-run
        bug: 26 phantom directories were re-marked done at every --resume.
        """
        from scripts._common import _discover_completed_runs

        results_dir = tmp_path / "market"
        results_dir.mkdir()
        # Phantom: only VM3's federation rsync survived; no .csv from workload
        (results_dir / "phantom_run" / "vm3" / "federation").mkdir(parents=True)
        (results_dir / "phantom_run" / "vm3" / "federation" / ".progress.json").write_text("{}")

        completed = _discover_completed_runs(results_dir)
        assert "phantom_run" not in completed, (
            "phantom_run has vm3/ rsync residue but no .csv; must not be "
            "marked as done"
        )

    def test_discover_completed_runs_skips_dir_with_only_old_csv(self, tmp_path):
        """A directory whose sibling .csv was renamed to .csv.old (after a
        bug fix invalidated the data) is NOT done. The .csv must exist as
        the canonical name, not as .csv.old.
        """
        from scripts._common import _discover_completed_runs

        results_dir = tmp_path / "market"
        results_dir.mkdir()
        (results_dir / "invalidated_run" / "vm1").mkdir(parents=True)
        (results_dir / "invalidated_run.csv.old").write_text("header\nrow1\n")

        completed = _discover_completed_runs(results_dir)
        assert "invalidated_run" not in completed


# ---------------------------------------------------------------------------
# T5: Stale "running" entries reset
# ---------------------------------------------------------------------------

class TestStaleRunningReset:
    """--resume must reset stale 'running' entries to 'queued'."""

    def test_running_entries_reset_on_resume(self, tmp_path):
        from scripts._common import _load_progress

        progress_file = tmp_path / ".progress.json"
        old = datetime.now() - timedelta(hours=2)
        progress = {
            "run_a": {"status": "running", "timestamp": old.isoformat()},
            "run_b": {"status": "done", "timestamp": old.isoformat()},
            "run_c": {"status": "queued", "timestamp": ""},
        }
        progress_file.write_text(json.dumps(progress))

        loaded = _load_progress(tmp_path)
        # After loading for resume, "running" should be reset
        # (this will be done in phase_main after load, but we test the helper)
        from scripts._common import _reset_stale_running
        reset = _reset_stale_running(loaded)

        assert reset["run_a"]["status"] == "queued"
        assert reset["run_b"]["status"] == "done"  # unchanged
        assert reset["run_c"]["status"] == "queued"  # unchanged


# ---------------------------------------------------------------------------
# T6: Stalled run display
# ---------------------------------------------------------------------------

class TestStalledRunDisplay:
    """Monitor must show OVERDUE for runs exceeding expected duration."""

    def test_format_overdue(self):
        from scripts.monitor import format_run_eta

        # Normal: within expected duration
        normal = format_run_eta(elapsed_s=300, expected_s=720)
        assert "OVERDUE" not in normal

        # Overdue: 1.5x expected
        overdue = format_run_eta(elapsed_s=1200, expected_s=720)
        assert "OVERDUE" in overdue


# ---------------------------------------------------------------------------
# T7: Robust ETA computation
# ---------------------------------------------------------------------------

class TestRobustEta:
    """ETA computation must handle stale timestamps and use actual run durations."""

    def test_eta_filters_stale_timestamps(self, tmp_path):
        from scripts.monitor import phase_summary

        phase_dir = tmp_path / "test_phase"
        phase_dir.mkdir()

        now = datetime.now()
        progress = {
            # Stale: from 2 days ago (crashed run)
            "old_run": {"status": "done", "timestamp": (now - timedelta(days=2)).isoformat()},
            # Recent: from this session
            "recent_a": {"status": "done", "timestamp": (now - timedelta(minutes=25)).isoformat()},
            "recent_b": {"status": "done", "timestamp": (now - timedelta(minutes=12)).isoformat()},
            "queued_c": {"status": "queued", "timestamp": ""},
        }
        (phase_dir / ".progress.json").write_text(json.dumps(progress))

        summary = phase_summary(phase_dir)
        # ETA should be based on recent run rate, not inflated by 2-day-old timestamp
        # With 2 recent runs in ~13 minutes, ETA for 1 remaining should be ~6-7 min
        # Not ~48 hours (if stale timestamp were included)
        assert summary["eta_s"] < 3600, (
            f"ETA {summary['eta_s']}s is too high — stale timestamp not filtered"
        )


# ---------------------------------------------------------------------------
# T8: Summary CSV writer handles extra fields (L51 failure dict)
# ---------------------------------------------------------------------------


class TestSummaryCsvWriter:
    """Summary CSV must not crash when result dicts contain extra fields.

    The L51 fix (commit 79718c7) added an 'error' key to the failure
    dict from run_single. The summary CSV writer in _common.py:862 used
    csv.DictWriter with fieldnames=["run_id", "status", "result_file"],
    which raises ValueError on the unexpected 'error' key. This crashed
    the campaign after all 225 runs completed (75 failed rr-global +
    150 successful oracle/market), losing the summary.
    """

    def test_summary_csv_with_extra_fields(self, tmp_path):
        """DictWriter must tolerate extra keys (e.g. 'error') in result dicts."""
        import csv
        from scripts._common import _write_summary_csv

        results = [
            {"run_id": "run_a", "status": "completed", "result_file": "a.csv"},
            {"run_id": "run_b", "status": "failed", "result_file": "b.csv",
             "error": "federation_timeout"},
        ]
        summary_file = tmp_path / "test_summary.csv"
        _write_summary_csv(summary_file, results)

        rows = list(csv.DictReader(open(summary_file)))
        assert len(rows) == 2
        assert rows[0]["status"] == "completed"
        assert rows[1]["status"] == "failed"
        # 'error' should not appear as a column
        assert "error" not in rows[0]
