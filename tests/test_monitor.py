"""Tests for scripts.monitor.

The monitor displays per-phase progress by merging three data sources:
1. .progress.json (authoritative, from the phase runner)
2. CSV files on disk (fallback for bash-based runners)
3. Distributed run directories with vm*/ children (rsync residue)

Bugs discovered via systematic-debugging (Phase 1 evidence gathering,
2026-04-11):
  - _discover_distributed_runs counted phantom vm*/ dirs as "done"
    even when no sibling .csv existed (same bug as _common.py L50/L51)
  - discover_phases included legacy phase_c/phase_d dirs, inflating
    phase counts
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# _discover_distributed_runs: phantom-dir bug
# ---------------------------------------------------------------------------


class TestDiscoverDistributedRuns:
    """_discover_distributed_runs must require a sibling .csv to mark done."""

    def test_dir_with_csv_sibling_is_done(self, tmp_path):
        from scripts.monitor import _discover_distributed_runs

        results_dir = tmp_path / "market"
        results_dir.mkdir()
        (results_dir / "real_run" / "vm1").mkdir(parents=True)
        (results_dir / "real_run.csv").write_text("header\nrow\n")

        progress = _discover_distributed_runs(results_dir)
        assert "real_run" in progress
        assert progress["real_run"]["status"] == "done"

    def test_phantom_dir_without_csv_is_not_done(self, tmp_path):
        """A directory with vm*/ children but no sibling .csv is a phantom
        from an aborted run (partial rsync). Must NOT be counted as done.

        This was the root cause of the ablation 225/225 false-positive
        and the inflated federation/contention counts in the monitor.
        """
        from scripts.monitor import _discover_distributed_runs

        results_dir = tmp_path / "ablation"
        results_dir.mkdir()
        # Phantom: vm3/federation rsync residue, no .csv from workload
        (results_dir / "phantom_run" / "vm3" / "federation").mkdir(parents=True)

        progress = _discover_distributed_runs(results_dir)
        assert "phantom_run" not in progress

    def test_dir_with_csv_old_sibling_is_not_done(self, tmp_path):
        """A .csv.old file (invalidated data) does not count as a .csv."""
        from scripts.monitor import _discover_distributed_runs

        results_dir = tmp_path / "market"
        results_dir.mkdir()
        (results_dir / "invalidated" / "vm1").mkdir(parents=True)
        (results_dir / "invalidated.csv.old").write_text("old data\n")

        progress = _discover_distributed_runs(results_dir)
        assert "invalidated" not in progress

    def test_multiple_vms_without_csv_still_phantom(self, tmp_path):
        """Even with vm1/vm2/vm3/vm4 all present, no .csv → phantom."""
        from scripts.monitor import _discover_distributed_runs

        results_dir = tmp_path / "market"
        results_dir.mkdir()
        for i in range(1, 5):
            (results_dir / "full_phantom" / f"vm{i}").mkdir(parents=True)

        progress = _discover_distributed_runs(results_dir)
        assert "full_phantom" not in progress


# ---------------------------------------------------------------------------
# discover_phases: legacy directory filtering
# ---------------------------------------------------------------------------


class TestDiscoverPhases:
    """discover_phases must skip legacy phase_X dirs and archive dirs."""

    def test_skips_legacy_phase_dirs(self, tmp_path):
        from scripts.monitor import discover_phases

        for name in ["phase_a", "phase_b", "phase_c", "phase_d", "phase_e"]:
            (tmp_path / name).mkdir()
        # Canonical names should be discovered
        (tmp_path / "baseline").mkdir()
        (tmp_path / "market").mkdir()

        phases = discover_phases(tmp_path)
        assert "baseline" in phases
        assert "market" in phases
        for legacy in ["phase_a", "phase_b", "phase_c", "phase_d", "phase_e"]:
            assert legacy not in phases, f"{legacy} should be skipped"

    def test_skips_archive_dirs(self, tmp_path):
        from scripts.monitor import discover_phases

        (tmp_path / "_archive").mkdir()
        (tmp_path / "_attic_pre_factorial").mkdir()
        (tmp_path / "ablation").mkdir()

        phases = discover_phases(tmp_path)
        assert "ablation" in phases
        assert "_archive" not in phases
        assert "_attic_pre_factorial" not in phases

    def test_skips_analysis_and_test_dirs(self, tmp_path):
        from scripts.monitor import discover_phases

        for name in ["analysis", "local", "test_cleanup", "test_signal"]:
            (tmp_path / name).mkdir()
        (tmp_path / "slicing").mkdir()

        phases = discover_phases(tmp_path)
        assert "slicing" in phases
        for skip in ["analysis", "local", "test_cleanup", "test_signal"]:
            assert skip not in phases


# ---------------------------------------------------------------------------
# phase_summary: phantom inflation
# ---------------------------------------------------------------------------


class TestPhaseSummary:
    """phase_summary must not inflate done counts from phantom dirs."""

    def test_phase_summary_excludes_phantom_dirs(self, tmp_path):
        from scripts.monitor import phase_summary

        phase_dir = tmp_path / "ablation"
        phase_dir.mkdir()

        # 2 real runs (csv + dir)
        for run_id in ["run_a", "run_b"]:
            (phase_dir / run_id / "vm1").mkdir(parents=True)
            (phase_dir / f"{run_id}.csv").write_text("header\nrow\n")

        # 3 phantom dirs (no csv)
        for run_id in ["phantom_1", "phantom_2", "phantom_3"]:
            (phase_dir / run_id / "vm3").mkdir(parents=True)

        summary = phase_summary(phase_dir)
        assert summary["done"] == 2, (
            f"Expected 2 real done, got {summary['done']} "
            f"(phantom dirs must not inflate count)"
        )
        assert summary["total"] == 2
