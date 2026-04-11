"""Tests for monitor.py fixes: import robustness and multi-directory support.

Covers:
  1. Import robustness: monitor.py importable without ModuleNotFoundError
  2. --all flag: argparse accepts it, defaults to False
  3. Phase discovery: scanning results/ finds all phase directories
  4. Archive skipping: _archive, _attic*, analysis are excluded
  5. Per-phase summary: correct counts per phase
  6. Single-dir regression: existing single-directory mode unchanged
  7. Remote+all: --all + --remote produces valid output structure
"""

from __future__ import annotations

import importlib
import json
import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ── Test 1: Import robustness ───────────────────────────────────────────

def test_monitor_importable_directly():
    """Importing monitor.py must not raise ModuleNotFoundError.

    This simulates the case where scripts/ is NOT on sys.path (direct
    invocation: ``python scripts/monitor.py``).  The module should still
    import successfully because it adds PROJECT_ROOT to sys.path.
    """
    # Force a fresh import so the sys.path fix actually runs
    mod_name = "scripts.monitor"
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    # Import must succeed without error
    mod = importlib.import_module(mod_name)
    assert hasattr(mod, "main")
    assert hasattr(mod, "render")


# ── Test 2: --all flag in argparse ──────────────────────────────────────

def test_all_flag_in_argparse():
    """The --all flag must exist and default to False."""
    from scripts.monitor import main
    import argparse

    # Build parser the same way main() does — we need to peek at the parser
    from scripts.monitor import _build_parser
    parser = _build_parser()

    args = parser.parse_args([])
    assert hasattr(args, "all_phases")
    assert args.all_phases is False


def test_all_flag_set_true():
    """Passing --all sets all_phases to True."""
    from scripts.monitor import _build_parser
    parser = _build_parser()
    args = parser.parse_args(["--all"])
    assert args.all_phases is True


# ── Test 3: Discover all phases ─────────────────────────────────────────

def test_discover_all_phases(tmp_path):
    """discover_phases() finds all subdirectories under results/."""
    from scripts.monitor import discover_phases

    # Create phase directories
    for name in ["resilience", "slicing", "stress", "baseline", "federation"]:
        (tmp_path / name).mkdir()

    # Create a regular file (should be ignored)
    (tmp_path / "summary.csv").write_text("a,b\n1,2\n")

    phases = discover_phases(tmp_path)
    assert set(phases) == {"resilience", "slicing", "stress", "baseline", "federation"}


# ── Test 4: Skip archive directories ────────────────────────────────────

def test_skip_archive_directories(tmp_path):
    """_archive, _attic_pre_factorial, and analysis are excluded."""
    from scripts.monitor import discover_phases

    for name in ["resilience", "_archive", "_attic_pre_factorial", "analysis", "slicing"]:
        (tmp_path / name).mkdir()

    phases = discover_phases(tmp_path)
    assert "_archive" not in phases
    assert "_attic_pre_factorial" not in phases
    assert "analysis" not in phases
    assert "resilience" in phases
    assert "slicing" in phases


def test_skip_directories_starting_with_underscore(tmp_path):
    """Any directory starting with _ is excluded."""
    from scripts.monitor import discover_phases

    for name in ["baseline", "_anything", "__pycache__"]:
        (tmp_path / name).mkdir()

    phases = discover_phases(tmp_path)
    assert phases == ["baseline"]


# ── Test 5: Summary per phase ───────────────────────────────────────────

def test_summary_per_phase(tmp_path):
    """phase_summary() returns correct counts for each phase."""
    from scripts.monitor import phase_summary

    # Create a results dir with progress data
    phase_dir = tmp_path / "resilience"
    phase_dir.mkdir()

    progress = {
        "run1": {"status": "done", "timestamp": "2026-03-25T10:00:00"},
        "run2": {"status": "done", "timestamp": "2026-03-25T10:05:00"},
        "run3": {"status": "running", "timestamp": "2026-03-25T10:10:00"},
        "run4": {"status": "queued", "timestamp": ""},
        "run5": {"status": "failed", "detail": "timeout"},
    }
    (phase_dir / ".progress.json").write_text(json.dumps(progress))

    summary = phase_summary(phase_dir)
    assert summary["phase"] == "resilience"
    assert summary["total"] == 5
    assert summary["done"] == 2
    assert summary["running"] == 1
    assert summary["failed"] == 1


def test_summary_empty_phase(tmp_path):
    """phase_summary() handles a phase with no results."""
    from scripts.monitor import phase_summary

    phase_dir = tmp_path / "stress"
    phase_dir.mkdir()

    summary = phase_summary(phase_dir)
    assert summary["total"] == 0
    assert summary["done"] == 0
    assert summary["running"] == 0
    assert summary["failed"] == 0


def test_summary_from_csv_files(tmp_path):
    """phase_summary() falls back to CSV discovery when no .progress.json."""
    from scripts.monitor import phase_summary

    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()

    # Create some result CSVs (simulating completed runs)
    (phase_dir / "A1_rate-medium_seed-42.csv").write_text("col1,col2\n1,2\n")
    (phase_dir / "A2_rate-medium_seed-42.csv").write_text("col1,col2\n1,2\n")
    (phase_dir / "A3_rate-medium_seed-42.FAILED").write_text("")

    summary = phase_summary(phase_dir)
    assert summary["total"] == 3
    assert summary["done"] == 2
    assert summary["failed"] == 1


# ── Test 6: Single-dir still works (regression) ────────────────────────

def test_single_dir_still_works(tmp_path):
    """Existing single-directory mode must continue to work unchanged.

    The --all flag should not break the default behavior of watching a
    single results directory.
    """
    from scripts.monitor import _build_parser

    parser = _build_parser()

    # Default: single directory mode
    args = parser.parse_args([])
    assert args.all_phases is False
    assert args.results_dir == "results/baseline"

    # Explicit directory: single directory mode
    args = parser.parse_args(["results/resilience"])
    assert args.all_phases is False
    assert args.results_dir == "results/resilience"


def test_all_flag_ignores_positional_dir():
    """When --all is set, the positional results_dir is irrelevant."""
    from scripts.monitor import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["--all"])
    assert args.all_phases is True


# ── Test 7: --all + --remote ────────────────────────────────────────────

def test_render_all_phases_output(tmp_path, capsys):
    """render_all_phases() produces a structured multi-phase summary."""
    from scripts.monitor import render_all_phases, phase_summary

    # Set up two phases with progress
    resilience_dir = tmp_path / "resilience"
    resilience_dir.mkdir()
    progress_r = {
        "run1": {"status": "done", "timestamp": "2026-03-25T10:00:00"},
        "run2": {"status": "running", "timestamp": "2026-03-25T10:10:00"},
    }
    (resilience_dir / ".progress.json").write_text(json.dumps(progress_r))

    slicing_dir = tmp_path / "slicing"
    slicing_dir.mkdir()
    # Empty phase (no results yet)

    phases = ["resilience", "slicing"]
    summaries = [phase_summary(tmp_path / p) for p in phases]

    render_all_phases(summaries, results_root=tmp_path)

    captured = capsys.readouterr()
    assert "All Phases" in captured.out
    assert "resilience" in captured.out
    assert "slicing" in captured.out
    # Check table structure has total/done/running/failed columns
    assert "Total" in captured.out or "total" in captured.out.lower()
    assert "Done" in captured.out or "done" in captured.out.lower()


def test_discover_phases_remote(tmp_path):
    """discover_phases with remote_host uses SSH, not local filesystem."""
    from unittest.mock import patch
    from scripts.monitor import discover_phases

    ssh_output = "/home/user/results/resilience/\n/home/user/results/slicing/\n/home/user/results/_archive/\n"

    with patch("subprocess.check_output", return_value=ssh_output) as mock_ssh:
        phases = discover_phases(tmp_path / "results", remote_host="test-host")

    mock_ssh.assert_called_once()
    assert "resilience" in phases
    assert "slicing" in phases
    assert "_archive" not in phases  # skipped


def test_discover_phases_remote_passes_host(tmp_path):
    """discover_phases passes remote_host to SSH command."""
    from unittest.mock import patch
    from scripts.monitor import discover_phases

    with patch("subprocess.check_output", return_value="") as mock_ssh:
        discover_phases(tmp_path / "results", remote_host="my-server")

    cmd = mock_ssh.call_args[0][0]
    assert cmd[1] == "my-server"


# ── Test 8: per-phase progress bar and ETA ──────────────────────────────

def test_phase_summary_includes_fraction_and_eta(tmp_path):
    """phase_summary must return fraction and eta_s for rendering progress bars."""
    from scripts.monitor import phase_summary
    from datetime import datetime, timedelta

    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()
    now = datetime.now()

    progress = {
        "run1": {"status": "done", "timestamp": (now - timedelta(minutes=30)).isoformat()},
        "run2": {"status": "done", "timestamp": (now - timedelta(minutes=18)).isoformat()},
        "run3": {"status": "running", "timestamp": (now - timedelta(minutes=5)).isoformat()},
        "run4": {"status": "queued", "timestamp": ""},
    }
    (phase_dir / ".progress.json").write_text(json.dumps(progress))

    summary = phase_summary(phase_dir)
    assert "fraction" in summary, "phase_summary must include 'fraction'"
    assert "eta_s" in summary, "phase_summary must include 'eta_s'"
    assert 0 <= summary["fraction"] <= 1.0
    assert summary["fraction"] > 0  # 2 done out of 4
    assert summary["eta_s"] >= 0 or summary["eta_s"] == -1


def test_phase_fraction_with_partial_credit(tmp_path):
    """Phase fraction should give partial credit for running runs."""
    from scripts.monitor import phase_summary
    from datetime import datetime, timedelta

    phase_dir = tmp_path / "slicing"
    phase_dir.mkdir()
    now = datetime.now()

    # 1 done, 1 running (halfway through based on DEFAULT_RUN_DURATION_S)
    progress = {
        "run1": {"status": "done", "timestamp": (now - timedelta(minutes=12)).isoformat()},
        "run2": {"status": "running", "timestamp": (now - timedelta(minutes=6)).isoformat()},
    }
    (phase_dir / ".progress.json").write_text(json.dumps(progress))

    summary = phase_summary(phase_dir)
    # Fraction should be > 0.5 (1 done + partial credit for running)
    assert summary["fraction"] > 0.5, (
        f"Expected fraction > 0.5 with 1 done + 1 running, got {summary['fraction']}"
    )


def test_render_all_phases_shows_per_phase_bars(tmp_path, capsys):
    """render_all_phases must show a progress bar per phase."""
    from scripts.monitor import render_all_phases, phase_summary

    phase_dir = tmp_path / "resilience"
    phase_dir.mkdir()
    progress = {
        "run1": {"status": "done", "timestamp": "2026-03-25T10:00:00"},
        "run2": {"status": "done", "timestamp": "2026-03-25T10:12:00"},
        "run3": {"status": "queued", "timestamp": ""},
    }
    (phase_dir / ".progress.json").write_text(json.dumps(progress))

    summaries = [phase_summary(phase_dir)]
    render_all_phases(summaries, results_root=tmp_path)

    captured = capsys.readouterr()
    # Should contain Unicode block characters from progress_bar
    assert "\u2588" in captured.out or "\u2591" in captured.out, (
        "Per-phase progress bar must use block characters"
    )
    assert "ETA" in captured.out or "eta" in captured.out.lower(), (
        "Per-phase ETA must be shown"
    )


# ── Test 9: monitor reads .progress.json written by phase_main ─────────

def test_monitor_reads_progress_from_distributed_runs(tmp_path):
    """Monitor must read .progress.json that phase_main writes during
    distributed runs, showing correct running/queued/done counts."""
    from scripts.monitor import phase_summary

    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()
    from datetime import datetime, timedelta
    now = datetime.now()

    progress = {
        "rr_http_rate-medium_stages-3_seed-42": {
            "status": "done",
            "detail": "results/baseline/rr_http_rate-medium_stages-3_seed-42.csv",
            "timestamp": (now - timedelta(minutes=14)).isoformat(),
        },
        "rr_http_rate-medium_stages-3_seed-123": {
            "status": "running",
            "detail": "",
            "timestamp": (now - timedelta(minutes=2)).isoformat(),
        },
        "rr_http_rate-medium_stages-3_seed-456": {
            "status": "queued",
            "detail": "",
            "timestamp": "",
        },
        "random_http_rate-medium_stages-3_seed-42": {
            "status": "queued",
            "detail": "",
            "timestamp": "",
        },
    }
    (phase_dir / ".progress.json").write_text(json.dumps(progress))

    summary = phase_summary(phase_dir)
    assert summary["done"] == 1
    assert summary["running"] == 1
    assert summary["queued"] == 2
    assert summary["total"] == 4
    assert summary["fraction"] > 0.25  # 1 done + partial credit for running
    assert summary["fraction"] < 0.75  # but not too much
    assert summary["eta_s"] > 0


# ── Test 10: discover distributed run directories ──────────────────────

def test_discover_progress_finds_distributed_run_dirs(tmp_path):
    """Merged progress must detect distributed run directories
    (subdirs with vm1/vm2/... inside) as completed runs, but ONLY
    when a sibling .csv exists (the workload's primary output).

    A directory with vm*/ children but no .csv is a phantom from an
    aborted run and must NOT be counted. See L50 in Tasks/lessons.md.
    """
    from scripts.monitor import (
        _discover_progress_from_csvs, _discover_distributed_runs, merge_progress,
    )

    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()

    # Simulate 3 completed distributed runs (each has vm1/ subdir + sibling .csv)
    for run_name in ["rr_seed-42", "rr_seed-123", "neural_seed-42"]:
        run_dir = phase_dir / run_name / "vm1"
        run_dir.mkdir(parents=True)
        (phase_dir / f"{run_name}.csv").write_text("header\nrow1\n")

    # One old-style direct CSV (no vm*/ dir)
    (phase_dir / "old_run.csv").write_text("header\nrow1\n")

    # One phantom dir (vm3/ rsync residue, no .csv) — must NOT be found
    (phase_dir / "phantom" / "vm3").mkdir(parents=True)

    progress = merge_progress(
        _discover_progress_from_csvs(phase_dir),
        _discover_distributed_runs(phase_dir),
    )

    assert "old_run" in progress, "Should find direct CSV"
    assert "rr_seed-42" in progress, "Should find distributed run with .csv"
    assert "rr_seed-123" in progress, "Should find distributed run with .csv"
    assert "neural_seed-42" in progress, "Should find distributed run with .csv"
    assert "phantom" not in progress, "Phantom dir (no .csv) must NOT be found"
    assert len(progress) == 4
    assert all(v["status"] == "done" for v in progress.values())


def test_phase_summary_counts_distributed_runs(tmp_path):
    """phase_summary must count distributed run directories as done runs,
    but ONLY when a sibling .csv exists (the workload's primary output).
    """
    from scripts.monitor import phase_summary

    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()

    # 3 completed distributed runs (vm1/ dir + sibling .csv)
    for run_name in ["run_a", "run_b", "run_c"]:
        (phase_dir / run_name / "vm1").mkdir(parents=True)
        (phase_dir / f"{run_name}.csv").write_text("header\nrow1\n")

    summary = phase_summary(phase_dir)
    assert summary["done"] == 3
    assert summary["total"] == 3
    assert summary["fraction"] == 1.0


# ── Test 11: auto-exit guard ────────────────────────────────────────────

def test_monitor_does_not_exit_when_no_active_progress(tmp_path):
    """Monitor must NOT auto-exit when all progress comes from CSV discovery
    (no .progress.json with running/queued entries). This prevents premature
    exit between phases when the orchestrator is still running."""
    from scripts.monitor import load_progress, discover_phases

    # Phase with only CSV-discovered results (no .progress.json)
    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()
    (phase_dir / "run_a.csv").write_text("header\nrow\n")
    (phase_dir / "run_b.csv").write_text("header\nrow\n")

    # No .progress.json exists
    assert not (phase_dir / ".progress.json").exists()

    # load_progress returns empty → has_active_progress is False
    progress = load_progress(phase_dir)
    assert not progress, "No .progress.json should mean no active progress"


def test_monitor_exits_when_progress_shows_all_done(tmp_path):
    """Monitor should auto-exit when .progress.json exists and all entries are done."""
    from scripts.monitor import load_progress

    phase_dir = tmp_path / "baseline"
    phase_dir.mkdir()

    progress = {
        "run_a": {"status": "done", "timestamp": "2026-04-01T10:00:00"},
        "run_b": {"status": "done", "timestamp": "2026-04-01T10:12:00"},
    }
    (phase_dir / ".progress.json").write_text(json.dumps(progress))

    loaded = load_progress(phase_dir)
    assert loaded, "Active .progress.json should be truthy"
    assert all(v["status"] == "done" for v in loaded.values())


# ── Test 12: --distributed flag ────────────────────────────────────────

def test_distributed_flag_accepted():
    """Monitor argparser must accept --distributed flag."""
    from scripts.monitor import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["--distributed", "--once"])
    assert args.distributed is True


def test_distributed_flag_default_false():
    """Default distributed flag is False."""
    from scripts.monitor import _build_parser

    parser = _build_parser()
    args = parser.parse_args([])
    assert args.distributed is False


def test_get_distributed_containers():
    """get_distributed_containers queries all VMs and aggregates results."""
    from scripts.monitor import get_distributed_containers

    with patch("scripts.monitor.get_docker_containers") as mock_get:
        mock_get.side_effect = [
            [{"name": "deploy-broker-1", "status": "Up 2 min"},
             {"name": "deploy-worker-0-1", "status": "Up 2 min"}],
            [{"name": "deploy-broker-1", "status": "Up 2 min"}],
            [],
            [{"name": "deploy-broker-1", "status": "Up 1 min"}],
        ]
        result = get_distributed_containers()

    assert len(result) == 4  # entries from 3 VMs (one empty)
    assert mock_get.call_count == 4  # queried all 4 VMs


def test_discover_phases_local_ignores_remote_host(tmp_path):
    """discover_phases without remote_host uses local filesystem, not SSH."""
    from scripts.monitor import discover_phases

    (tmp_path / "resilience").mkdir()
    (tmp_path / "slicing").mkdir()

    phases = discover_phases(tmp_path, remote_host=None)
    assert "resilience" in phases
    assert "slicing" in phases
