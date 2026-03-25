"""Tests for --warmup / --measurement CLI overrides in phase_main.

These flags allow short smoke runs (e.g. --warmup 15 --measurement 30)
without modifying per-phase runner code.
"""

from __future__ import annotations

import subprocess
import sys


def _dry_run(runner: str, extra_args: list[str] | None = None) -> str:
    """Run a phase runner with --dry-run and return stdout+stderr."""
    cmd = [sys.executable, "-m", runner, "--dry-run", "--configs", "neural", "--seeds", "42"]
    if extra_args:
        cmd.extend(extra_args)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    return result.stdout + result.stderr


class TestWarmupMeasurementOverride:
    """All phase runners that use phase_main should accept --warmup/--measurement."""

    def test_baseline_default_duration(self):
        output = _dry_run("scripts.run_baseline", ["--rates", "medium", "--transports", "http"])
        assert "duration=720s" in output, f"Expected default 720s, got: {output[-200:]}"

    def test_baseline_short_smoke(self):
        output = _dry_run("scripts.run_baseline", [
            "--rates", "medium", "--transports", "http",
            "--warmup", "15", "--measurement", "30",
        ])
        assert "duration=45s" in output, f"Expected 45s smoke, got: {output[-200:]}"

    def test_slicing_default_duration(self):
        output = _dry_run("scripts.run_slicing")
        assert "duration=720s" in output, f"Expected default 720s, got: {output[-200:]}"

    def test_slicing_short_smoke(self):
        output = _dry_run("scripts.run_slicing", ["--warmup", "15", "--measurement", "30"])
        assert "duration=45s" in output, f"Expected 45s smoke, got: {output[-200:]}"

    def test_contention_short_smoke(self):
        output = _dry_run("scripts.run_contention", [
            "--configs", "20pps",
            "--warmup", "15", "--measurement", "30",
        ])
        assert "duration=45s" in output, f"Expected 45s smoke, got: {output[-200:]}"

    def test_resilience_short_smoke(self):
        output = _dry_run("scripts.run_resilience", [
            "--configs", "embb-kill",
            "--warmup", "15", "--measurement", "30",
        ])
        assert "duration=45s" in output, f"Expected 45s smoke, got: {output[-200:]}"

    def test_stress_short_smoke(self):
        output = _dry_run("scripts.run_stress", [
            "--configs", "10pps-rr-nofail",
            "--warmup", "15", "--measurement", "30",
        ])
        assert "duration=45s" in output, f"Expected 45s smoke, got: {output[-200:]}"
