"""Tests for the repo-root run-experiments.sh dispatcher.

Validates that the dispatcher correctly forwards flags to the Python
orchestrators for all phases.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from scripts._common import PROJECT_ROOT

SCRIPT_ROOT = str(PROJECT_ROOT / "run-experiments.sh")


def run_root_script(*args: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run the repo-root run-experiments.sh dispatcher."""
    return subprocess.run(
        [SCRIPT_ROOT, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(PROJECT_ROOT),
        env={**os.environ, "TMUX": "fake-session", "PYTHONPATH": str(PROJECT_ROOT)},
    )


class TestDispatcherSlicingPassthrough:
    """The dispatcher must forward --configs to the slicing orchestrator."""

    def test_configs_flag_forwarded_to_slicing(self):
        """run-experiments.sh slicing --configs flat --dry-run should
        only plan flat runs (10), not all 50."""
        result = run_root_script("slicing", "--configs", "flat", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "10 runs planned" in output, (
            f"--configs flat should produce 10 runs.\nOutput:\n{output[-2000:]}"
        )

    def test_configs_flag_forwarded_multiple(self):
        """--configs flat,neural should produce 20 runs."""
        result = run_root_script("slicing", "--configs", "flat,neural", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "20 runs planned" in output, (
            f"--configs flat,neural should produce 20 runs.\nOutput:\n{output[-2000:]}"
        )

    def test_dry_run_forwarded_to_slicing(self):
        result = run_root_script("slicing", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "DRY RUN" in output.upper()

    def test_resume_forwarded_to_slicing(self):
        result = run_root_script("slicing", "--resume", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "RESUME" in output.upper()

    def test_legacy_phase_b_alias(self):
        """phase-b should still work as a legacy alias."""
        result = run_root_script("phase-b", "--configs", "flat", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Legacy phase-b alias failed:\n{output}"


class TestDispatcherResiliencePassthrough:
    """The dispatcher must forward --configs and --dry-run to resilience."""

    def test_resilience_dry_run(self):
        result = run_root_script("resilience", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "DRY RUN" in output.upper()

    def test_resilience_configs_forwarded(self):
        result = run_root_script("resilience", "--configs", "embb-kill", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "embb-kill" in output

    def test_legacy_phase_d_alias(self):
        result = run_root_script("phase-d", "--configs", "embb-kill", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Legacy phase-d alias failed:\n{output}"


class TestDispatcherNewFlags:
    """Tests for --strategy, --warmup, --measurement forwarding."""

    def test_strategy_flag_parsed(self):
        result = run_root_script("resilience", "--strategy", "S1,S2", "--dry-run", timeout=15)
        assert result.returncode == 0

    def test_strategy_forwarded_to_python(self):
        result = run_root_script("resilience", "--strategy", "S1", "--configs", "embb-kill", "--seeds", "42", "--dry-run", timeout=15)
        combined = result.stdout + result.stderr
        assert "S1" in combined
        assert "embb-kill_failure-worker_S1_seed-42" in combined

    def test_warmup_flag_forwarded(self):
        result = run_root_script("resilience", "--warmup", "30", "--configs", "embb-kill", "--seeds", "42", "--dry-run", timeout=15)
        combined = result.stdout + result.stderr
        assert "duration=630s" in combined

    def test_measurement_flag_forwarded(self):
        result = run_root_script("resilience", "--measurement", "120", "--configs", "embb-kill", "--seeds", "42", "--dry-run", timeout=15)
        combined = result.stdout + result.stderr
        assert "duration=240s" in combined

    def test_both_timing_flags_forwarded(self):
        result = run_root_script("resilience", "--warmup", "30", "--measurement", "120", "--configs", "embb-kill", "--seeds", "42", "--dry-run", timeout=15)
        combined = result.stdout + result.stderr
        assert "duration=150s" in combined


class TestDispatcherStress:
    """The dispatcher must forward --configs and --dry-run to stress."""

    def test_stress_dry_run(self):
        result = run_root_script("stress", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "DRY RUN" in output.upper()

    def test_stress_configs_forwarded(self):
        result = run_root_script("stress", "--configs", "20pps-rr-fail", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "5 runs planned" in output

    def test_legacy_phase_e_alias(self):
        result = run_root_script("phase-e", "--configs", "20pps-rr-fail", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Legacy phase-e alias failed:\n{output}"


class TestDispatcherBaseline:
    """The dispatcher must forward flags to baseline."""

    def test_baseline_dry_run(self):
        result = run_root_script("baseline", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"
        assert "DRY RUN" in output.upper()

    def test_legacy_phase_a_alias(self):
        result = run_root_script("phase-a", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Legacy phase-a alias failed:\n{output}"


class TestDispatcherContention:
    """The dispatcher must forward flags to contention."""

    def test_contention_dry_run(self):
        result = run_root_script("contention", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Script failed:\n{output}"

    def test_legacy_phase_a6_alias(self):
        result = run_root_script("phase-a6", "--dry-run")
        output = result.stdout + result.stderr
        assert result.returncode == 0, f"Legacy phase-a6 alias failed:\n{output}"


class TestDispatcherFederation:
    """The dispatcher must recognize federation action."""

    def test_federation_requires_host_d2(self):
        """federation requires HOST_D2 to be set."""
        env = {**os.environ, "TMUX": "fake-session", "PYTHONPATH": str(PROJECT_ROOT)}
        env.pop("HOST_D2", None)
        result = subprocess.run(
            [SCRIPT_ROOT, "federation", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT), env=env,
        )
        assert result.returncode != 0


class TestDispatcherHelp:
    """Help text should mention new command names."""

    def test_help_mentions_new_names(self):
        result = run_root_script("help")
        output = result.stdout + result.stderr
        for name in ["baseline", "placement", "slicing", "federation", "resilience", "stress", "contention"]:
            assert name in output, f"Help should mention {name}.\nOutput:\n{output}"
