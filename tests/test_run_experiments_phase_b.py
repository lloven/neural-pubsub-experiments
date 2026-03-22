"""Tests for Phase B integration in scripts/5gtn/run-experiments.sh.

Validates that the 5GTN VM bash orchestrator correctly maps Phase B
configs (B1, B1eq, B2, B3, B4) to compose overlays, environment
variables, transport dimensions, and result file naming.

The 5gtn script runs Docker directly (no Python), so we test via
--dry-run mode which prints the planned runs without invoking Docker.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from scripts._common import PROJECT_ROOT

SCRIPT_5GTN = str(PROJECT_ROOT / "scripts" / "5gtn" / "run-experiments.sh")


def run_5gtn_script(*args: str, timeout: int = 10) -> subprocess.CompletedProcess:
    """Run the 5gtn script with given arguments, return CompletedProcess."""
    return subprocess.run(
        [SCRIPT_5GTN, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(PROJECT_ROOT / "scripts" / "5gtn"),
        env={**os.environ, "TMUX": "fake-session"},  # prevent tmux wrapping
    )


def get_dry_run_lines(result: subprocess.CompletedProcess) -> list[str]:
    """Extract [dry-run] lines from stdout+stderr."""
    output = result.stdout + result.stderr
    return [line for line in output.splitlines() if "[dry-run]" in line.lower() or "DRY-RUN" in line or "dry_run" in line]


class TestPhaseBConfigMapping:
    """Verify each B config maps to the correct compose overlays."""

    def test_B1_uses_local_and_flat(self):
        """B1 should use docker-compose.local.yaml + docker-compose.flat.yaml."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B1")
        output = result.stdout + result.stderr
        # Find lines mentioning B1 compose files
        b1_lines = [l for l in output.splitlines() if "B1" in l and ("flat.yaml" in l or "compose" in l.lower())]
        # B1 must include flat overlay
        assert any("docker-compose.flat.yaml" in l for l in output.splitlines()), (
            f"B1 must use flat overlay.\nOutput:\n{output}"
        )
        assert "docker-compose.flat-equalized.yaml" not in output.replace("B1eq", ""), (
            "B1 must NOT use flat-equalized overlay"
        )

    def test_B1eq_uses_local_and_flat_equalized(self):
        """B1eq should use docker-compose.local.yaml + docker-compose.flat-equalized.yaml."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B1eq")
        output = result.stdout + result.stderr
        assert any("docker-compose.flat-equalized.yaml" in l for l in output.splitlines()), (
            f"B1eq must use flat-equalized overlay.\nOutput:\n{output}"
        )

    def test_B2_uses_local_only(self):
        """B2 uses docker-compose.local.yaml (base 3-slice)."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B2")
        output = result.stdout + result.stderr
        b2_lines = [l for l in output.splitlines() if "B2" in l and "compose" in l.lower()]
        # B2 should NOT use flat or governance overlays
        for line in b2_lines:
            assert "flat" not in line, f"B2 must not use flat overlay: {line}"
            assert "governance" not in line, f"B2 must not use governance overlay: {line}"

    def test_B3_uses_local_and_governance(self):
        """B3 should use docker-compose.local.yaml + docker-compose.governance.yaml."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B3")
        output = result.stdout + result.stderr
        assert any("docker-compose.governance.yaml" in l for l in output.splitlines()), (
            f"B3 must use governance overlay.\nOutput:\n{output}"
        )

    def test_B4_uses_local_and_governance(self):
        """B4 should use docker-compose.local.yaml + docker-compose.governance.yaml."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B4")
        output = result.stdout + result.stderr
        assert any("docker-compose.governance.yaml" in l for l in output.splitlines()), (
            f"B4 must use governance overlay.\nOutput:\n{output}"
        )

    def test_kafka_transport_adds_kafka_overlay(self):
        """Each config with kafka transport should add docker-compose.kafka.yaml."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B1")
        output = result.stdout + result.stderr
        assert any("docker-compose.kafka.yaml" in l for l in output.splitlines()), (
            f"Kafka transport must add kafka overlay.\nOutput:\n{output}"
        )


class TestPhaseBMatrixSize:
    """Verify Phase B generates the correct number of runs."""

    def test_full_matrix_is_50_runs(self):
        """5 configs x 2 transports x 5 seeds = 50 runs."""
        result = run_5gtn_script("phase-b", "--dry-run")
        output = result.stdout + result.stderr
        # Count "Would run:" lines (each run produces exactly one)
        run_lines = [l for l in output.splitlines() if "Would run:" in l]
        assert len(run_lines) == 50, (
            f"Expected 50 runs (5 configs x 2 transports x 5 seeds), got {len(run_lines)}.\n"
            f"Output:\n{output[-2000:]}"
        )

    def test_single_config_is_10_runs(self):
        """1 config x 2 transports x 5 seeds = 10 runs."""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B2")
        output = result.stdout + result.stderr
        # Count "Would run:" lines only
        run_lines = [l for l in output.splitlines() if "Would run:" in l]
        assert len(run_lines) == 10, (
            f"Expected 10 runs for B2 (1 config x 2 transports x 5 seeds), got {len(run_lines)}.\n"
            f"Output:\n{output[-2000:]}"
        )


class TestPhaseBResume:
    """Verify --resume skips runs with existing CSVs."""

    def test_resume_skips_existing_csv(self):
        """With --resume, existing CSV files should be skipped."""
        # The 5gtn script cd's to SCRIPT_DIR, so we create a fake result
        # in the script's own results/phase_b/ directory.
        script_dir = Path(SCRIPT_5GTN).parent
        results_dir = script_dir / "results" / "phase_b"
        results_dir.mkdir(parents=True, exist_ok=True)
        fake_csv = results_dir / "B1_http_rate-medium_stages-3_seed-42.csv"
        created = not fake_csv.exists()
        fake_csv.write_text("header\nrow\n")

        try:
            result = run_5gtn_script("phase-b", "--dry-run", "--resume", "--configs", "B1")
            output = result.stdout + result.stderr
            # The skipped run should be mentioned as "already done"
            assert any(
                "skip" in l.lower() and "B1" in l and "seed-42" in l
                for l in output.splitlines()
            ) or any(
                "already" in l.lower() and "B1" in l and "seed-42" in l
                for l in output.splitlines()
            ), (
                f"--resume should skip B1/http/seed-42 (CSV exists).\nOutput:\n{output}"
            )
            # Other seeds should still appear as "Would run"
            assert any("Would run:" in l and "seed-123" in l for l in output.splitlines()), (
                f"Other seeds should still be planned.\nOutput:\n{output}"
            )
        finally:
            # Clean up the fake CSV only if we created it
            if created:
                fake_csv.unlink(missing_ok=True)


class TestPhaseBResultNaming:
    """Verify output files follow the expected naming pattern."""

    def test_result_file_pattern(self):
        """Results should follow: results/phase_b/{CONFIG}_{TRANSPORT}_rate-medium_stages-3_seed-{SEED}.csv"""
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", "B3")
        output = result.stdout + result.stderr
        # Check for expected result file paths
        pattern = r"results/phase_b/B3_(http|kafka)_rate-medium_stages-3_seed-\d+\.csv"
        matches = re.findall(pattern, output)
        assert len(matches) >= 1, (
            f"Expected result file pattern 'results/phase_b/B3_{{transport}}_rate-medium_stages-3_seed-{{seed}}.csv'.\n"
            f"Output:\n{output[-2000:]}"
        )


class TestPhaseBEnvVars:
    """Verify each config sets correct environment variables."""

    def _get_env_output(self, config: str) -> str:
        result = run_5gtn_script("phase-b", "--dry-run", "--configs", config)
        return result.stdout + result.stderr

    def test_B1_env_vars(self):
        """B1: NUM_SLICES=1, GOVERNANCE_ENABLED=false."""
        output = self._get_env_output("B1")
        assert "NUM_SLICES=1" in output, f"B1 must set NUM_SLICES=1.\nOutput:\n{output}"
        assert "GOVERNANCE_ENABLED=false" in output, f"B1 must set GOVERNANCE_ENABLED=false.\nOutput:\n{output}"

    def test_B1eq_env_vars(self):
        """B1eq: NUM_SLICES=1, GOVERNANCE_ENABLED=false."""
        output = self._get_env_output("B1eq")
        assert "NUM_SLICES=1" in output, f"B1eq must set NUM_SLICES=1.\nOutput:\n{output}"
        assert "GOVERNANCE_ENABLED=false" in output, f"B1eq must set GOVERNANCE_ENABLED=false.\nOutput:\n{output}"

    def test_B2_env_vars(self):
        """B2: NUM_SLICES=3, GOVERNANCE_ENABLED=false."""
        output = self._get_env_output("B2")
        assert "NUM_SLICES=3" in output, f"B2 must set NUM_SLICES=3.\nOutput:\n{output}"
        assert "GOVERNANCE_ENABLED=false" in output, f"B2 must set GOVERNANCE_ENABLED=false.\nOutput:\n{output}"

    def test_B3_env_vars(self):
        """B3: NUM_SLICES=3, GOVERNANCE_ENABLED=true."""
        output = self._get_env_output("B3")
        assert "NUM_SLICES=3" in output, f"B3 must set NUM_SLICES=3.\nOutput:\n{output}"
        assert "GOVERNANCE_ENABLED=true" in output, f"B3 must set GOVERNANCE_ENABLED=true.\nOutput:\n{output}"

    def test_B4_env_vars(self):
        """B4: NUM_SLICES=3, GOVERNANCE_ENABLED=true, FAILURE_DELAY_S=300."""
        output = self._get_env_output("B4")
        assert "NUM_SLICES=3" in output, f"B4 must set NUM_SLICES=3.\nOutput:\n{output}"
        assert "GOVERNANCE_ENABLED=true" in output, f"B4 must set GOVERNANCE_ENABLED=true.\nOutput:\n{output}"
        assert "FAILURE_DELAY_S=300" in output, f"B4 must set FAILURE_DELAY_S=300.\nOutput:\n{output}"

    def test_placement_strategy_neural(self):
        """All Phase B configs should use PLACEMENT_STRATEGY=neural."""
        for config in ["B1", "B1eq", "B2", "B3", "B4"]:
            output = self._get_env_output(config)
            assert "PLACEMENT_STRATEGY=neural" in output, (
                f"{config} must set PLACEMENT_STRATEGY=neural.\nOutput:\n{output}"
            )


class TestPhaseBHelpText:
    """Verify help mentions Phase B."""

    def test_help_mentions_phase_b(self):
        """Help text should list phase-b command."""
        result = run_5gtn_script("help")
        output = result.stdout + result.stderr
        assert "phase-b" in output, f"Help should mention phase-b.\nOutput:\n{output}"


class TestDeployPhaseB:
    """Verify deploy.sh copies Phase B compose overlays."""

    def test_deploy_copies_flat_overlay(self):
        """deploy.sh should copy docker-compose.flat.yaml."""
        deploy_script = (PROJECT_ROOT / "scripts" / "5gtn" / "deploy.sh").read_text()
        # The glob cp "$REPO_DIR"/docker-compose.*.yaml catches all overlays
        assert "docker-compose.*.yaml" in deploy_script, (
            "deploy.sh should copy all compose overlays via glob pattern"
        )

    def test_deploy_copies_phase_b_runner(self):
        """deploy.sh should copy run_phase_b.py to the VM."""
        deploy_script = (PROJECT_ROOT / "scripts" / "5gtn" / "deploy.sh").read_text()
        assert "run_phase_b.py" in deploy_script, (
            "deploy.sh should copy run_phase_b.py to the VM"
        )
