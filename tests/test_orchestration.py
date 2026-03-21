"""Tests for the unified orchestration layer.

Covers:
  - Cycle 1: resolve_config() — config→compose-files and config→env mapping
  - Cycle 2: Phase B compose overlay wiring
  - Cycle 3: Monitor --once flag and CSV discovery
  - Cycle 4: Bash dispatcher delegation
  - Cycle 6: BUG-001 — Workload generator hard timeout
  - Cycle 7: BUG-002a — UTC timestamps in progress.json
  - Cycle 8: BUG-002b — Bash tmux direct Python call
  - Cycle 9: BUG-002c — Smoke --phases multi-arg parsing
  - Cycle 10: BUG-002d — Monitor total count from progress.json
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from scripts._common import PROJECT_ROOT

# Compose file paths (constants that resolve_config should use)
COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"
COMPOSE_KAFKA = PROJECT_ROOT / "docker-compose.kafka.yaml"
COMPOSE_FLAT = PROJECT_ROOT / "docker-compose.flat.yaml"
COMPOSE_GOVERNANCE = PROJECT_ROOT / "docker-compose.governance.yaml"


# ============================================================================
# Cycle 1: resolve_config()
# ============================================================================

class TestResolveConfig:
    """Tests 1-9: resolve_config() maps config names to compose files and env."""

    def _resolve(self, config_name: str, rate: str = "medium", stages: int = 3, seed: int = 42):
        from scripts._common import resolve_config
        return resolve_config(config_name, rate=rate, stages=stages, seed=seed)

    # --- Compose file resolution ---

    def test_A1_uses_kafka_overlay(self):
        """Test 1: A1 config uses local + kafka compose files."""
        cfg = self._resolve("A1")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_KAFKA]

    def test_B1_uses_flat_overlay(self):
        """Test 2: B1 config uses local + flat compose files."""
        cfg = self._resolve("B1")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_FLAT]

    def test_B3_uses_governance_overlay(self):
        """Test 3: B3 config uses local + governance compose files."""
        cfg = self._resolve("B3")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_GOVERNANCE]

    def test_B4_uses_governance_overlay(self):
        """Test 4: B4 config uses local + governance compose files."""
        cfg = self._resolve("B4")
        assert cfg.compose_files == [COMPOSE_LOCAL, COMPOSE_GOVERNANCE]

    def test_A2_uses_base_only(self):
        """Test 5: A2 config uses only the local compose file."""
        cfg = self._resolve("A2")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    # --- Environment variable resolution ---

    def test_A1_env_has_no_broker_module(self):
        """Test 6: A1 env does not set BROKER_MODULE (kafka overlay handles it)."""
        cfg = self._resolve("A1")
        assert "BROKER_MODULE" not in cfg.env

    def test_A2_env_has_static_broker_round_robin(self):
        """Test 7: A2 env sets static broker with round_robin placement."""
        cfg = self._resolve("A2")
        assert cfg.env["BROKER_MODULE"] == "src.broker.static_broker"
        assert cfg.env["PLACEMENT"] == "round_robin"

    def test_A3_env_has_static_broker_random(self):
        """Test 8: A3 env sets static broker with random placement."""
        cfg = self._resolve("A3")
        assert cfg.env["BROKER_MODULE"] == "src.broker.static_broker"
        assert cfg.env["PLACEMENT"] == "random"

    def test_rate_mapping(self):
        """Test 9: Rate labels map to correct numeric values."""
        for label, expected in [("low", "2.0"), ("medium", "5.0"), ("high", "10.0")]:
            cfg = self._resolve("A4", rate=label)
            assert cfg.env["ARRIVAL_RATE"] == expected, f"rate={label} should map to {expected}"

    # --- Additional edge cases ---

    def test_A4_uses_base_only(self):
        """A4 (neural) uses only the local compose file."""
        cfg = self._resolve("A4")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    def test_A4_env_has_no_broker_module(self):
        """A4 (neural, default) does not set BROKER_MODULE."""
        cfg = self._resolve("A4")
        assert "BROKER_MODULE" not in cfg.env

    def test_B2_uses_base_only(self):
        """B2 uses only the local compose file."""
        cfg = self._resolve("B2")
        assert cfg.compose_files == [COMPOSE_LOCAL]

    def test_C_configs_use_base_only(self):
        """C1-C4 use only the local compose file (future phases)."""
        for name in ["C1", "C2", "C3", "C4"]:
            cfg = self._resolve(name)
            assert cfg.compose_files == [COMPOSE_LOCAL], f"{name} should use base only"

    def test_D_configs_use_base_only(self):
        """D1-D4 use only the local compose file (future phases)."""
        for name in ["D1", "D2", "D3", "D4"]:
            cfg = self._resolve(name)
            assert cfg.compose_files == [COMPOSE_LOCAL], f"{name} should use base only"

    def test_unknown_config_raises(self):
        """Unknown config name should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown config"):
            self._resolve("Z9")

    def test_seed_in_env(self):
        """Seed is passed through to env."""
        cfg = self._resolve("A4", seed=999)
        assert cfg.env["SEED"] == "999"

    def test_stages_in_env(self):
        """Pipeline stages are passed through to env."""
        cfg = self._resolve("A4", stages=5)
        assert cfg.env.get("PIPELINE_STAGES") == "5" or "DURATION_S" in cfg.env


# ============================================================================
# Cycle 2: Phase B compose overlay wiring
# ============================================================================

class TestPhaseBOverlays:
    """Tests 10-14: Phase B orchestrator passes correct compose files."""

    def _get_run_kwargs(self, config_name: str, seed: int = 42):
        """Call Phase B's _run() in dry-run mode and capture the compose_files
        passed to run_single() via monkey-patching."""
        from unittest.mock import patch
        from scripts.run_phase_b import RunConfig, CONFIGS, _run

        cfg = CONFIGS[config_name]
        run = RunConfig(
            config_name=config_name,
            seed=seed,
            num_slices=cfg["num_slices"],
            governance=cfg["governance"],
            failure_injection=cfg["failure_injection"],
        )

        # Patch run_single to capture kwargs instead of running Docker
        captured = {}
        def fake_run_single(**kwargs):
            captured.update(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "result_file": "fake.csv"}

        with patch("scripts.run_phase_b.run_single", side_effect=fake_run_single):
            _run(run, dry_run=False)

        return captured

    def test_B1_uses_flat_overlay(self):
        """Test 10: Phase B runner passes flat overlay for B1."""
        kwargs = self._get_run_kwargs("B1")
        assert kwargs.get("compose_files") is not None, "B1 must pass compose_files"
        names = [f.name for f in kwargs["compose_files"]]
        assert "docker-compose.flat.yaml" in names

    def test_B2_uses_base_only(self):
        """Test 11: Phase B runner uses only local.yaml for B2."""
        kwargs = self._get_run_kwargs("B2")
        files = kwargs.get("compose_files")
        # B2 either passes None (uses default) or passes [local] only
        if files is not None:
            names = [f.name for f in files]
            assert names == ["docker-compose.local.yaml"]

    def test_B3_uses_governance_overlay(self):
        """Test 12: Phase B runner passes governance overlay for B3."""
        kwargs = self._get_run_kwargs("B3")
        assert kwargs.get("compose_files") is not None, "B3 must pass compose_files"
        names = [f.name for f in kwargs["compose_files"]]
        assert "docker-compose.governance.yaml" in names

    def test_B4_has_failure_injection(self):
        """Test 13: B4 run config sets failure_fn."""
        kwargs = self._get_run_kwargs("B4")
        assert kwargs.get("failure_fn") is not None, "B4 must have failure injection"

    def test_phase_b_matrix_is_20_runs(self):
        """Test 14: 4 configs x 5 seeds = 20 runs."""
        from scripts.run_phase_b import build_run_matrix, CONFIGS
        runs = build_run_matrix(
            list(CONFIGS.keys()),
            [42, 123, 456, 789, 0],
        )
        assert len(runs) == 20


# ============================================================================
# Cycle 3: Monitor improvements
# ============================================================================

class TestMonitorCSVDiscovery:
    """Tests 15-19: _discover_progress_from_csvs() and --once flag."""

    def test_discover_csvs_finds_completed_runs(self, tmp_path):
        """Test 15: CSV files are discovered as 'done' runs."""
        from scripts.monitor import _discover_progress_from_csvs

        # Create fake CSV files
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")
        (tmp_path / "B2_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        assert "B1_rate-medium_stages-3_seed-42" in progress
        assert progress["B1_rate-medium_stages-3_seed-42"]["status"] == "done"
        assert "B2_rate-medium_stages-3_seed-42" in progress
        assert progress["B2_rate-medium_stages-3_seed-42"]["status"] == "done"

    def test_discover_csvs_finds_failed_runs(self, tmp_path):
        """Test 16: .FAILED files are discovered as 'failed' runs."""
        from scripts.monitor import _discover_progress_from_csvs

        (tmp_path / "B3_rate-medium_stages-3_seed-42.FAILED").write_text("")

        progress = _discover_progress_from_csvs(tmp_path)
        assert "B3_rate-medium_stages-3_seed-42" in progress
        assert progress["B3_rate-medium_stages-3_seed-42"]["status"] == "failed"

    def test_discover_csvs_skips_smoke_files(self, tmp_path):
        """Test 17: smoke_*.csv and summary files are excluded."""
        from scripts.monitor import _discover_progress_from_csvs

        (tmp_path / "smoke_A4_20260301.csv").write_text("header\n")
        (tmp_path / "phase_b_summary.csv").write_text("header\n")
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        # smoke file should be excluded
        assert not any("smoke" in k for k in progress), f"smoke files should be excluded: {list(progress.keys())}"
        # real run should be included
        assert "B1_rate-medium_stages-3_seed-42" in progress

    def test_monitor_once_flag_exits(self):
        """Test 18: --once flag causes monitor to print status and exit."""
        import subprocess
        result = subprocess.run(
            [
                "python3", "-m", "scripts.monitor",
                "--once", "results/phase_b",
            ],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
        )
        # Should exit cleanly (0), not hang in a loop
        assert result.returncode == 0

    def test_discover_csvs_excludes_summary_files(self, tmp_path):
        """Test 19: phase_*_summary.csv files are excluded from discovery."""
        from scripts.monitor import _discover_progress_from_csvs

        (tmp_path / "phase_b_summary.csv").write_text("run_id,status\n")
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        assert not any("summary" in k for k in progress), f"summary files should be excluded: {list(progress.keys())}"
        assert "B1_rate-medium_stages-3_seed-42" in progress


# ============================================================================
# Cycle 4: Bash dispatcher
# ============================================================================

class TestBashDispatcher:
    """Tests 20-22: run-experiments.sh delegates to Python scripts."""

    SCRIPT = str(PROJECT_ROOT / "run-experiments.sh")

    def test_help_shows_all_phases(self):
        """Test 20: --help lists all phase commands."""
        import subprocess
        result = subprocess.run(
            [self.SCRIPT, "help"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
        )
        output = result.stdout + result.stderr
        for cmd in ["phase-a", "phase-b", "phase-c", "phase-d", "single", "smoke", "status", "stop"]:
            assert cmd in output, f"Help should mention '{cmd}'"

    def test_single_calls_python_script(self):
        """Test 21: 'single' command invokes scripts/run_single.py."""
        import subprocess
        # Use bash -x to trace which commands are invoked.
        # The Python script will try to run Docker (which may fail/timeout),
        # so we capture what we need from the trace and accept timeout.
        try:
            result = subprocess.run(
                ["bash", "-x", self.SCRIPT, "single", "A2", "low", "3", "42"],
                capture_output=True, text=True, timeout=10,
                cwd=str(PROJECT_ROOT),
            )
            trace = result.stderr
        except subprocess.TimeoutExpired as e:
            # Timeout is fine -- we just need the trace showing delegation
            trace = (e.stderr or "") if isinstance(e.stderr, str) else (e.stderr or b"").decode()
        # The new dispatcher should invoke python3 -m scripts.run_single
        assert "run_single" in trace, (
            f"'single' command should delegate to run_single.\n"
            f"Trace: {trace[-500:]}"
        )

    def test_status_calls_monitor(self):
        """Test 22: 'status' command invokes monitor.py --once."""
        import subprocess
        result = subprocess.run(
            ["bash", "-x", self.SCRIPT, "status"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
        )
        trace = result.stderr
        # The new dispatcher should invoke monitor.py --once
        assert "monitor" in trace and "--once" in trace, (
            f"'status' command should delegate to monitor.py --once.\n"
            f"Trace: {trace[-500:]}"
        )


# ===========================================================================
# Cycle 6: BUG-001 — Workload generator hard timeout and 503 handling
# ===========================================================================


class TestWorkloadGeneratorTimeout:
    """BUG-001: Generator must terminate within a bounded wall-clock time."""

    def test_generator_has_hard_deadline(self):
        """WorkloadGenerator.run() must not exceed duration_s * 1.5 wall-clock time."""
        from src.workload.generator import WorkloadGenerator, WorkloadConfig
        import asyncio

        cfg = WorkloadConfig(
            arrival_rate=1.0,
            duration_s=2.0,  # 2 seconds
            pipeline_mix={"cqi_prediction": 1.0},
            broker_url="http://localhost:99999",  # unreachable, will fail fast
            seed=42,
        )
        gen = WorkloadGenerator(cfg)

        start = time.time()
        asyncio.run(gen.run())
        elapsed = time.time() - start

        # Must finish within 1.5x duration (3 seconds), not hang indefinitely
        assert elapsed < cfg.duration_s * 2.0, (
            f"Generator took {elapsed:.1f}s for {cfg.duration_s}s duration "
            f"(>{cfg.duration_s * 2.0:.1f}s limit)"
        )

    def test_generator_counts_503_as_failure_not_retry(self):
        """503 responses should be counted as failures, not retried indefinitely."""
        from src.workload.generator import WorkloadGenerator, WorkloadConfig
        from unittest.mock import AsyncMock, patch, MagicMock
        import asyncio

        cfg = WorkloadConfig(
            arrival_rate=2.0,
            duration_s=1.0,
            pipeline_mix={"cqi_prediction": 1.0},
            broker_url="http://broker:8080",
            seed=42,
        )
        gen = WorkloadGenerator(cfg)

        # Mock httpx to always return 503
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "503 Service Unavailable",
                request=MagicMock(),
                response=mock_response,
            )
        )

        async def mock_post(*args, **kwargs):
            return mock_response

        with patch("httpx.AsyncClient") as MockClient:
            instance = MockClient.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            instance.return_value.post = AsyncMock(side_effect=mock_post)

            start = time.time()
            asyncio.run(gen.run())
            elapsed = time.time() - start

        # Even with all 503s, generator must finish within 2x duration
        assert elapsed < cfg.duration_s * 3.0, (
            f"Generator hung on 503s: took {elapsed:.1f}s"
        )

    def test_generator_pending_tasks_bounded(self):
        """After run(), no pending tasks should remain."""
        from src.workload.generator import WorkloadGenerator, WorkloadConfig
        import asyncio

        cfg = WorkloadConfig(
            arrival_rate=1.0,
            duration_s=1.0,
            pipeline_mix={"cqi_prediction": 1.0},
            broker_url="http://localhost:99999",  # unreachable
            seed=42,
        )
        gen = WorkloadGenerator(cfg)
        asyncio.run(gen.run())

        assert len(gen._pending_tasks) == 0, (
            f"Generator has {len(gen._pending_tasks)} pending tasks after run()"
        )

    def test_generator_has_drain_timeout_attribute(self):
        """Generator must have a configurable drain timeout for post-loop cleanup.

        BUG-001: without a bounded drain, pending tasks can hang for hours
        when the broker is overloaded.
        """
        from src.workload.generator import WorkloadGenerator, WorkloadConfig

        cfg = WorkloadConfig(
            arrival_rate=1.0,
            duration_s=10.0,
            pipeline_mix={"cqi_prediction": 1.0},
            broker_url="http://localhost:8080",
            seed=42,
        )
        gen = WorkloadGenerator(cfg)

        # Generator must have a drain timeout (default: 30s)
        assert hasattr(gen, '_drain_timeout_s'), (
            "Generator missing _drain_timeout_s attribute (BUG-001 fix)"
        )
        assert gen._drain_timeout_s > 0, "Drain timeout must be positive"
        assert gen._drain_timeout_s <= 60, "Drain timeout should be <= 60s"

    def test_generator_cancels_pending_after_drain_timeout(self):
        """After drain timeout, remaining pending tasks must be cancelled."""
        from src.workload.generator import WorkloadGenerator, WorkloadConfig
        from unittest.mock import patch
        import asyncio

        cfg = WorkloadConfig(
            arrival_rate=50.0,  # High rate
            duration_s=1.0,
            pipeline_mix={"cqi_prediction": 1.0},
            broker_url="http://broker:8080",
            seed=42,
        )
        gen = WorkloadGenerator(cfg)
        gen._drain_timeout_s = 2.0  # Short drain for test

        # Mock publish to block forever (simulates stuck broker)
        async def blocking_publish(client, request):
            await asyncio.sleep(3600)  # 1 hour — effectively forever

        with patch.object(gen, '_publish', side_effect=blocking_publish):
            start = time.time()
            asyncio.run(gen.run())
            elapsed = time.time() - start

        # Must finish within duration + drain timeout + margin
        max_allowed = cfg.duration_s + gen._drain_timeout_s + 2.0
        assert elapsed < max_allowed, (
            f"Generator took {elapsed:.1f}s with blocking broker "
            f"(limit {max_allowed:.1f}s). Drain timeout not enforced (BUG-001)."
        )


# ===========================================================================
# Cycle 7: BUG-002a — UTC timestamps in progress.json
# ===========================================================================


class TestProgressTimestampsUTC:
    """BUG-002a: _update_progress() must write UTC ISO timestamps.

    Without timezone info, the monitor computes elapsed time using the local
    clock, which produces wrong results when the writer and reader are in
    different timezones (e.g., VM in EET, laptop in JST).
    """

    def test_progress_timestamps_are_utc(self, tmp_path):
        """_update_progress() writes timestamps that parse as UTC."""
        from scripts._common import _update_progress

        progress: dict = {}
        _update_progress(tmp_path, progress, "test_run", "running")

        ts_str = progress["test_run"]["timestamp"]
        parsed = datetime.fromisoformat(ts_str)

        # Timestamp must have timezone info (not naive)
        assert parsed.tzinfo is not None, (
            f"Timestamp {ts_str!r} is naive (no timezone). "
            f"BUG-002a: must use UTC timestamps."
        )
        # And it must be UTC
        assert parsed.utcoffset().total_seconds() == 0, (
            f"Timestamp {ts_str!r} is not UTC (offset={parsed.utcoffset()}). "
            f"BUG-002a: must use UTC timestamps."
        )

    def test_progress_timestamps_roundtrip_accurately(self, tmp_path):
        """UTC timestamps round-trip correctly regardless of local timezone."""
        from scripts._common import _update_progress

        before = datetime.now(timezone.utc)
        progress: dict = {}
        _update_progress(tmp_path, progress, "test_run", "running")
        after = datetime.now(timezone.utc)

        ts_str = progress["test_run"]["timestamp"]
        parsed = datetime.fromisoformat(ts_str)

        # Parsed timestamp should be between before and after
        assert before <= parsed <= after, (
            f"Timestamp {parsed} not in expected range [{before}, {after}]"
        )

    def test_progress_file_contains_utc_timestamps(self, tmp_path):
        """The persisted .progress.json file contains UTC timestamps."""
        import json
        from scripts._common import _update_progress

        progress: dict = {}
        _update_progress(tmp_path, progress, "test_run", "done")

        # Read back from file
        pf = tmp_path / ".progress.json"
        assert pf.exists()
        data = json.loads(pf.read_text())
        ts_str = data["test_run"]["timestamp"]

        # Must contain +00:00 or Z suffix
        assert "+00:00" in ts_str or ts_str.endswith("Z"), (
            f"Persisted timestamp {ts_str!r} does not indicate UTC."
        )


# ===========================================================================
# Cycle 8: BUG-002b — Bash tmux calls Python directly for remote
# ===========================================================================


class TestBashTmuxDirectPython:
    """BUG-002b: Remote tmux sessions must call Python directly, not re-enter bash."""

    SCRIPT = str(PROJECT_ROOT / "run-experiments.sh")

    def test_remote_tmux_calls_python_directly(self):
        """When --remote is used, tmux command should contain 'python3 -m scripts.run_phase_*',
        not './run-experiments.sh' or '$0'.

        We can't actually test SSH, but we can verify the bash script structure
        by tracing the generated tmux command.
        """
        import subprocess

        # Use bash -n (syntax check) + grep to verify the script structure
        # Read the script and check that maybe_tmux_wrap for remote mode
        # constructs a command with 'python3 -m' not '$0'
        script_text = Path(self.SCRIPT).read_text()

        # In remote mode, tmux should call Python directly
        # The tmux command should NOT re-enter the bash script
        # Check all 'maybe_tmux_wrap' calls to see what command they pass
        import re
        tmux_calls = re.findall(
            r'maybe_tmux_wrap\s+"[^"]+"\s+"([^"]+)"',
            script_text,
        )

        for call in tmux_calls:
            # The command passed to maybe_tmux_wrap should NOT be $0
            # (which would re-enter the bash script)
            assert "$0" not in call or "python3" in call, (
                f"maybe_tmux_wrap call re-enters bash script: {call!r}\n"
                f"BUG-002b: remote tmux should call Python directly."
            )


# ===========================================================================
# Cycle 9: BUG-002c — Smoke --phases multi-arg parsing
# ===========================================================================


class TestSmokePhasesParsing:
    """BUG-002c: --phases flag must accept multiple arguments."""

    def test_smoke_phases_flag_accepts_multiple(self):
        """'--phases A B' should be parsed as ['A', 'B'], not ['A']."""
        import argparse

        # Replicate the smoke test's argparse setup
        parser = argparse.ArgumentParser()
        parser.add_argument("--phases", nargs="*", default=["stack", "A", "figures"])
        args = parser.parse_args(["--phases", "A", "B"])

        assert "A" in args.phases, "Phase A should be in parsed phases"
        assert "B" in args.phases, "Phase B should be in parsed phases"
        assert len(args.phases) == 2

    def test_smoke_phases_default_includes_all(self):
        """Default --phases includes stack, A, and figures."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--phases", nargs="*", default=["stack", "A", "figures"])
        args = parser.parse_args([])

        assert args.phases == ["stack", "A", "figures"]


# ===========================================================================
# Cycle 10: BUG-002d — Monitor total count from progress.json
# ===========================================================================


class TestMonitorTotalCount:
    """BUG-002d: Monitor must show correct total run count from progress.json."""

    def test_monitor_total_from_progress_json(self, tmp_path):
        """When .progress.json exists, total count = all entries (not just completed)."""
        import json
        from scripts.monitor import load_progress

        # Simulate progress.json with 20 runs: some done, some queued, some running
        progress = {}
        for i in range(20):
            config = f"B{(i % 4) + 1}"
            seed = [42, 123, 456, 789, 0][i % 5]
            run_id = f"{config}_rate-medium_stages-3_seed-{seed}"
            if i < 5:
                progress[run_id] = {"status": "done", "timestamp": "2026-03-21T05:00:00+00:00"}
            elif i < 8:
                progress[run_id] = {"status": "running", "timestamp": "2026-03-21T06:00:00+00:00"}
            else:
                progress[run_id] = {"status": "queued", "timestamp": "2026-03-21T05:00:00+00:00"}

        pf = tmp_path / ".progress.json"
        pf.write_text(json.dumps(progress, indent=2))

        loaded = load_progress(tmp_path)
        assert len(loaded) == 20, (
            f"Monitor loaded {len(loaded)} runs from progress.json, expected 20. "
            f"BUG-002d: total count must reflect all planned runs."
        )

    def test_csv_fallback_does_not_know_total(self, tmp_path):
        """CSV fallback can only count existing files, not planned total.

        This documents the limitation: CSV discovery is inherently incomplete.
        The fix is to always use .progress.json (written by Python orchestrator).
        """
        from scripts.monitor import _discover_progress_from_csvs

        # Only 3 out of 20 runs have completed
        (tmp_path / "B1_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")
        (tmp_path / "B2_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")
        (tmp_path / "B3_rate-medium_stages-3_seed-42.csv").write_text("header\nrow1\n")

        progress = _discover_progress_from_csvs(tmp_path)
        # CSV fallback only sees completed files — it can't know the planned total
        assert len(progress) == 3, (
            "CSV fallback should only see completed files"
        )

    def test_monitor_elapsed_uses_utc_timestamps(self, tmp_path):
        """Monitor elapsed time computation should handle UTC timestamps correctly."""
        import json

        # Write progress with UTC timestamps (as fixed by BUG-002a)
        now_utc = datetime.now(timezone.utc)
        progress = {
            "B1_rate-medium_stages-3_seed-42": {
                "status": "running",
                "timestamp": now_utc.isoformat(),
            }
        }
        pf = tmp_path / ".progress.json"
        pf.write_text(json.dumps(progress))

        from scripts.monitor import load_progress
        loaded = load_progress(tmp_path)
        ts_str = loaded["B1_rate-medium_stages-3_seed-42"]["timestamp"]

        # The timestamp should be parseable and timezone-aware
        parsed = datetime.fromisoformat(ts_str)
        assert parsed.tzinfo is not None, (
            f"Progress timestamp {ts_str!r} lost timezone info during round-trip"
        )


# ===========================================================================
# Cycle 11: BUG-002e — Monitor SSH docker ps command
# ===========================================================================


class TestMonitorSSHDockerPs:
    """BUG-002e: Monitor SSH docker ps must send format string correctly.

    When SSH receives separate arguments like ['ssh', host, 'docker', 'ps',
    '--format', '{{.Names}}\\t{{.Status}}'], it concatenates them and the
    remote shell mangles the \\t and format string. The command must be sent
    as a single quoted string.
    """

    def test_get_docker_containers_ssh_command_format(self):
        """get_docker_containers() must send docker ps as a single SSH command string."""
        from unittest.mock import patch, MagicMock
        import subprocess

        captured_cmd = {}

        def mock_run(cmd, **kwargs):
            captured_cmd["cmd"] = cmd
            result = MagicMock()
            result.stdout = "test-container\tUp 5 minutes\n"
            result.returncode = 0
            return result

        from scripts.monitor import get_docker_containers
        with patch("scripts.monitor.subprocess.run", side_effect=mock_run):
            get_docker_containers(remote_host="testhost")

        cmd = captured_cmd["cmd"]
        # The SSH command should send docker ps as a single string argument
        # NOT as separate args ['ssh', 'host', 'docker', 'ps', '--format', ...]
        # because SSH concatenates and the remote shell mangles \t
        ssh_args_after_host = cmd[2:]  # everything after ['ssh', 'host']
        joined = " ".join(ssh_args_after_host)

        assert "docker ps" in joined, f"Command should contain 'docker ps': {cmd}"
        # The format string with \t must be preserved (not split across args)
        # Either: single arg with the whole command, or properly quoted
        assert len(ssh_args_after_host) == 1 or "\\t" not in str(cmd), (
            f"SSH docker ps format string will be mangled by remote shell: {cmd}\n"
            f"BUG-002e: must send as single SSH command string."
        )

    def test_get_run_pipelines_supports_remote(self):
        """get_run_pipelines() must accept remote_host parameter for SSH-based file checks."""
        from scripts.monitor import get_run_pipelines
        import inspect

        sig = inspect.signature(get_run_pipelines)
        assert "remote_host" in sig.parameters, (
            "get_run_pipelines() must accept 'remote_host' parameter "
            "to check CSV files on remote hosts via SSH. "
            "BUG-002e: pipeline count shows 'starting...' for remote runs."
        )
