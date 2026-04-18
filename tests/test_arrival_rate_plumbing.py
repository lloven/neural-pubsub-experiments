"""Tests for ARRIVAL_RATE plumbing from run_* scripts through multi_vm_runner
into the workload generator CLI.

Regression: 2026-04-18 bug. run_ablation / run_market / run_baseline / etc.
populated workload_env={"ARRIVAL_RATE": "<rate>"}. multi_vm_runner.run_single
passed this to `docker run -e ARRIVAL_RATE=<rate>` but the workload generator
(src/workload/generator.py) only consumes --arrival-rate from its CLI (default
1.0) and does NOT read ARRIVAL_RATE from env. Result: every distributed run,
across every phase, executed at 1.0 pps regardless of the scenario's configured
rate. All tier-1 and tier-2 data collected via run_single is rate-degenerate.

These tests pin the fix: ARRIVAL_RATE must be consumed as --arrival-rate on
the generator CLI, not left as a silent env var.
"""

from __future__ import annotations

import pytest


class TestBuildWorkloadCmdArrivalRate:
    """_build_workload_cmd must pass --arrival-rate as a CLI flag."""

    def test_arrival_rate_appears_as_cli_flag(self):
        from scripts.multi_vm_runner import _build_workload_cmd
        cmd = _build_workload_cmd(
            run_id="failure-150-12_market-quad_cqi-chain_seed-42",
            results_subdir="ablation",
            seed=42,
            warmup_s=30,
            measurement_s=60,
            workload_env={"PIPELINE_TYPE": "cqi_chain", "ARRIVAL_RATE": "150.0"},
        )
        assert "--arrival-rate 150.0" in cmd, (
            f"workload_cmd must pass --arrival-rate on CLI; got: {cmd}"
        )

    def test_arrival_rate_missing_raises(self):
        """Silent default to 1.0 pps was the bug. Omitting ARRIVAL_RATE must fail loud."""
        from scripts.multi_vm_runner import _build_workload_cmd
        with pytest.raises((KeyError, ValueError)):
            _build_workload_cmd(
                run_id="r",
                results_subdir="ablation",
                seed=0,
                warmup_s=10,
                measurement_s=20,
                workload_env={"PIPELINE_TYPE": "cqi_chain"},
            )

    def test_arrival_rate_not_duplicated_as_env(self):
        """ARRIVAL_RATE must be stripped from env flags once extracted to CLI.
        Leaving it in env is harmless but signals inconsistent contract."""
        from scripts.multi_vm_runner import _build_workload_cmd
        cmd = _build_workload_cmd(
            run_id="r",
            results_subdir="ablation",
            seed=0,
            warmup_s=10,
            measurement_s=20,
            workload_env={"PIPELINE_TYPE": "cqi_chain", "ARRIVAL_RATE": "5.0"},
        )
        assert "-e ARRIVAL_RATE=" not in cmd, (
            f"ARRIVAL_RATE env var should be stripped once passed as CLI; got: {cmd}"
        )

    def test_pipeline_type_still_passed_as_env(self):
        """PIPELINE_TYPE IS consumed by the generator via os.environ. Don't break it."""
        from scripts.multi_vm_runner import _build_workload_cmd
        cmd = _build_workload_cmd(
            run_id="r",
            results_subdir="ablation",
            seed=0,
            warmup_s=10,
            measurement_s=20,
            workload_env={"PIPELINE_TYPE": "anomaly_sp", "ARRIVAL_RATE": "5.0"},
        )
        assert "-e PIPELINE_TYPE=anomaly_sp" in cmd

    def test_other_env_vars_still_passed(self):
        """Resilience phase uses FUNNEL_MODE; stress uses other vars. Preserve them."""
        from scripts.multi_vm_runner import _build_workload_cmd
        cmd = _build_workload_cmd(
            run_id="r",
            results_subdir="resilience",
            seed=0,
            warmup_s=10,
            measurement_s=20,
            workload_env={
                "PIPELINE_TYPE": "cqi_chain",
                "ARRIVAL_RATE": "5.0",
                "FUNNEL_MODE": "noisy",
            },
        )
        assert "-e FUNNEL_MODE=noisy" in cmd

    def test_arrival_rate_value_fidelity(self):
        """Integer and float rates both round-trip correctly."""
        from scripts.multi_vm_runner import _build_workload_cmd
        for rate in ("1.0", "5.0", "50", "100.5", "200.0"):
            cmd = _build_workload_cmd(
                run_id="r",
                results_subdir="ablation",
                seed=0,
                warmup_s=10,
                measurement_s=20,
                workload_env={"PIPELINE_TYPE": "cqi_chain", "ARRIVAL_RATE": rate},
            )
            assert f"--arrival-rate {rate}" in cmd, (
                f"rate {rate} not preserved: {cmd}"
            )
