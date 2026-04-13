"""Tests for the ablation experiment infrastructure.

The ablation experiment introduces five scenarios that stress the
strategies in ways the main campaign does not:

1. Worker failure during measurement (information completeness)
2. Saturation arrival-rate sweep at 20/25/30 pps (admission control)
3. Heterogeneous worker capacities via processing_speed (price discovery)

This module provides:
- A new worker module path (src.worker.ablation_worker) that re-exports
  the existing worker without modification (reproducibility of the main
  campaign).
- A new compose file (deploy/docker-compose.vm-ablation.yaml) that uses
  the ablation worker module and supports per-VM WORKER_PROCESSING_SPEED.
- A new phase runner (scripts/run_ablation.py) with 3 scenarios.
"""

from __future__ import annotations

import pytest


class TestAblationWorkerModule:
    """The ablation worker module is a thin re-export of the main worker."""

    def test_ablation_worker_module_imports(self):
        """src.worker.ablation_worker is importable."""
        from src.worker import ablation_worker
        assert ablation_worker is not None

    def test_ablation_worker_exports_main(self):
        """ablation_worker exposes a main() callable for python -m invocation."""
        from src.worker import ablation_worker
        assert callable(ablation_worker.main)

    def test_ablation_worker_main_is_same_as_worker_main(self):
        """The ablation worker delegates to the main worker (no behavioral diff)."""
        from src.worker import ablation_worker
        from src.worker.worker import main as worker_main
        assert ablation_worker.main is worker_main


class TestAblationComposeFile:
    """docker-compose.vm-ablation.yaml uses ablation_worker and supports speed factor."""

    def test_ablation_compose_exists(self):
        from pathlib import Path
        path = Path("deploy/docker-compose.vm-ablation.yaml")
        assert path.exists(), "deploy/docker-compose.vm-ablation.yaml must exist"

    def test_ablation_compose_uses_ablation_worker(self):
        from pathlib import Path
        content = Path("deploy/docker-compose.vm-ablation.yaml").read_text()
        assert "src.worker.ablation_worker" in content, (
            "ablation compose must invoke src.worker.ablation_worker"
        )

    def test_ablation_compose_uses_processing_speed_env_var(self):
        from pathlib import Path
        content = Path("deploy/docker-compose.vm-ablation.yaml").read_text()
        assert "WORKER_PROCESSING_SPEED" in content, (
            "ablation compose must reference WORKER_PROCESSING_SPEED env var"
        )

    def test_ablation_compose_has_processing_speed_flag(self):
        from pathlib import Path
        content = Path("deploy/docker-compose.vm-ablation.yaml").read_text()
        assert "--processing-speed" in content

    def test_ablation_compose_enables_market_load_aware_via_env(self):
        """The ablation compose must enable load-aware market placement via
        the MARKET_LOAD_AWARE=true environment variable, NOT via the
        --market-load-aware CLI flag in the entrypoint.

        Using the CLI flag breaks rr-global (StaticBroker) because
        StaticBroker's argparse doesn't accept --market-load-aware.
        The env var works for NeuralBroker (reads it as the default
        for --market-load-aware) and is silently ignored by StaticBroker.
        """
        from pathlib import Path
        content = Path("deploy/docker-compose.vm-ablation.yaml").read_text()
        assert "MARKET_LOAD_AWARE=true" in content, (
            "Ablation compose must set MARKET_LOAD_AWARE=true in environment"
        )

    def test_ablation_compose_no_market_load_aware_cli_flag(self):
        """The --market-load-aware CLI flag must NOT appear in the compose
        entrypoint — it breaks StaticBroker (rr-global). L50 incident:
        75 rr-global runs failed because StaticBroker rejected the flag.
        """
        from pathlib import Path
        content = Path("deploy/docker-compose.vm-ablation.yaml").read_text()
        assert "--market-load-aware" not in content, (
            "Ablation compose must NOT pass --market-load-aware as CLI flag "
            "(breaks StaticBroker). Use MARKET_LOAD_AWARE=true env var instead."
        )

    def test_main_compose_does_not_enable_market_load_aware(self):
        """The main campaign compose must NOT have the flag, preserving
        reproducibility of market runs already collected.
        """
        from pathlib import Path
        content = Path("deploy/docker-compose.vm.yaml").read_text()
        assert "--market-load-aware" not in content, (
            "Main compose must NOT enable market_load_aware "
            "(reproducibility of completed market runs)"
        )


class TestRunAblationPhase:
    """run_ablation.py defines 5 scenarios x 3 strategies x 3 pipelines."""

    def test_run_ablation_imports(self):
        from scripts import run_ablation
        assert run_ablation is not None

    def test_five_scenarios_defined(self):
        """failure, sat-20, sat-25, sat-30, heterogeneous."""
        from scripts.run_ablation import SCENARIOS
        assert set(SCENARIOS.keys()) == {
            "failure", "sat-20", "sat-25", "sat-30", "heterogeneous",
        }

    def test_three_strategies_per_scenario(self):
        from scripts.run_ablation import STRATEGIES
        assert set(STRATEGIES) == {"oracle-global", "rr-global", "market-quad"}

    def test_run_matrix_225_runs(self):
        """5 scenarios x 3 strategies x 3 pipelines x 5 seeds = 225."""
        from scripts.run_ablation import build_run_matrix, STRATEGIES, SCENARIOS
        runs = build_run_matrix(
            scenarios=list(SCENARIOS),
            strategies=list(STRATEGIES),
            seeds=[42, 123, 456, 789, 0],
        )
        assert len(runs) == 225

    def test_failure_scenario_has_failure_target(self):
        from scripts.run_ablation import SCENARIOS
        assert SCENARIOS["failure"].get("failure_target") is not None

    def test_saturation_sweep_covers_inflection_point(self):
        """Three saturation rates around the empirical 25 pps inflection."""
        from scripts.run_ablation import SCENARIOS
        assert SCENARIOS["sat-20"]["arrival_rate"] == 20.0
        assert SCENARIOS["sat-25"]["arrival_rate"] == 25.0
        assert SCENARIOS["sat-30"]["arrival_rate"] == 30.0

    def test_heterogeneous_scenario_has_speed_factors(self):
        from scripts.run_ablation import SCENARIOS
        sf = SCENARIOS["heterogeneous"].get("speed_factors")
        assert sf is not None
        # Edge VMs slower (>1.0 multiplier means slower), cloud faster (<1.0)
        assert sf["vm1"] != sf["vm3"], "edge and cloud must differ"

    def test_ablation_run_length_inherits_from_experiment_matrix(self):
        """Ablation must inherit warmup_s/measurement_s from the SSoT in
        scripts.experiment_matrix, not hardcode them.

        The values must be equal to EXPERIMENTS["market"] so that ablation
        data is directly comparable to the main market campaign data
        (same statistical window, same warmup convergence, same p95
        sample basis).
        """
        from scripts.experiment_matrix import EXPERIMENTS
        market_w = EXPERIMENTS["market"]["warmup_s"]
        market_m = EXPERIMENTS["market"]["measurement_s"]
        abl_w = EXPERIMENTS["ablation"]["warmup_s"]
        abl_m = EXPERIMENTS["ablation"]["measurement_s"]
        assert abl_w == market_w, (
            f"EXPERIMENTS['ablation']['warmup_s']={abl_w} must equal "
            f"EXPERIMENTS['market']['warmup_s']={market_w}"
        )
        assert abl_m == market_m, (
            f"EXPERIMENTS['ablation']['measurement_s']={abl_m} must equal "
            f"EXPERIMENTS['market']['measurement_s']={market_m}"
        )

    def test_all_scenarios_use_matrix_run_length(self):
        """SCENARIOS dict in run_ablation.py must read warmup/measurement
        from EXPERIMENTS["ablation"] so a single edit to the SSoT
        rescales every scenario at once.
        """
        from scripts.experiment_matrix import EXPERIMENTS
        from scripts.run_ablation import SCENARIOS
        expected_w = EXPERIMENTS["ablation"]["warmup_s"]
        expected_m = EXPERIMENTS["ablation"]["measurement_s"]
        for name, cfg in SCENARIOS.items():
            assert cfg["warmup_s"] == expected_w, (
                f"scenario {name!r} warmup_s={cfg['warmup_s']} != "
                f"EXPERIMENTS['ablation']['warmup_s']={expected_w}"
            )
            assert cfg["measurement_s"] == expected_m, (
                f"scenario {name!r} measurement_s={cfg['measurement_s']} != "
                f"EXPERIMENTS['ablation']['measurement_s']={expected_m}"
            )

    def test_market_run_config_default_inherits_from_matrix(self):
        """MarketRunConfig defaults must come from EXPERIMENTS["market"]
        in scripts.experiment_matrix, not from hardcoded literals.
        """
        from scripts.experiment_matrix import EXPERIMENTS
        from scripts.run_market import MarketRunConfig
        cfg = MarketRunConfig(
            config_name="dummy",
            pipeline_type="cqi_chain",
            load_label="medium",
            arrival_rate=5.0,
            seed=42,
        )
        assert cfg.warmup_s == EXPERIMENTS["market"]["warmup_s"]
        assert cfg.measurement_s == EXPERIMENTS["market"]["measurement_s"]

    def test_failure_injection_delay_halfway_through_measurement(self):
        """Failure scenario must inject failure at measurement_s // 2 so
        that pre- and post-failure observation windows are both at least
        long enough to characterise the failure transient. The delay
        is computed from measurement_s rather than hardcoded so the
        ratio is preserved when the run length is rescaled.
        """
        from scripts.run_ablation import SCENARIOS
        scen = SCENARIOS["failure"]
        delay = scen["failure_delay_s"]
        meas = scen["measurement_s"]
        assert delay == meas // 2, (
            f"failure_delay_s={delay} must equal measurement_s//2={meas // 2}"
        )
        # Post-failure window must be at least 2 minutes
        assert meas - delay >= 120, (
            f"post-failure window {meas - delay}s < 120s is too short"
        )


class TestPerVmEnvWiring:
    """per_vm_env propagates correctly through start_cluster."""

    def test_per_vm_env_appears_in_compose_command(self):
        from unittest.mock import patch
        from scripts.multi_vm_runner import start_cluster, VMS

        with patch("scripts.multi_vm_runner._exec") as mock_exec:
            start_cluster(
                placement_mode="market",
                per_vm_env={
                    "vm1": {"WORKER_PROCESSING_SPEED": "2.0"},
                    "vm3": {"WORKER_PROCESSING_SPEED": "0.67"},
                },
                dry_run=True,
            )

            # Find vm1 and vm3 calls
            calls_by_vm = {c[0][0].name: c[0][1] for c in mock_exec.call_args_list}
            assert "WORKER_PROCESSING_SPEED=2.0" in calls_by_vm["vm1"]
            assert "WORKER_PROCESSING_SPEED=0.67" in calls_by_vm["vm3"]
            # vm2 has no override, should NOT have processing speed
            assert "WORKER_PROCESSING_SPEED" not in calls_by_vm["vm2"]

    def test_compose_file_parameter_overrides_default(self):
        from unittest.mock import patch
        from scripts.multi_vm_runner import start_cluster

        with patch("scripts.multi_vm_runner._exec") as mock_exec:
            start_cluster(
                placement_mode="market",
                compose_file="deploy/docker-compose.vm-ablation.yaml",
                dry_run=True,
            )
            for c in mock_exec.call_args_list:
                cmd = c[0][1]
                assert "docker-compose.vm-ablation.yaml" in cmd, (
                    f"Expected ablation compose file in: {cmd[:200]}"
                )


class TestFailureInjectionWiring:
    """run_ablation._run_distributed wires failure_fn correctly for failure scenarios."""

    def test_failure_scenario_passes_failure_fn(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            run = AblationRunConfig(
                scenario_name="failure",
                strategy="rr-global",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=5.0,
                warmup_s=60,
                measurement_s=180,
            )
            _run_distributed(run, dry_run=True)
            _, kwargs = mock_run.call_args
            assert kwargs["failure_fn"] is not None, (
                "failure scenario must pass a non-None failure_fn"
            )

    def test_saturation_scenario_no_failure_fn(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            run = AblationRunConfig(
                scenario_name="sat-25",
                strategy="rr-global",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=25.0,
                warmup_s=60,
                measurement_s=180,
            )
            _run_distributed(run, dry_run=True)
            _, kwargs = mock_run.call_args
            assert kwargs["failure_fn"] is None

    def test_heterogeneous_scenario_passes_per_vm_env(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            run = AblationRunConfig(
                scenario_name="heterogeneous",
                strategy="market-quad",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=5.0,
                warmup_s=60,
                measurement_s=180,
            )
            _run_distributed(run, dry_run=True)
            _, kwargs = mock_run.call_args
            per_vm = kwargs["per_vm_env"]
            assert per_vm is not None
            assert "vm1" in per_vm
            assert "vm3" in per_vm
            assert per_vm["vm1"]["WORKER_PROCESSING_SPEED"] == "2.0"
            assert per_vm["vm3"]["WORKER_PROCESSING_SPEED"] == "0.67"

    def test_ablation_uses_ablation_compose_file(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            run = AblationRunConfig(
                scenario_name="sat-25",
                strategy="oracle-global",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=25.0,
                warmup_s=60,
                measurement_s=180,
            )
            _run_distributed(run, dry_run=True)
            _, kwargs = mock_run.call_args
            assert kwargs["compose_file"] == "deploy/docker-compose.vm-ablation.yaml"


class TestFailurePropagation:
    """L51: _run_distributed must propagate run_single failure status.

    When run_single returns {"status": "failed"} (e.g. federation timeout
    due to stale Docker image), _run_distributed must NOT unconditionally
    return "completed". The silent-success bug (225 runs "successful" with
    0 CSVs, commit 79718c7) was caused by ignoring run_single's return.
    """

    def test_ablation_propagates_federation_failure(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            mock_run.return_value = {
                "run_id": "test", "status": "failed",
                "error": "federation_timeout",
            }
            run = AblationRunConfig(
                scenario_name="sat-25",
                strategy="oracle-global",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=25.0,
                warmup_s=240,
                measurement_s=600,
            )
            result = _run_distributed(run, dry_run=False)
            assert result["status"] == "failed", (
                "L51: _run_distributed must propagate run_single failure, "
                f"got status={result['status']!r}"
            )

    def test_ablation_returns_completed_on_success(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            mock_run.return_value = {"run_id": "test", "status": "completed"}
            run = AblationRunConfig(
                scenario_name="sat-25",
                strategy="oracle-global",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=25.0,
                warmup_s=240,
                measurement_s=600,
            )
            result = _run_distributed(run, dry_run=False)
            assert result["status"] == "completed"

    def test_ablation_returns_dry_run_on_dry_run(self):
        from unittest.mock import patch
        from scripts.run_ablation import _run_distributed, AblationRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            mock_run.return_value = None  # dry-run returns None
            run = AblationRunConfig(
                scenario_name="sat-25",
                strategy="oracle-global",
                pipeline_type="cqi_chain",
                seed=42,
                arrival_rate=25.0,
                warmup_s=240,
                measurement_s=600,
            )
            result = _run_distributed(run, dry_run=True)
            assert result["status"] == "dry_run"

    def test_market_propagates_federation_failure(self):
        from unittest.mock import patch
        from scripts.run_market import _run_distributed, MarketRunConfig

        with patch("scripts.multi_vm_runner.run_single") as mock_run:
            mock_run.return_value = {
                "run_id": "test", "status": "failed",
                "error": "federation_timeout",
            }
            run = MarketRunConfig(
                config_name="oracle-global",
                pipeline_type="cqi_chain",
                load_label="medium",
                arrival_rate=5.0,
                seed=42,
            )
            result = _run_distributed(run, dry_run=False)
            assert result["status"] == "failed", (
                "L51: market _run_distributed must propagate run_single failure"
            )
