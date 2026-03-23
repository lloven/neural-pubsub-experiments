"""Tests for funnel resilience mode (Phase D: D3/D4/D5).

The architecture (Section 4.4.3) defines three funnel resilience modes for
how a funnel stage handles missing inputs from a failed upstream worker:
  - wait:    Buffer and wait for the failed input (bounded by timeout). Pipeline stalls.
  - proceed: Execute with partial inputs. Output quality degrades but pipeline completes.
  - abort:   Signal failure immediately. Pipeline fails fast.

D3/D4/D5 kill a sensor-input worker that feeds into the fuse stage of the
sensor_fusion pipeline, with each funnel resilience mode active.

TDD RED phase: all tests must fail before implementation.
"""

from __future__ import annotations

import itertools

import pytest
import yaml
from pathlib import Path

from scripts._common import PROJECT_ROOT, EXTENDED_SEEDS


# ---------------------------------------------------------------------------
# 1. FunnelMode enum and validation
# ---------------------------------------------------------------------------

class TestFunnelModeEnum:
    """A FunnelMode enum must define the three resilience modes."""

    def test_funnel_mode_importable(self):
        """FunnelMode must be importable from src.broker.funnel_resilience."""
        from src.broker.funnel_resilience import FunnelMode
        assert FunnelMode is not None

    def test_funnel_mode_has_wait(self):
        from src.broker.funnel_resilience import FunnelMode
        assert hasattr(FunnelMode, "WAIT")

    def test_funnel_mode_has_proceed(self):
        from src.broker.funnel_resilience import FunnelMode
        assert hasattr(FunnelMode, "PROCEED")

    def test_funnel_mode_has_abort(self):
        from src.broker.funnel_resilience import FunnelMode
        assert hasattr(FunnelMode, "ABORT")

    def test_funnel_mode_values_are_lowercase_strings(self):
        """Enum values should be the lowercase mode name for env var compatibility."""
        from src.broker.funnel_resilience import FunnelMode
        assert FunnelMode.WAIT.value == "wait"
        assert FunnelMode.PROCEED.value == "proceed"
        assert FunnelMode.ABORT.value == "abort"

    def test_funnel_mode_from_env_string(self):
        """FunnelMode must be constructable from an env var string."""
        from src.broker.funnel_resilience import FunnelMode
        assert FunnelMode("wait") == FunnelMode.WAIT
        assert FunnelMode("proceed") == FunnelMode.PROCEED
        assert FunnelMode("abort") == FunnelMode.ABORT

    def test_exactly_three_modes(self):
        """There must be exactly 3 funnel resilience modes."""
        from src.broker.funnel_resilience import FunnelMode
        assert len(FunnelMode) == 3


# ---------------------------------------------------------------------------
# 2. D3/D4/D5 config existence and correctness
# ---------------------------------------------------------------------------

class TestFunnelResilienceConfigs:
    """D3, D4, D5 must exist in Phase D CONFIGS with correct funnel modes."""

    def test_d3_exists_in_configs(self):
        from scripts.run_phase_d import CONFIGS
        assert "D3" in CONFIGS, "D3 (funnel-wait) must exist in Phase D CONFIGS"

    def test_d4_exists_in_configs(self):
        from scripts.run_phase_d import CONFIGS
        assert "D4" in CONFIGS, "D4 (funnel-proceed) must exist in Phase D CONFIGS"

    def test_d5_exists_in_configs(self):
        from scripts.run_phase_d import CONFIGS
        assert "D5" in CONFIGS, "D5 (funnel-abort) must exist in Phase D CONFIGS"

    def test_d3_funnel_mode_is_wait(self):
        from scripts.run_phase_d import CONFIGS
        assert CONFIGS["D3"]["funnel_mode"] == "wait", (
            "D3 must use funnel_mode='wait'"
        )

    def test_d4_funnel_mode_is_proceed(self):
        from scripts.run_phase_d import CONFIGS
        assert CONFIGS["D4"]["funnel_mode"] == "proceed", (
            "D4 must use funnel_mode='proceed'"
        )

    def test_d5_funnel_mode_is_abort(self):
        from scripts.run_phase_d import CONFIGS
        assert CONFIGS["D5"]["funnel_mode"] == "abort", (
            "D5 must use funnel_mode='abort'"
        )

    def test_d3_d4_d5_are_distinct_configs(self):
        """All three funnel resilience configs must have distinct modes."""
        from scripts.run_phase_d import CONFIGS
        modes = {
            CONFIGS["D3"]["funnel_mode"],
            CONFIGS["D4"]["funnel_mode"],
            CONFIGS["D5"]["funnel_mode"],
        }
        assert len(modes) == 3, f"Expected 3 distinct funnel modes, got {modes}"


# ---------------------------------------------------------------------------
# 3. Failure targets are sensor-input workers (not eMBB or URLLC main workers)
# ---------------------------------------------------------------------------

class TestFunnelFailureTargets:
    """D3/D4/D5 must kill a sensor worker feeding into the fuse stage."""

    def _load_compose_services(self) -> set[str]:
        compose = PROJECT_ROOT / "docker-compose.local.yaml"
        with open(compose) as f:
            dc = yaml.safe_load(f) or {}
        return set(dc.get("services", {}).keys())

    def test_d3_failure_type_is_worker(self):
        from scripts.run_phase_d import CONFIGS
        assert CONFIGS["D3"]["failure_type"] == "worker"

    def test_d4_failure_type_is_worker(self):
        from scripts.run_phase_d import CONFIGS
        assert CONFIGS["D4"]["failure_type"] == "worker"

    def test_d5_failure_type_is_worker(self):
        from scripts.run_phase_d import CONFIGS
        assert CONFIGS["D5"]["failure_type"] == "worker"

    def test_d3_target_is_valid_compose_service(self):
        from scripts.run_phase_d import CONFIGS
        services = self._load_compose_services()
        target = CONFIGS["D3"]["failure_target"]
        assert target in services, (
            f"D3 failure_target '{target}' not in compose services: {sorted(services)}"
        )

    def test_d4_target_is_valid_compose_service(self):
        from scripts.run_phase_d import CONFIGS
        services = self._load_compose_services()
        target = CONFIGS["D4"]["failure_target"]
        assert target in services, (
            f"D4 failure_target '{target}' not in compose services: {sorted(services)}"
        )

    def test_d5_target_is_valid_compose_service(self):
        from scripts.run_phase_d import CONFIGS
        services = self._load_compose_services()
        target = CONFIGS["D5"]["failure_target"]
        assert target in services, (
            f"D5 failure_target '{target}' not in compose services: {sorted(services)}"
        )

    def test_d3_d4_d5_target_same_worker(self):
        """All three funnel configs must target the same sensor-input worker,
        since the independent variable is the funnel mode, not the target."""
        from scripts.run_phase_d import CONFIGS
        targets = {
            CONFIGS["D3"]["failure_target"],
            CONFIGS["D4"]["failure_target"],
            CONFIGS["D5"]["failure_target"],
        }
        assert len(targets) == 1, (
            f"D3/D4/D5 should target the same worker (IV is funnel mode, "
            f"not target). Got {targets}"
        )

    def test_funnel_target_is_not_embb_main_worker(self):
        """D3/D4/D5 must NOT target the eMBB worker used by D1."""
        from scripts.run_phase_d import CONFIGS
        d1_target = CONFIGS["D1"]["failure_target"]
        for cfg_name in ("D3", "D4", "D5"):
            assert CONFIGS[cfg_name]["failure_target"] != d1_target, (
                f"{cfg_name} targets the same worker as D1 ({d1_target}). "
                f"Funnel configs must target a sensor-input worker, not "
                f"the eMBB main worker."
            )

    def test_funnel_target_is_not_urllc_main_worker(self):
        """D3/D4/D5 must NOT target the URLLC worker used by D2."""
        from scripts.run_phase_d import CONFIGS
        d2_target = CONFIGS["D2"]["failure_target"]
        for cfg_name in ("D3", "D4", "D5"):
            assert CONFIGS[cfg_name]["failure_target"] != d2_target, (
                f"{cfg_name} targets the same worker as D2 ({d2_target}). "
                f"Funnel configs must target a sensor-input worker."
            )


# ---------------------------------------------------------------------------
# 4. FUNNEL_MODE env var is passed to broker containers
# ---------------------------------------------------------------------------

class TestFunnelModeEnvVar:
    """D3/D4/D5 runs must pass FUNNEL_MODE to the broker/worker environment."""

    def test_d3_run_env_contains_funnel_mode(self):
        """D3 run must set FUNNEL_MODE=wait in the container environment."""
        from scripts.run_phase_d import CONFIGS, RunConfig, _run
        import inspect
        source = inspect.getsource(_run)
        assert "FUNNEL_MODE" in source, (
            "_run() must set FUNNEL_MODE in the env dict for funnel configs"
        )

    def test_funnel_mode_env_passed_for_d3(self):
        """When config has funnel_mode, the env dict must include FUNNEL_MODE."""
        from scripts.run_phase_d import CONFIGS, RunConfig
        # D3 config must have funnel_mode key
        assert "funnel_mode" in CONFIGS["D3"], "D3 missing funnel_mode key"


# ---------------------------------------------------------------------------
# 5. Run matrix: 3 funnel modes x seeds
# ---------------------------------------------------------------------------

class TestFunnelResilienceMatrix:
    """Funnel resilience configs produce the correct run matrix."""

    def test_funnel_configs_in_matrix(self):
        """D3/D4/D5 must appear in the run matrix."""
        from scripts.run_phase_d import CONFIGS, build_run_matrix
        funnel_configs = ["D3", "D4", "D5"]
        matrix = build_run_matrix(funnel_configs, [42])
        assert len(matrix) == 3, f"Expected 3 runs (1 seed x 3 configs), got {len(matrix)}"

    def test_funnel_matrix_3_modes_x_5_seeds(self):
        """3 funnel modes x 5 seeds = 15 runs."""
        from scripts._common import DEFAULT_SEEDS
        from scripts.run_phase_d import build_run_matrix
        matrix = build_run_matrix(["D3", "D4", "D5"], DEFAULT_SEEDS)
        assert len(matrix) == 15, (
            f"Expected 15 runs (3 modes x 5 seeds), got {len(matrix)}"
        )

    def test_funnel_matrix_unique_run_ids(self):
        """All 15 funnel runs must have unique run_ids."""
        from scripts._common import DEFAULT_SEEDS
        from scripts.run_phase_d import build_run_matrix
        matrix = build_run_matrix(["D3", "D4", "D5"], DEFAULT_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(set(run_ids)) == 15, f"Duplicate run_ids: {run_ids}"

    def test_full_phase_d_matrix_size(self):
        """Full Phase D: 5 configs (D1-D5) x 10 seeds = 50 runs."""
        from scripts.run_phase_d import CONFIGS, build_run_matrix
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        assert len(matrix) == 50, (
            f"Expected 50 runs (5 configs x 10 seeds), got {len(matrix)}"
        )


# ---------------------------------------------------------------------------
# 6. PIPELINE_MIX must enable sensor_fusion for D3/D4/D5
# ---------------------------------------------------------------------------

class TestFunnelPipelineMix:
    """D3/D4/D5 must enable sensor_fusion pipelines (funnel pattern)."""

    def test_d3_d4_d5_enable_fusion_pipeline(self):
        """D3/D4/D5 configs must set PIPELINE_MIX_FUSION > 0."""
        from scripts.run_phase_d import CONFIGS
        for cfg_name in ("D3", "D4", "D5"):
            cfg = CONFIGS[cfg_name]
            # The config or the _run function must ensure fusion is enabled
            assert "pipeline_mix_fusion" in cfg or "funnel_mode" in cfg, (
                f"{cfg_name} must enable sensor_fusion pipeline for funnel testing"
            )


# ---------------------------------------------------------------------------
# 7. Funnel resilience behavior contracts
# ---------------------------------------------------------------------------

class TestFunnelResilienceBehavior:
    """Each funnel mode must exhibit its defined behavior contract."""

    def test_wait_mode_stalls_pipeline(self):
        """In wait mode, the broker must NOT mark the pipeline as complete or
        failed until timeout; it waits for the missing input."""
        from src.broker.funnel_resilience import FunnelMode, apply_funnel_policy

        # Simulate: fuse stage has 3 inputs, only 2 have completed
        result = apply_funnel_policy(
            mode=FunnelMode.WAIT,
            expected_inputs={"sensor_0", "sensor_1", "sensor_2"},
            received_inputs={"sensor_0", "sensor_1"},
            timeout_reached=False,
        )
        assert result.action == "wait", (
            f"Wait mode with missing inputs should wait, got: {result.action}"
        )
        assert not result.pipeline_complete
        assert not result.pipeline_failed

    def test_wait_mode_fails_on_timeout(self):
        """In wait mode, if the timeout is reached, the pipeline should fail."""
        from src.broker.funnel_resilience import FunnelMode, apply_funnel_policy

        result = apply_funnel_policy(
            mode=FunnelMode.WAIT,
            expected_inputs={"sensor_0", "sensor_1", "sensor_2"},
            received_inputs={"sensor_0", "sensor_1"},
            timeout_reached=True,
        )
        assert result.action == "fail", (
            f"Wait mode after timeout should fail, got: {result.action}"
        )
        assert result.pipeline_failed

    def test_proceed_mode_completes_with_partial(self):
        """In proceed mode, the broker marks the pipeline as proceeding with
        partial inputs (partial=True flag)."""
        from src.broker.funnel_resilience import FunnelMode, apply_funnel_policy

        result = apply_funnel_policy(
            mode=FunnelMode.PROCEED,
            expected_inputs={"sensor_0", "sensor_1", "sensor_2"},
            received_inputs={"sensor_0", "sensor_1"},
            timeout_reached=False,
        )
        assert result.action == "proceed", (
            f"Proceed mode should proceed with partial inputs, got: {result.action}"
        )
        assert result.partial is True
        assert not result.pipeline_failed

    def test_abort_mode_fails_immediately(self):
        """In abort mode, a missing input causes immediate pipeline failure."""
        from src.broker.funnel_resilience import FunnelMode, apply_funnel_policy

        result = apply_funnel_policy(
            mode=FunnelMode.ABORT,
            expected_inputs={"sensor_0", "sensor_1", "sensor_2"},
            received_inputs={"sensor_0", "sensor_1"},
            timeout_reached=False,
        )
        assert result.action == "abort", (
            f"Abort mode should abort immediately, got: {result.action}"
        )
        assert result.pipeline_failed

    def test_all_inputs_received_always_proceeds(self):
        """When all inputs are received, all modes should proceed normally."""
        from src.broker.funnel_resilience import FunnelMode, apply_funnel_policy

        for mode in FunnelMode:
            result = apply_funnel_policy(
                mode=mode,
                expected_inputs={"sensor_0", "sensor_1", "sensor_2"},
                received_inputs={"sensor_0", "sensor_1", "sensor_2"},
                timeout_reached=False,
            )
            assert result.action == "proceed", (
                f"Mode {mode.value} with all inputs should proceed, got: {result.action}"
            )
            assert result.partial is False
            assert not result.pipeline_failed


# ---------------------------------------------------------------------------
# 8. FunnelPolicyResult dataclass
# ---------------------------------------------------------------------------

class TestFunnelPolicyResult:
    """FunnelPolicyResult must carry the decision and metadata."""

    def test_funnel_policy_result_importable(self):
        from src.broker.funnel_resilience import FunnelPolicyResult
        assert FunnelPolicyResult is not None

    def test_funnel_policy_result_has_action(self):
        from src.broker.funnel_resilience import FunnelPolicyResult
        r = FunnelPolicyResult(action="wait", partial=False, pipeline_complete=False, pipeline_failed=False)
        assert r.action == "wait"

    def test_funnel_policy_result_has_partial_flag(self):
        from src.broker.funnel_resilience import FunnelPolicyResult
        r = FunnelPolicyResult(action="proceed", partial=True, pipeline_complete=False, pipeline_failed=False)
        assert r.partial is True

    def test_funnel_policy_result_has_pipeline_status(self):
        from src.broker.funnel_resilience import FunnelPolicyResult
        r = FunnelPolicyResult(action="abort", partial=False, pipeline_complete=False, pipeline_failed=True)
        assert r.pipeline_failed is True
        assert r.pipeline_complete is False


# ---------------------------------------------------------------------------
# 9. Default funnel mode is wait (backward compatible)
# ---------------------------------------------------------------------------

class TestFunnelModeDefault:
    """The default funnel mode must be 'wait' for backward compatibility."""

    def test_default_funnel_mode_is_wait(self):
        """When FUNNEL_MODE env var is not set, the default should be 'wait'."""
        from src.broker.funnel_resilience import get_funnel_mode
        import os
        # Ensure env var is not set
        old = os.environ.pop("FUNNEL_MODE", None)
        try:
            mode = get_funnel_mode()
            assert mode.value == "wait", (
                f"Default funnel mode must be 'wait', got '{mode.value}'"
            )
        finally:
            if old is not None:
                os.environ["FUNNEL_MODE"] = old

    def test_funnel_mode_from_env(self):
        """FUNNEL_MODE env var should be respected."""
        from src.broker.funnel_resilience import get_funnel_mode
        import os
        os.environ["FUNNEL_MODE"] = "proceed"
        try:
            mode = get_funnel_mode()
            assert mode.value == "proceed"
        finally:
            del os.environ["FUNNEL_MODE"]
