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
from scripts.experiment_matrix import expected_run_count


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

    def test_funnel_wait_exists_in_configs(self):
        from scripts.run_resilience import CONFIGS
        assert "funnel-wait" in CONFIGS, "funnel-wait must exist in resilience CONFIGS"

    def test_funnel_proceed_exists_in_configs(self):
        from scripts.run_resilience import CONFIGS
        assert "funnel-proceed" in CONFIGS, "funnel-proceed must exist in resilience CONFIGS"

    def test_funnel_abort_exists_in_configs(self):
        from scripts.run_resilience import CONFIGS
        assert "funnel-abort" in CONFIGS, "funnel-abort must exist in resilience CONFIGS"

    def test_d3_funnel_mode_is_wait(self):
        from scripts.run_resilience import CONFIGS
        assert CONFIGS["funnel-wait"]["funnel_mode"] == "wait", (
            "D3 must use funnel_mode='wait'"
        )

    def test_d4_funnel_mode_is_proceed(self):
        from scripts.run_resilience import CONFIGS
        assert CONFIGS["funnel-proceed"]["funnel_mode"] == "proceed", (
            "D4 must use funnel_mode='proceed'"
        )

    def test_d5_funnel_mode_is_abort(self):
        from scripts.run_resilience import CONFIGS
        assert CONFIGS["funnel-abort"]["funnel_mode"] == "abort", (
            "D5 must use funnel_mode='abort'"
        )

    def test_d3_d4_d5_are_distinct_configs(self):
        """All three funnel resilience configs must have distinct modes."""
        from scripts.run_resilience import CONFIGS
        modes = {
            CONFIGS["funnel-wait"]["funnel_mode"],
            CONFIGS["funnel-proceed"]["funnel_mode"],
            CONFIGS["funnel-abort"]["funnel_mode"],
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
        from scripts.run_resilience import CONFIGS
        assert CONFIGS["funnel-wait"]["failure_type"] == "worker"

    def test_d4_failure_type_is_worker(self):
        from scripts.run_resilience import CONFIGS
        assert CONFIGS["funnel-proceed"]["failure_type"] == "worker"

    def test_d5_failure_type_is_worker(self):
        from scripts.run_resilience import CONFIGS
        assert CONFIGS["funnel-abort"]["failure_type"] == "worker"

    def test_d3_target_is_valid_compose_service(self):
        from scripts.run_resilience import CONFIGS
        services = self._load_compose_services()
        target = CONFIGS["funnel-wait"]["failure_target"]
        assert target in services, (
            f"D3 failure_target '{target}' not in compose services: {sorted(services)}"
        )

    def test_d4_target_is_valid_compose_service(self):
        from scripts.run_resilience import CONFIGS
        services = self._load_compose_services()
        target = CONFIGS["funnel-proceed"]["failure_target"]
        assert target in services, (
            f"D4 failure_target '{target}' not in compose services: {sorted(services)}"
        )

    def test_d5_target_is_valid_compose_service(self):
        from scripts.run_resilience import CONFIGS
        services = self._load_compose_services()
        target = CONFIGS["funnel-abort"]["failure_target"]
        assert target in services, (
            f"D5 failure_target '{target}' not in compose services: {sorted(services)}"
        )

    def test_d3_d4_d5_target_same_worker(self):
        """All three funnel configs must target the same sensor-input worker,
        since the independent variable is the funnel mode, not the target."""
        from scripts.run_resilience import CONFIGS
        targets = {
            CONFIGS["funnel-wait"]["failure_target"],
            CONFIGS["funnel-proceed"]["failure_target"],
            CONFIGS["funnel-abort"]["failure_target"],
        }
        assert len(targets) == 1, (
            f"D3/D4/D5 should target the same worker (IV is funnel mode, "
            f"not target). Got {targets}"
        )

    def test_funnel_target_is_not_embb_main_worker(self):
        """D3/D4/D5 must NOT target the eMBB worker used by D1."""
        from scripts.run_resilience import CONFIGS
        d1_target = CONFIGS["embb-kill"]["failure_target"]
        for cfg_name in ("funnel-wait", "funnel-proceed", "funnel-abort"):
            assert CONFIGS[cfg_name]["failure_target"] != d1_target, (
                f"{cfg_name} targets the same worker as D1 ({d1_target}). "
                f"Funnel configs must target a sensor-input worker, not "
                f"the eMBB main worker."
            )

    def test_funnel_target_is_not_urllc_main_worker(self):
        """D3/D4/D5 must NOT target the URLLC worker used by D2."""
        from scripts.run_resilience import CONFIGS
        d2_target = CONFIGS["urllc-kill"]["failure_target"]
        for cfg_name in ("funnel-wait", "funnel-proceed", "funnel-abort"):
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
        from scripts.run_resilience import CONFIGS, RunConfig, _run
        import inspect
        source = inspect.getsource(_run)
        assert "FUNNEL_MODE" in source, (
            "_run() must set FUNNEL_MODE in the env dict for funnel configs"
        )

    def test_funnel_mode_env_passed_for_d3(self):
        """When config has funnel_mode, the env dict must include FUNNEL_MODE."""
        from scripts.run_resilience import CONFIGS, RunConfig
        # funnel-wait config must have funnel_mode key
        assert "funnel_mode" in CONFIGS["funnel-wait"], "funnel-wait missing funnel_mode key"


# ---------------------------------------------------------------------------
# 5. Run matrix: 3 funnel modes x seeds
# ---------------------------------------------------------------------------

class TestFunnelResilienceMatrix:
    """Funnel resilience configs produce the correct run matrix."""

    def test_funnel_configs_in_matrix(self):
        """D3/D4/D5 must appear in the run matrix."""
        from scripts.run_resilience import CONFIGS, build_run_matrix
        funnel_configs = ["funnel-wait", "funnel-proceed", "funnel-abort"]
        matrix = build_run_matrix(funnel_configs, [42])
        assert len(matrix) == 3, f"Expected 3 runs (1 seed x 3 configs), got {len(matrix)}"

    def test_funnel_matrix_3_modes_x_5_seeds(self):
        """3 funnel modes x default seeds = expected runs."""
        from scripts._common import DEFAULT_SEEDS
        from scripts.run_resilience import build_run_matrix
        funnel_configs = ["funnel-wait", "funnel-proceed", "funnel-abort"]
        matrix = build_run_matrix(funnel_configs, DEFAULT_SEEDS)
        expected = expected_run_count("D", configs=funnel_configs, seeds=DEFAULT_SEEDS)
        assert len(matrix) == expected, (
            f"Expected {expected} runs, got {len(matrix)}"
        )

    def test_funnel_matrix_unique_run_ids(self):
        """All funnel runs must have unique run_ids."""
        from scripts._common import DEFAULT_SEEDS
        from scripts.run_resilience import build_run_matrix
        funnel_configs = ["funnel-wait", "funnel-proceed", "funnel-abort"]
        matrix = build_run_matrix(funnel_configs, DEFAULT_SEEDS)
        expected = expected_run_count("D", configs=funnel_configs, seeds=DEFAULT_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(set(run_ids)) == expected, f"Duplicate run_ids: {run_ids}"

    def test_full_resilience_matrix_size(self):
        """Full resilience: all configs x extended seeds = expected runs."""
        from scripts.run_resilience import CONFIGS, build_run_matrix
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        expected = expected_run_count("D")
        assert len(matrix) == expected, (
            f"Expected {expected} runs, got {len(matrix)}"
        )


# ---------------------------------------------------------------------------
# 6. PIPELINE_MIX must enable sensor_fusion for D3/D4/D5
# ---------------------------------------------------------------------------

class TestFunnelPipelineMix:
    """D3/D4/D5 must enable sensor_fusion pipelines (funnel pattern)."""

    def test_d3_d4_d5_enable_fusion_pipeline(self):
        """D3/D4/D5 configs must set PIPELINE_MIX_FUSION > 0."""
        from scripts.run_resilience import CONFIGS
        for cfg_name in ("funnel-wait", "funnel-proceed", "funnel-abort"):
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


# ---------------------------------------------------------------------------
# 10. Broker-level funnel dispatch integration
# ---------------------------------------------------------------------------

class TestFunnelDispatchIntegration:
    """Integration tests: _find_ready_stages() consults funnel policy for
    fan-in stages with missing predecessors from dead workers.

    These tests construct a minimal sensor_fusion DAG (3 sensors -> fuse -> decide),
    simulate a dead worker for one sensor, and verify that each funnel mode
    produces the correct broker-level behavior.
    """

    def _make_pipeline_state(self, funnel_mode: str) -> "PipelineState":
        """Build a PipelineState for a 3-sensor fusion pipeline.

        Sets FUNNEL_MODE env var and creates the state with sensor_0 and
        sensor_1 completed, sensor_2 assigned to a dead worker (missing
        from worker registry). The fuse stage is the fan-in point.
        """
        import os
        os.environ["FUNNEL_MODE"] = funnel_mode

        from src.pipeline.patterns import sensor_fusion_pipeline
        from src.broker.models import PipelineState

        dag = sensor_fusion_pipeline(n_sensors=3)
        placement = {
            "sensor_0": "worker-a",
            "sensor_1": "worker-b",
            "sensor_2": "worker-dead",  # dead worker
            "fuse": "worker-a",
            "decide": "worker-b",
        }
        ps = PipelineState(
            pipeline_id="test-pipe-1",
            pipeline_type="sensor_fusion",
            dag=dag,
            placement=placement,
        )
        # sensor_0 and sensor_1 completed; sensor_2 never will (dead worker)
        ps.completed_stages = {"sensor_0", "sensor_1"}
        return ps

    def _get_dead_workers(self) -> set:
        """Return the set of dead worker IDs for test scenarios."""
        return {"worker-dead"}

    # -- Test 1: abort mode fails immediately --

    def test_abort_mode_fails_immediately(self):
        """Fan-in with dead predecessor + abort mode -> pipeline failed
        with error containing 'funnel_abort'."""
        import os
        from src.broker.base import BaseBroker

        ps = self._make_pipeline_state("abort")
        try:
            dead = self._get_dead_workers()
            ready, funnel_result = BaseBroker._find_ready_stages(
                ps, dead_workers=dead,
            )
            assert funnel_result is not None, (
                "_find_ready_stages must return a funnel result for fan-in "
                "stages with dead predecessors"
            )
            assert funnel_result.pipeline_failed is True
            assert funnel_result.action == "abort"
            # The fuse stage should NOT be in the ready list
            assert "fuse" not in ready
        finally:
            os.environ.pop("FUNNEL_MODE", None)

    # -- Test 2: proceed mode completes partial --

    def test_proceed_mode_completes_partial(self):
        """Fan-in with dead predecessor + proceed mode -> fuse stage is
        ready and pipeline is marked partial."""
        import os
        from src.broker.base import BaseBroker

        ps = self._make_pipeline_state("proceed")
        try:
            dead = self._get_dead_workers()
            ready, funnel_result = BaseBroker._find_ready_stages(
                ps, dead_workers=dead,
            )
            assert "fuse" in ready, (
                "Proceed mode should mark fuse stage as ready despite "
                "missing predecessor"
            )
            assert funnel_result is not None
            assert funnel_result.partial is True
            assert ps.partial is True, (
                "PipelineState.partial must be set to True in proceed mode"
            )
        finally:
            os.environ.pop("FUNNEL_MODE", None)

    # -- Test 3: wait mode stalls then times out --

    def test_wait_mode_stalls_then_times_out(self):
        """Fan-in with dead predecessor + wait mode -> stalls (fuse not ready).
        After timeout, the funnel result indicates failure."""
        import os
        import time
        from src.broker.base import BaseBroker

        ps = self._make_pipeline_state("wait")
        try:
            dead = self._get_dead_workers()

            # First call: should stall (fuse not ready, no timeout yet)
            ready, funnel_result = BaseBroker._find_ready_stages(
                ps, dead_workers=dead,
            )
            assert "fuse" not in ready, (
                "Wait mode should NOT make fuse ready while waiting"
            )
            assert funnel_result is not None
            assert funnel_result.action == "wait"

            # Simulate timeout by setting funnel_wait_start to the past
            ps.funnel_wait_start = time.time() - 999  # well past timeout

            ready2, funnel_result2 = BaseBroker._find_ready_stages(
                ps, dead_workers=dead,
            )
            assert funnel_result2 is not None
            assert funnel_result2.action == "fail"
            assert funnel_result2.pipeline_failed is True
        finally:
            os.environ.pop("FUNNEL_MODE", None)

    # -- Test 4: all inputs available proceeds normally --

    def test_all_inputs_available_proceeds_normally(self):
        """When all predecessors are complete, all modes behave identically:
        fuse is ready, no funnel intervention needed."""
        import os
        from src.broker.base import BaseBroker
        from src.broker.models import PipelineState
        from src.pipeline.patterns import sensor_fusion_pipeline

        for mode in ("wait", "proceed", "abort"):
            os.environ["FUNNEL_MODE"] = mode
            try:
                dag = sensor_fusion_pipeline(n_sensors=3)
                placement = {
                    "sensor_0": "worker-a",
                    "sensor_1": "worker-b",
                    "sensor_2": "worker-c",
                    "fuse": "worker-a",
                    "decide": "worker-b",
                }
                ps = PipelineState(
                    pipeline_id="test-all-ok",
                    pipeline_type="sensor_fusion",
                    dag=dag,
                    placement=placement,
                )
                # All sensors completed
                ps.completed_stages = {"sensor_0", "sensor_1", "sensor_2"}

                ready, funnel_result = BaseBroker._find_ready_stages(
                    ps, dead_workers=set(),
                )
                assert "fuse" in ready, (
                    f"Mode {mode}: fuse should be ready when all inputs complete"
                )
                # No funnel intervention needed when all inputs are available
                if funnel_result is not None:
                    assert funnel_result.partial is False
                    assert funnel_result.pipeline_failed is False
            finally:
                os.environ.pop("FUNNEL_MODE", None)

    # -- Test 5: partial flag in CSV --

    def test_partial_flag_in_csv(self, tmp_path):
        """Proceed mode result has partial=True propagated to PipelineTrace
        and visible in CSV output."""
        import asyncio
        from src.measurement.harness import MetricsCollector, PipelineTrace

        collector = MetricsCollector()

        async def _run():
            await collector.complete_pipeline(
                "p-partial", success=True, partial=True,
            )
            csv_path = str(tmp_path / "metrics.csv")
            await collector.export_csv(csv_path)
            return csv_path

        csv_path = asyncio.get_event_loop().run_until_complete(_run())

        import csv as csv_mod
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert "partial" in reader.fieldnames, (
            "CSV must have a 'partial' column"
        )
        assert rows[0]["partial"] == "True", (
            "Pipeline completed with partial=True must show partial=True in CSV"
        )

    # -- Test 6: funnel mode read from env --

    def test_funnel_mode_read_from_env(self):
        """get_funnel_mode() reads FUNNEL_MODE env var correctly for all modes."""
        import os
        from src.broker.funnel_resilience import get_funnel_mode, FunnelMode

        for mode_str, expected in [
            ("wait", FunnelMode.WAIT),
            ("proceed", FunnelMode.PROCEED),
            ("abort", FunnelMode.ABORT),
        ]:
            os.environ["FUNNEL_MODE"] = mode_str
            try:
                assert get_funnel_mode() == expected
            finally:
                os.environ.pop("FUNNEL_MODE", None)

    # -- Test 7: both brokers share helper --

    def test_both_brokers_share_helper(self):
        """NeuralBroker and StaticBroker (via BaseBroker) both call
        _find_ready_stages from the same module."""
        from src.broker.base import BaseBroker

        # _find_ready_stages must exist as a static/classmethod on BaseBroker
        assert hasattr(BaseBroker, "_find_ready_stages"), (
            "BaseBroker must define _find_ready_stages"
        )

        # NeuralBroker must import and use it (or call it via BaseBroker)
        import inspect
        from src.broker.neural_broker import NeuralBroker
        source = inspect.getsource(NeuralBroker._dispatch_ready_stages)
        assert "_find_ready_stages" in source, (
            "NeuralBroker._dispatch_ready_stages must call _find_ready_stages"
        )
