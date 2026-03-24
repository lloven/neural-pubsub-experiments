"""Tests for Phase D: Failure and adaptation.

Validates timing, configuration, and run matrix for Phase D experiments.
"""

from __future__ import annotations

import pytest

from scripts._common import DEFAULT_SEEDS, EXTENDED_SEEDS
from scripts.experiment_matrix import expected_run_count, get_configs, get_seeds
from scripts.run_phase_d import (
    CONFIGS,
    STRATEGIES,
    RunConfig,
    build_run_matrix,
    _strategy_env,
)


class TestPhaseDTiming:
    """Failure must be injected BEFORE the run ends."""

    def test_failure_delay_less_than_total_duration(self):
        """failure_delay_s must be < warmup_s + measurement_s,
        otherwise the failure never triggers."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        assert rc.failure_delay_s < total, (
            f"Failure at {rc.failure_delay_s}s but run ends at {total}s "
            f"(warmup={rc.warmup_s} + measurement={rc.measurement_s}). "
            f"Failure would never trigger."
        )

    def test_post_failure_observation_at_least_5_minutes(self):
        """After failure injection, at least 300s of observation must remain
        to measure recovery time and steady-state return."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        total = rc.warmup_s + rc.measurement_s
        post_failure = total - rc.failure_delay_s
        assert post_failure >= 300, (
            f"Only {post_failure}s after failure at {rc.failure_delay_s}s "
            f"(total={total}s). Need ≥300s for recovery observation."
        )


class TestPhaseDMatrix:
    """Phase D run matrix validation."""

    def test_uses_extended_seeds(self):
        """Phase D uses 10 seeds for statistical power."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        seeds_per_config = len(matrix) // len(CONFIGS)
        assert seeds_per_config == 10, (
            f"Phase D should use 10 seeds, got {seeds_per_config}"
        )

    def test_full_matrix_size(self):
        """configs x seeds = expected_run_count('D') runs."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        expected = expected_run_count("D")
        assert len(matrix) == expected, f"Expected {expected} runs, got {len(matrix)}"

    def test_single_config_matrix(self):
        """Single config x default seeds = expected runs."""
        from scripts._common import DEFAULT_SEEDS
        matrix = build_run_matrix(["D1"], DEFAULT_SEEDS)
        expected = expected_run_count("D", configs=["D1"], seeds=DEFAULT_SEEDS)
        assert len(matrix) == expected

    def test_all_configs_have_failure_type(self):
        """Every config must define a failure_type and target."""
        for name, cfg in CONFIGS.items():
            assert "failure_type" in cfg, f"{name} missing failure_type"
            assert "failure_target" in cfg, f"{name} missing failure_target"


class TestPhaseDRunId:
    """Each RunConfig must have a unique run_id for resume to work correctly."""

    def test_run_id_property_exists(self):
        """RunConfig must expose a run_id property (not just config_name)."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        assert hasattr(rc, 'run_id'), "RunConfig must have a run_id property"

    def test_run_id_includes_seed(self):
        """run_id must distinguish different seeds of the same config."""
        rc1 = RunConfig(config_name="D1", seed=42, failure_type="worker", failure_target="worker-d1-embb-1")
        rc2 = RunConfig(config_name="D1", seed=123, failure_type="worker", failure_target="worker-d1-embb-1")
        assert rc1.run_id != rc2.run_id, "Different seeds must have different run_ids"

    def test_run_id_includes_failure_type(self):
        """run_id must include the failure type for clarity."""
        rc = RunConfig(config_name="D1", seed=42, failure_type="worker", failure_target="worker-d1-embb-1")
        assert "worker" in rc.run_id

    def test_run_id_matches_run_function_format(self):
        """run_id property must match the format used in _run()."""
        rc = RunConfig(config_name="D1", seed=42, failure_type="worker", failure_target="worker-d1-embb-1")
        expected = f"{rc.config_name}_failure-{rc.failure_type}_{rc.strategy}_seed-{rc.seed}"
        assert rc.run_id == expected, f"Expected '{expected}', got '{rc.run_id}'"

    def test_all_d1_seeds_have_unique_run_ids(self):
        """All 10 D1 seeds must produce unique run_ids for resume tracking."""
        matrix = build_run_matrix(["D1"], EXTENDED_SEEDS)
        run_ids = [r.run_id for r in matrix]
        assert len(run_ids) == len(set(run_ids)), f"Duplicate run_ids: {run_ids}"

    def test_full_matrix_unique_run_ids(self):
        """All 50 runs in the full matrix must have unique run_ids."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        run_ids = [r.run_id for r in matrix]
        expected = expected_run_count("D")
        assert len(run_ids) == expected
        assert len(set(run_ids)) == expected, f"Duplicate run_ids found"


class TestPhaseDConfigs:
    """Phase D has 5 configs: D1 (eMBB worker kill), D2 (URLLC worker kill),
    D3 (funnel-wait), D4 (funnel-proceed), D5 (funnel-abort).
    Network partition moved to Phase C where cross-domain traffic makes it
    meaningful."""

    def test_configs_has_expected_entries(self):
        """Phase D configs must match the experiment matrix."""
        expected_configs = get_configs("D")
        assert sorted(CONFIGS.keys()) == sorted(expected_configs), (
            f"Expected configs {sorted(expected_configs)}, got {sorted(CONFIGS.keys())}"
        )

    def test_d2_target_is_urllc_worker(self):
        """D2 (formerly D4) must target the URLLC worker."""
        assert "D2" in CONFIGS, "D2 must exist in CONFIGS"
        target = CONFIGS["D2"]["failure_target"]
        assert target == "worker-d1-urllc-1", (
            f"D2 must target 'worker-d1-urllc-1' (URLLC worker), got '{target}'"
        )

    def test_d1_and_d2_target_different_workers(self):
        """D1 and D2 must target different workers to test distinct failure modes."""
        d1_target = CONFIGS["D1"]["failure_target"]
        d2_target = CONFIGS["D2"]["failure_target"]
        assert d1_target != d2_target, (
            f"D1 and D2 target the same worker '{d1_target}'. "
            f"D1 targets eMBB, D2 targets URLLC."
        )

    def test_all_configs_present(self):
        """D1 through D5 must all be present."""
        assert set(CONFIGS.keys()) == {"D1", "D2", "D3", "D4", "D5"}, (
            f"Expected configs {{D1, D2, D3, D4, D5}}, got {set(CONFIGS.keys())}"
        )

    def test_full_matrix_size(self):
        """All configs x extended seeds = expected_run_count('D') runs."""
        matrix = build_run_matrix(list(CONFIGS.keys()), EXTENDED_SEEDS)
        expected = expected_run_count("D")
        assert len(matrix) == expected, f"Expected {expected} runs, got {len(matrix)}"


class TestPhaseDDefaultSeeds:
    """Phase D should default to 10 extended seeds for recovery-time analysis."""

    def test_phase_d_defaults_to_extended_seeds(self):
        """run_phase_d.py with no --seeds arg should plan 10 seeds per config."""
        import subprocess, sys, os
        from scripts._common import PROJECT_ROOT
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_d", "--configs", "D1", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        assert "10 runs planned" in output, (
            f"Phase D should default to 10 seeds (EXTENDED_SEEDS), got:\n{output[-1000:]}"
        )


# ---------------------------------------------------------------------------
# Strategy support (S1/S2/S3)
# ---------------------------------------------------------------------------

class TestPhaseDStrategyFlag:
    """Phase D must support --strategy S1|S2|S3|all."""

    def test_strategy_cli_flag_exists(self):
        """run_phase_d.py must accept --strategy."""
        import subprocess, sys, os
        from scripts._common import PROJECT_ROOT
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_d", "--help"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        assert "--strategy" in result.stdout, (
            f"--strategy flag not found in Phase D help output:\n{result.stdout[-500:]}"
        )

    def test_strategy_defaults_to_s3(self):
        """Without --strategy, Phase D should default to S3 (neural)."""
        import subprocess, sys, os
        from scripts._common import PROJECT_ROOT
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_phase_d",
             "--configs", "D1", "--seeds", "42", "--dry-run"],
            capture_output=True, text=True, timeout=10,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        output = result.stdout + result.stderr
        # Default S3 means 1 run (1 config x 1 seed x 1 strategy)
        assert "1 runs planned" in output, (
            f"Default strategy S3 with 1 config, 1 seed should give 1 run:\n{output[-500:]}"
        )


class TestPhaseDStrategyConstants:
    """STRATEGIES dict must define S1, S2, S3 with correct env mappings."""

    def test_strategies_has_three_entries(self):
        assert set(STRATEGIES.keys()) == {"S1", "S2", "S3"}, (
            f"Expected STRATEGIES={{S1, S2, S3}}, got {set(STRATEGIES.keys())}"
        )

    def test_s1_is_round_robin(self):
        assert STRATEGIES["S1"]["placement"] == "round_robin"

    def test_s2_is_random(self):
        assert STRATEGIES["S2"]["placement"] == "random"

    def test_s3_is_neural(self):
        assert STRATEGIES["S3"]["placement"] == "neural"


class TestPhaseDStrategyEnv:
    """S1/S2/S3 must pass different env vars to the broker container."""

    def test_s1_env_sets_static_broker_round_robin(self):
        """S1 must set BROKER_MODULE to static_broker and PLACEMENT to round_robin."""
        env = _strategy_env("S1")
        assert env["BROKER_MODULE"] == "src.broker.static_broker"
        assert env["PLACEMENT"] == "round_robin"

    def test_s2_env_sets_static_broker_random(self):
        """S2 must set BROKER_MODULE to static_broker and PLACEMENT to random."""
        env = _strategy_env("S2")
        assert env["BROKER_MODULE"] == "src.broker.static_broker"
        assert env["PLACEMENT"] == "random"

    def test_s3_env_does_not_set_broker_module(self):
        """S3 (neural) uses the default broker; BROKER_MODULE must not be set."""
        env = _strategy_env("S3")
        assert "BROKER_MODULE" not in env, (
            "S3 must not set BROKER_MODULE (uses default neural_broker)"
        )

    def test_s3_env_sets_placement_strategy_neural(self):
        """S3 must set PLACEMENT_STRATEGY to neural."""
        env = _strategy_env("S3")
        assert env.get("PLACEMENT_STRATEGY") == "neural"


class TestPhaseDStrategyRunId:
    """run_id must include the strategy name for disambiguation."""

    def test_run_id_includes_strategy(self):
        """RunConfig run_id must contain the strategy label."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
            strategy="S1",
        )
        assert "S1" in rc.run_id, f"Strategy S1 not in run_id: {rc.run_id}"

    def test_different_strategies_different_run_ids(self):
        """Same config+seed but different strategies must yield different run_ids."""
        common = dict(config_name="D1", seed=42,
                      failure_type="worker", failure_target="worker-d1-embb-1")
        rc_s1 = RunConfig(**common, strategy="S1")
        rc_s2 = RunConfig(**common, strategy="S2")
        rc_s3 = RunConfig(**common, strategy="S3")
        ids = {rc_s1.run_id, rc_s2.run_id, rc_s3.run_id}
        assert len(ids) == 3, f"Expected 3 unique run_ids, got {ids}"

    def test_run_id_format(self):
        """run_id must follow the pattern: {config}_failure-{type}_{strategy}_seed-{seed}."""
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
            strategy="S2",
        )
        expected = "D1_failure-worker_S2_seed-42"
        assert rc.run_id == expected, f"Expected '{expected}', got '{rc.run_id}'"


class TestPhaseDStrategyMatrix:
    """--strategy all produces configs x 3 strategies x seeds runs."""

    def test_strategy_all_matrix_d1_d2_default_seeds(self):
        """2 configs x 3 strategies x default seeds runs (H6 comparison)."""
        matrix = build_run_matrix(
            ["D1", "D2"], DEFAULT_SEEDS, strategies=["S1", "S2", "S3"],
        )
        expected = expected_run_count(
            "D", configs=["D1", "D2"], seeds=DEFAULT_SEEDS,
            strategies=["S1", "S2", "S3"],
        )
        assert len(matrix) == expected, (
            f"Expected {expected} runs, got {len(matrix)}"
        )

    def test_strategy_all_full_configs_default_seeds(self):
        """All configs x 3 strategies x 5 default seeds."""
        n_configs = len(CONFIGS)
        matrix = build_run_matrix(
            list(CONFIGS.keys()), DEFAULT_SEEDS, strategies=["S1", "S2", "S3"],
        )
        expected = n_configs * 3 * len(DEFAULT_SEEDS)
        assert len(matrix) == expected, (
            f"Expected {expected} runs ({n_configs} configs x 3 strategies x "
            f"{len(DEFAULT_SEEDS)} seeds), got {len(matrix)}"
        )

    def test_strategy_all_unique_run_ids(self):
        """All runs in the strategy-all matrix must have unique run_ids."""
        matrix = build_run_matrix(
            ["D1", "D2"], DEFAULT_SEEDS, strategies=["S1", "S2", "S3"],
        )
        run_ids = [r.run_id for r in matrix]
        expected = expected_run_count(
            "D", configs=["D1", "D2"], seeds=DEFAULT_SEEDS,
            strategies=["S1", "S2", "S3"],
        )
        assert len(set(run_ids)) == expected, f"Duplicate run_ids in strategy-all matrix"

    def test_single_strategy_matrix_unchanged(self):
        """Passing strategies=['S3'] must give the same count as no strategy arg."""
        matrix_default = build_run_matrix(list(CONFIGS.keys()), DEFAULT_SEEDS)
        matrix_s3 = build_run_matrix(
            list(CONFIGS.keys()), DEFAULT_SEEDS, strategies=["S3"],
        )
        assert len(matrix_default) == len(matrix_s3), (
            f"Default ({len(matrix_default)}) vs S3-only ({len(matrix_s3)}) "
            f"should be equal"
        )

    def test_strategy_all_contains_all_three_strategies(self):
        """The matrix with all strategies must contain S1, S2, and S3 runs."""
        matrix = build_run_matrix(
            ["D1"], [42], strategies=["S1", "S2", "S3"],
        )
        strategies_found = {r.strategy for r in matrix}
        assert strategies_found == {"S1", "S2", "S3"}, (
            f"Expected all three strategies, got {strategies_found}"
        )

    def test_strategy_all_each_config_gets_each_strategy(self):
        """Every config must appear with every strategy."""
        matrix = build_run_matrix(
            list(CONFIGS.keys()), [42], strategies=["S1", "S2", "S3"],
        )
        for config_name in CONFIGS:
            for strat in ["S1", "S2", "S3"]:
                matches = [r for r in matrix
                           if r.config_name == config_name and r.strategy == strat]
                assert len(matches) == 1, (
                    f"Config {config_name} with strategy {strat}: "
                    f"expected 1 run, got {len(matches)}"
                )
