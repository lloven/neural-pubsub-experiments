"""Tests for experiment_matrix.py new-name and legacy compatibility.

Validates that:
- New descriptive names (baseline, slicing, ...) are primary keys
- Old letter names (A, B, ...) work via LEGACY_MAP
- All accessor functions accept both old and new names
- print_summary() produces output without errors
"""

from __future__ import annotations

import pytest

from scripts.experiment_matrix import (
    EXPERIMENTS,
    LEGACY_MAP,
    expected_run_count,
    get_configs,
    get_seeds,
    get_transports,
    resolve_phase,
)


# ---------------------------------------------------------------------------
# New names are primary keys
# ---------------------------------------------------------------------------


class TestNewNamesArePrimary:
    """EXPERIMENTS dict uses new descriptive names as keys."""

    def test_baseline_is_key(self):
        assert "baseline" in EXPERIMENTS

    def test_slicing_is_key(self):
        assert "slicing" in EXPERIMENTS

    def test_contention_is_key(self):
        assert "contention" in EXPERIMENTS

    def test_federation_is_key(self):
        assert "federation" in EXPERIMENTS

    def test_resilience_is_key(self):
        assert "resilience" in EXPERIMENTS

    def test_stress_is_key(self):
        assert "stress" in EXPERIMENTS

    def test_old_letter_names_not_primary(self):
        """Old names (A, B, etc.) must NOT be primary keys."""
        for old in ["A", "B", "C", "D", "E", "A6"]:
            assert old not in EXPERIMENTS, f"{old} should not be a primary key"


# ---------------------------------------------------------------------------
# LEGACY_MAP
# ---------------------------------------------------------------------------


class TestLegacyMap:
    """LEGACY_MAP maps old letter names to new descriptive names."""

    def test_a_maps_to_baseline(self):
        assert LEGACY_MAP["A"] == "baseline"

    def test_b_maps_to_slicing(self):
        assert LEGACY_MAP["B"] == "slicing"

    def test_c_maps_to_federation(self):
        assert LEGACY_MAP["C"] == "federation"

    def test_d_maps_to_resilience(self):
        assert LEGACY_MAP["D"] == "resilience"

    def test_e_maps_to_stress(self):
        assert LEGACY_MAP["E"] == "stress"

    def test_a6_maps_to_contention(self):
        assert LEGACY_MAP["A6"] == "contention"

    def test_all_legacy_values_exist_in_experiments(self):
        for old, new in LEGACY_MAP.items():
            assert new in EXPERIMENTS, f"LEGACY_MAP[{old!r}] -> {new!r} not in EXPERIMENTS"


# ---------------------------------------------------------------------------
# resolve_phase() accepts both old and new names
# ---------------------------------------------------------------------------


class TestResolvePhase:
    """resolve_phase() translates old names, passes through new names."""

    def test_new_name_passthrough(self):
        assert resolve_phase("baseline") == "baseline"

    def test_old_name_translated(self):
        assert resolve_phase("A") == "baseline"

    def test_old_name_b(self):
        assert resolve_phase("B") == "slicing"

    def test_unknown_name_raises(self):
        with pytest.raises(KeyError):
            resolve_phase("Z")


# ---------------------------------------------------------------------------
# Accessor functions accept both old and new names
# ---------------------------------------------------------------------------


class TestAccessorsAcceptBothNames:
    """get_configs, get_seeds, get_transports, expected_run_count work with old and new."""

    @pytest.mark.parametrize("old,new", list(LEGACY_MAP.items()))
    def test_get_configs_same(self, old, new):
        assert get_configs(old) == get_configs(new)

    @pytest.mark.parametrize("old,new", list(LEGACY_MAP.items()))
    def test_get_seeds_same(self, old, new):
        assert get_seeds(old) == get_seeds(new)

    @pytest.mark.parametrize("old,new", [
        (k, v) for k, v in LEGACY_MAP.items() if k != "A"  # A raises ValueError
    ])
    def test_expected_run_count_same(self, old, new):
        assert expected_run_count(old) == expected_run_count(new)


# ---------------------------------------------------------------------------
# Data integrity: new names preserve the same data as old definitions
# ---------------------------------------------------------------------------


class TestDataIntegrity:
    """Experiment data is preserved after the rename."""

    def test_baseline_has_3_configs(self):
        assert get_configs("baseline") == ["rr", "random", "neural"]

    def test_contention_has_3_configs(self):
        assert get_configs("contention") == ["20pps", "50pps", "10pps-kill"]

    def test_slicing_has_5_configs(self):
        assert get_configs("slicing") == ["flat", "neural", "rr", "gov", "gov-fail"]

    def test_federation_has_5_configs(self):
        assert get_configs("federation") == ["static", "neural", "gov", "broker-kill", "net-part"]

    def test_resilience_has_5_configs(self):
        assert get_configs("resilience") == ["embb-kill", "urllc-kill", "funnel-wait", "funnel-proceed", "funnel-abort"]

    def test_stress_has_12_configs(self):
        assert len(get_configs("stress")) == 12

    def test_contention_run_count(self):
        assert expected_run_count("contention") == 15  # 3 configs x 5 seeds

    def test_slicing_run_count(self):
        assert expected_run_count("slicing") == 50  # 5 x 2 x 5

    def test_resilience_run_count(self):
        assert expected_run_count("resilience") == 50  # 5 x 1(S3) x 10

    def test_stress_run_count(self):
        assert expected_run_count("stress") == 60  # 12 x 5

    def test_market_run_count(self):
        assert expected_run_count("market") == 270  # 6 configs x 3 pipelines x 3 loads x 5 seeds

    def test_governance_run_count(self):
        assert expected_run_count("governance") == 60  # 4 configs x 3 pipelines x 1 load x 5 seeds

    def test_market_has_6_configs(self):
        assert len(get_configs("market")) == 6

    def test_governance_has_4_configs(self):
        assert len(get_configs("governance")) == 4


# ---------------------------------------------------------------------------
# print_summary()
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Manuscript hypothesis mapping
# ---------------------------------------------------------------------------


class TestHypothesisMap:
    """HYPOTHESIS_MAP links manuscript IDs to experiment phases and configs."""

    def test_hypothesis_map_exists(self):
        from scripts.experiment_matrix import HYPOTHESIS_MAP
        assert isinstance(HYPOTHESIS_MAP, dict)

    def test_all_tier2_hypotheses_present(self):
        from scripts.experiment_matrix import HYPOTHESIS_MAP
        tier2 = [
            "H-NEAR", "H-EDGE", "H-ENTANGLE", "H-OVERLOAD",
            "H-COMPOSE", "H-HEURISTIC", "H-ADAPT",
            "H-FEDERATION", "H-RESILIENCE",
        ]
        for h in tier2:
            assert h in HYPOTHESIS_MAP, f"Missing hypothesis {h}"

    def test_hypothesis_phases_are_valid(self):
        from scripts.experiment_matrix import HYPOTHESIS_MAP, EXPERIMENTS
        for h_id, spec in HYPOTHESIS_MAP.items():
            phase = spec["phase"]
            assert phase in EXPERIMENTS, (
                f"Hypothesis {h_id} references unknown phase {phase!r}"
            )

    def test_hypothesis_configs_exist_in_phase(self):
        from scripts.experiment_matrix import HYPOTHESIS_MAP, EXPERIMENTS
        for h_id, spec in HYPOTHESIS_MAP.items():
            if "configs" not in spec:
                continue
            phase_configs = EXPERIMENTS[spec["phase"]]["configs"]
            for cfg in spec["configs"]:
                assert cfg in phase_configs, (
                    f"Hypothesis {h_id}: config {cfg!r} not in "
                    f"phase {spec['phase']!r} configs {phase_configs}"
                )

    def test_oracle_global_in_market_configs(self):
        from scripts.experiment_matrix import EXPERIMENTS
        assert "oracle-global" in EXPERIMENTS["market"]["configs"]


# ---------------------------------------------------------------------------
# print_summary()
# ---------------------------------------------------------------------------


class TestPrintSummary:
    """print_summary() produces clean output."""

    def test_print_summary_runs(self, capsys):
        from scripts.experiment_matrix import print_summary
        print_summary()
        captured = capsys.readouterr()
        assert "baseline" in captured.out
        assert "slicing" in captured.out
        assert len(captured.out) > 100  # Nontrivial output
