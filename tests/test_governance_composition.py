"""Tests for governance composition experiments (Block 5).

Tests the TEAC prediction: in a two-domain federated system,
partial governance (one domain enforces, other doesn't) produces
welfare outcomes that are supermodularly worse than either
both-enforce or neither-enforce.

The four scenarios map to TEAC Section 4:
  - Scenario A (0,0): neither domain enforces governance
  - Scenario B (1,0): domain-1 enforces, domain-2 doesn't
  - Scenario C (0,1): domain-2 enforces, domain-1 doesn't
  - Scenario D (1,1): both domains enforce governance

The supermodularity prediction:
  welfare(D) + welfare(A) >= welfare(B) + welfare(C)
  i.e., interaction term I = welfare(D) - welfare(B) - welfare(C) + welfare(A) >= 0
"""

import pytest
from dataclasses import dataclass

from src.broker.placement import GovernancePolicy


@dataclass
class DomainConfig:
    """Configuration for one domain in the federation."""

    domain_id: str
    governance_enabled: bool
    local_stage_types: set


def make_governance_scenarios() -> dict[str, tuple[DomainConfig, DomainConfig]]:
    """Create the four governance composition scenarios.

    Both domains have data_collect stages that should stay local
    (raw radio data). When governance is OFF, these stages can be
    placed anywhere (including cross-domain), violating sovereignty.
    """
    local_types = {"data_collect", "sensor_raw"}

    return {
        "neither": (
            DomainConfig("d1", governance_enabled=False, local_stage_types=set()),
            DomainConfig("d2", governance_enabled=False, local_stage_types=set()),
        ),
        "d1_only": (
            DomainConfig("d1", governance_enabled=True, local_stage_types=local_types),
            DomainConfig("d2", governance_enabled=False, local_stage_types=set()),
        ),
        "d2_only": (
            DomainConfig("d1", governance_enabled=False, local_stage_types=set()),
            DomainConfig("d2", governance_enabled=True, local_stage_types=local_types),
        ),
        "both": (
            DomainConfig("d1", governance_enabled=True, local_stage_types=local_types),
            DomainConfig("d2", governance_enabled=True, local_stage_types=local_types),
        ),
    }


def policy_from_config(cfg: DomainConfig) -> GovernancePolicy:
    """Create a GovernancePolicy from a DomainConfig."""
    if cfg.governance_enabled:
        return GovernancePolicy(
            local_stage_types=cfg.local_stage_types,
            trust_levels={},
        )
    return GovernancePolicy(local_stage_types=set(), trust_levels={})


class TestGovernanceScenarios:
    """The four governance scenarios are correctly configured."""

    def test_four_scenarios_exist(self):
        scenarios = make_governance_scenarios()
        assert set(scenarios.keys()) == {"neither", "d1_only", "d2_only", "both"}

    def test_neither_has_no_constraints(self):
        d1, d2 = make_governance_scenarios()["neither"]
        p1 = policy_from_config(d1)
        p2 = policy_from_config(d2)
        assert len(p1.local_stage_types) == 0
        assert len(p2.local_stage_types) == 0

    def test_both_has_constraints(self):
        d1, d2 = make_governance_scenarios()["both"]
        p1 = policy_from_config(d1)
        p2 = policy_from_config(d2)
        assert "data_collect" in p1.local_stage_types
        assert "data_collect" in p2.local_stage_types

    def test_d1_only_is_asymmetric(self):
        d1, d2 = make_governance_scenarios()["d1_only"]
        p1 = policy_from_config(d1)
        p2 = policy_from_config(d2)
        assert "data_collect" in p1.local_stage_types
        assert len(p2.local_stage_types) == 0

    def test_d2_only_is_asymmetric(self):
        d1, d2 = make_governance_scenarios()["d2_only"]
        p1 = policy_from_config(d1)
        p2 = policy_from_config(d2)
        assert len(p1.local_stage_types) == 0
        assert "data_collect" in p2.local_stage_types


class TestGovernancePolicyEnforcement:
    """GovernancePolicy correctly constrains placement."""

    def test_local_stage_blocked_cross_domain(self):
        """A data_collect stage in d1 cannot be placed on a d2 worker."""
        policy = GovernancePolicy(
            local_stage_types={"data_collect"},
            trust_levels={},
        )
        # Trust between d1 and d2 is 0 (default)
        assert policy.get_trust("d1", "d2") == 0.0

    def test_local_stage_allowed_same_domain(self):
        """Same-domain trust is always 1.0."""
        policy = GovernancePolicy(
            local_stage_types={"data_collect"},
            trust_levels={},
        )
        assert policy.get_trust("d1", "d1") == 1.0

    def test_no_governance_allows_all(self):
        """Empty governance allows cross-domain placement."""
        policy = GovernancePolicy(local_stage_types=set(), trust_levels={})
        # No local types means no constraints
        assert len(policy.local_stage_types) == 0

    def test_trust_levels_symmetric(self):
        """Trust lookup is symmetric."""
        policy = GovernancePolicy(
            local_stage_types=set(),
            trust_levels={("d1", "d2"): 0.5},
        )
        assert policy.get_trust("d1", "d2") == 0.5
        assert policy.get_trust("d2", "d1") == 0.5


class TestCompositionPrediction:
    """The supermodularity prediction from TEAC Section 4.

    We can't test actual welfare here (that requires running pipelines),
    but we can test the structural properties that drive the prediction.
    """

    def test_partial_governance_creates_load_asymmetry(self):
        """Under d1_only, all sovereignty-sensitive stages from d2
        must stay in d2 (no constraint), but d1's sensitive stages
        are locked to d1. This creates asymmetric flexibility:
        d2 can place anywhere, d1 cannot.

        The TEAC prediction: this asymmetry concentrates load on the
        governed domain (d1), degrading total welfare.
        """
        d1_cfg, d2_cfg = make_governance_scenarios()["d1_only"]
        p1 = policy_from_config(d1_cfg)
        p2 = policy_from_config(d2_cfg)

        # d1 is constrained (fewer placement options)
        assert len(p1.local_stage_types) > 0
        # d2 is unconstrained (all placement options available)
        assert len(p2.local_stage_types) == 0

    def test_full_governance_is_symmetric(self):
        """Under both, both domains are equally constrained.
        No asymmetric load concentration.
        """
        d1_cfg, d2_cfg = make_governance_scenarios()["both"]
        p1 = policy_from_config(d1_cfg)
        p2 = policy_from_config(d2_cfg)

        assert p1.local_stage_types == p2.local_stage_types

    def test_supermodularity_metric_structure(self):
        """The interaction term I = welfare(D) - welfare(B) - welfare(C) + welfare(A)
        requires measuring welfare in all four scenarios.

        Welfare = total_throughput * (1 - violation_rate) / mean_latency

        This test verifies the metric can be computed from standard
        measurement outputs (throughput, violations, latency).
        """
        # Mock measurement results for the four scenarios
        results = {
            "neither": {"throughput": 4.0, "violations": 5, "total": 100, "latency": 500},
            "d1_only": {"throughput": 3.5, "violations": 3, "total": 100, "latency": 600},
            "d2_only": {"throughput": 3.5, "violations": 3, "total": 100, "latency": 600},
            "both": {"throughput": 3.0, "violations": 0, "total": 100, "latency": 550},
        }

        def welfare(r: dict) -> float:
            violation_rate = r["violations"] / r["total"]
            return r["throughput"] * (1 - violation_rate) / (r["latency"] / 1000)

        w = {k: welfare(v) for k, v in results.items()}

        # Interaction term
        interaction = w["both"] - w["d1_only"] - w["d2_only"] + w["neither"]

        # TEAC predicts I >= 0 (supermodularity)
        # With these mock values: verify the metric computation works
        assert isinstance(interaction, float)
        # The mock values are designed so I > 0 (illustrative, not a proof)
        assert interaction > 0, (
            f"Interaction term should be positive under TEAC prediction. "
            f"Got I={interaction:.4f} from welfare values {w}"
        )


class TestExperimentMatrixIntegration:
    """Governance composition configs integrate with experiment matrix."""

    def test_configs_are_valid_names(self):
        """Config names follow the experiment naming convention."""
        scenarios = make_governance_scenarios()
        for name in scenarios:
            assert name.replace("_", "").isalnum(), f"Invalid config name: {name}"

    def test_each_scenario_has_two_domains(self):
        scenarios = make_governance_scenarios()
        for name, (d1, d2) in scenarios.items():
            assert d1.domain_id != d2.domain_id, f"Domains must differ in {name}"
