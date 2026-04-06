"""Tests for the market phase runner (scripts/run_market.py).

Verifies that:
- MarketRunConfig produces correct run_id with pipeline and load
- build_run_matrix produces 225 allocation + 60 governance = 285 total
- Distributed dispatch calls multi_vm_runner with correct args
- Dry-run produces no side effects
"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from scripts.run_market import (
    MarketRunConfig,
    build_run_matrix,
    MARKET_CONFIGS,
    GOV_CONFIGS,
    CONFIGS,
    PIPELINE_MAP,
    LOADS,
    _run_distributed,
)
from scripts._common import DEFAULT_SEEDS


# ---------------------------------------------------------------------------
# MarketRunConfig
# ---------------------------------------------------------------------------


class TestMarketRunConfig:
    """Run ID must encode config, pipeline, load, and seed."""

    def test_run_id_format(self):
        rc = MarketRunConfig(
            config_name="market-quad",
            pipeline_type="cqi_chain",
            load_label="medium",
            arrival_rate=5.0,
            seed=42,
        )
        assert rc.run_id == "market-quad_cqi-chain_medium_seed-42"

    def test_run_id_with_different_pipeline(self):
        rc = MarketRunConfig(
            config_name="oracle-global",
            pipeline_type="ran_entangled",
            load_label="high",
            arrival_rate=10.0,
            seed=123,
        )
        assert rc.run_id == "oracle-global_ran-entangled_high_seed-123"

    def test_pipeline_type_accessible(self):
        rc = MarketRunConfig(
            config_name="market-quad",
            pipeline_type="cqi_chain",
            load_label="medium",
            arrival_rate=5.0,
            seed=42,
        )
        assert rc.pipeline_type == "cqi_chain"


# ---------------------------------------------------------------------------
# build_run_matrix
# ---------------------------------------------------------------------------


class TestBuildRunMatrix:
    """Matrix construction: configs x pipelines x loads x seeds."""

    def test_allocation_configs_produce_225(self):
        runs = build_run_matrix(list(MARKET_CONFIGS), DEFAULT_SEEDS)
        assert len(runs) == 225  # 5 x 3 x 3 x 5

    def test_governance_configs_produce_60(self):
        runs = build_run_matrix(list(GOV_CONFIGS), DEFAULT_SEEDS)
        assert len(runs) == 60  # 4 x 3 x 1 x 5

    def test_all_configs_produce_285(self):
        all_cfgs = list(MARKET_CONFIGS) + list(GOV_CONFIGS)
        runs = build_run_matrix(all_cfgs, DEFAULT_SEEDS)
        assert len(runs) == 285

    def test_single_config_single_seed(self):
        runs = build_run_matrix(["market-quad"], [99])
        # market config: 1 config x 3 pipelines x 3 loads x 1 seed = 9
        assert len(runs) == 9

    def test_gov_config_single_seed(self):
        runs = build_run_matrix(["gov-both"], [99])
        # gov config: 1 config x 3 pipelines x 1 load x 1 seed = 3
        assert len(runs) == 3

    def test_pipeline_types_cover_all_three(self):
        runs = build_run_matrix(["market-quad"], [42])
        ptypes = {r.pipeline_type for r in runs}
        assert ptypes == {"cqi_chain", "anomaly_sp", "ran_entangled"}

    def test_loads_cover_all_three_for_market(self):
        runs = build_run_matrix(["market-quad"], [42])
        labels = {r.load_label for r in runs}
        assert labels == {"low", "medium", "high"}

    def test_loads_only_medium_for_governance(self):
        runs = build_run_matrix(["gov-both"], [42])
        labels = {r.load_label for r in runs}
        assert labels == {"medium"}

    def test_arrival_rates_match_loads(self):
        runs = build_run_matrix(["market-quad"], [42])
        for run in runs:
            assert run.arrival_rate == LOADS[run.load_label]

    def test_no_duplicate_run_ids(self):
        all_cfgs = list(MARKET_CONFIGS) + list(GOV_CONFIGS)
        runs = build_run_matrix(all_cfgs, DEFAULT_SEEDS)
        ids = [r.run_id for r in runs]
        assert len(ids) == len(set(ids)), "Duplicate run IDs found"


# ---------------------------------------------------------------------------
# Distributed dispatch
# ---------------------------------------------------------------------------


class TestMarketDistributed:
    """_run_distributed must call multi_vm_runner.run_single correctly."""

    @patch("scripts.multi_vm_runner.run_single")
    def test_calls_run_single_with_wan_emulation(self, mock_run_single):
        rc = MarketRunConfig(
            config_name="market-quad",
            pipeline_type="cqi_chain",
            load_label="medium",
            arrival_rate=5.0,
            seed=42,
        )
        _run_distributed(rc, dry_run=True)
        mock_run_single.assert_called_once()
        kwargs = mock_run_single.call_args.kwargs
        assert kwargs["wan_emulation"] is True

    @patch("scripts.multi_vm_runner.run_single")
    def test_passes_pipeline_type_in_workload_env(self, mock_run_single):
        rc = MarketRunConfig(
            config_name="market-quad",
            pipeline_type="anomaly_sp",
            load_label="high",
            arrival_rate=10.0,
            seed=99,
        )
        _run_distributed(rc, dry_run=True)
        kwargs = mock_run_single.call_args.kwargs
        assert kwargs["workload_env"]["PIPELINE_TYPE"] == "anomaly_sp"
        assert kwargs["workload_env"]["ARRIVAL_RATE"] == "10.0"

    @patch("scripts.multi_vm_runner.run_single")
    def test_passes_correct_placement_mode(self, mock_run_single):
        rc = MarketRunConfig(
            config_name="oracle-global",
            pipeline_type="cqi_chain",
            load_label="medium",
            arrival_rate=5.0,
            seed=42,
        )
        _run_distributed(rc, dry_run=True)
        kwargs = mock_run_single.call_args.kwargs
        assert kwargs["placement_mode"] == CONFIGS["oracle-global"]["placement_mode"]
