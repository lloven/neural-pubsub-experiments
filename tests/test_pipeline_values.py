"""Tests for pipeline value budgets (Block 3, Step 11).

Each pipeline has a value_budget representing the maximum total cost
the submitter will pay. High-value pipelines (URLLC CQI) have high
budgets; low-value pipelines (batch anomaly detection) have low budgets.
This creates the demand curve needed for market clearing.
"""

import pytest

from src.pipeline.dag import PipelineDAG, Stage, Edge


class TestPipelineValueBudget:
    """PipelineDAG carries a value_budget."""

    def test_default_value_budget_is_none(self):
        dag = PipelineDAG()
        assert dag.value_budget is None

    def test_set_value_budget(self):
        dag = PipelineDAG(value_budget=10.0)
        assert dag.value_budget == 10.0

    def test_value_budget_zero_is_valid(self):
        dag = PipelineDAG(value_budget=0.0)
        assert dag.value_budget == 0.0

    def test_pipeline_accepts_when_cost_within_budget(self):
        dag = PipelineDAG(value_budget=10.0)
        assert dag.accepts_cost(8.0) is True

    def test_pipeline_rejects_when_cost_exceeds_budget(self):
        dag = PipelineDAG(value_budget=10.0)
        assert dag.accepts_cost(12.0) is False

    def test_pipeline_accepts_when_cost_equals_budget(self):
        dag = PipelineDAG(value_budget=10.0)
        assert dag.accepts_cost(10.0) is True

    def test_no_budget_always_accepts(self):
        """When value_budget is None (legacy), accept any cost."""
        dag = PipelineDAG()
        assert dag.accepts_cost(999999.0) is True
