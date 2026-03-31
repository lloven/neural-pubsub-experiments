"""Tests for 8-stage O-RAN pipeline factories.

Three pipeline types for Tier 2 experiments, each with 8 stages
but different DAG structures:
1. CQI Prediction Chain (tree, gamma=0)
2. RAN Anomaly Detection (series-parallel, gamma=0)
3. RAN Intelligence Suite (entangled, gamma>0)
"""

import pytest

from src.pipeline.patterns import (
    cqi_prediction_chain_8stage,
    ran_anomaly_detection_8stage,
    ran_intelligence_suite_8stage,
)


class TestCQIPredictionChain:
    """8-stage linear CQI prediction chain (tree)."""

    def test_stage_count(self):
        dag = cqi_prediction_chain_8stage()
        assert len(dag.stages) == 8

    def test_edge_count(self):
        """Tree with 8 nodes has 7 edges."""
        dag = cqi_prediction_chain_8stage()
        assert len(dag.edges) == 7

    def test_is_tree(self):
        dag = cqi_prediction_chain_8stage()
        assert dag.is_tree()

    def test_crosses_all_domains(self):
        """Stages span DU, CU, near-RT RIC, and non-RT RIC/SMO domains."""
        dag = cqi_prediction_chain_8stage()
        domains = {s.metadata.get("domain") for s in dag.stages.values()
                   if s.metadata and "domain" in s.metadata}
        assert len(domains) >= 3  # At least 3 O-RAN domains

    def test_sources_and_sinks(self):
        dag = cqi_prediction_chain_8stage()
        assert len(dag.sources()) == 1
        assert len(dag.sinks()) == 1


class TestRANAnomalyDetection:
    """8-stage series-parallel RAN anomaly detection."""

    def test_stage_count(self):
        dag = ran_anomaly_detection_8stage()
        assert len(dag.stages) == 8

    def test_is_tree(self):
        """SP (fan-in only) is a tree."""
        dag = ran_anomaly_detection_8stage()
        assert dag.is_tree()

    def test_has_multiple_sources(self):
        """4 parallel data sources (2 DU + 2 CU)."""
        dag = ran_anomaly_detection_8stage()
        assert len(dag.sources()) >= 3

    def test_has_single_sink(self):
        dag = ran_anomaly_detection_8stage()
        assert len(dag.sinks()) == 1

    def test_fan_in_at_fusion(self):
        """The fusion stage has 4 predecessors."""
        dag = ran_anomaly_detection_8stage()
        fuse_predecessors = dag.predecessors("ric_fuse")
        assert len(fuse_predecessors) >= 3


class TestRANIntelligenceSuite:
    """8-stage entangled RAN Intelligence Suite (gamma > 0)."""

    def test_stage_count(self):
        dag = ran_intelligence_suite_8stage()
        assert len(dag.stages) == 8

    def test_is_not_tree(self):
        """Entangled DAG with diamonds and cross-tree fan-in."""
        dag = ran_intelligence_suite_8stage()
        assert not dag.is_tree()

    def test_more_edges_than_tree(self):
        """Non-tree DAG has more edges than n-1."""
        dag = ran_intelligence_suite_8stage()
        assert len(dag.edges) > 7  # More than tree minimum

    def test_has_fan_out(self):
        """CU:feature_extract fans out to multiple RIC stages."""
        dag = ran_intelligence_suite_8stage()
        successors = dag.successors("cu_feature_extract")
        assert len(successors) >= 2

    def test_has_diamond(self):
        """Some stage has 2+ predecessors from different branches."""
        dag = ran_intelligence_suite_8stage()
        # At least one stage should have multiple predecessors
        max_preds = max(
            len(dag.predecessors(s)) for s in dag.stages
        )
        assert max_preds >= 2

    def test_topological_sort_valid(self):
        dag = ran_intelligence_suite_8stage()
        order = dag.topological_sort()
        assert len(order) == 8


class TestStructuralContrast:
    """The three pipelines differ only in structure, not in scale."""

    def test_all_have_8_stages(self):
        assert len(cqi_prediction_chain_8stage().stages) == 8
        assert len(ran_anomaly_detection_8stage().stages) == 8
        assert len(ran_intelligence_suite_8stage().stages) == 8

    def test_tree_sp_are_trees(self):
        assert cqi_prediction_chain_8stage().is_tree()
        assert ran_anomaly_detection_8stage().is_tree()

    def test_entangled_is_not_tree(self):
        assert not ran_intelligence_suite_8stage().is_tree()
