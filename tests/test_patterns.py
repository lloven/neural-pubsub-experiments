"""Unit tests for pipeline factory functions (src/pipeline/patterns.py)."""

import pytest

from src.pipeline.patterns import (
    anomaly_detection_pipeline,
    cqi_prediction_pipeline,
    funnel_pipeline,
    map_pipeline,
    sensor_fusion_pipeline,
)


# ---------------------------------------------------------------------------
# test_cqi_pipeline_structure
# ---------------------------------------------------------------------------

def test_cqi_pipeline_structure():
    dag = cqi_prediction_pipeline()
    assert len(dag) == 3
    assert len(dag.edges) == 2
    assert dag.is_tree() is True
    # 'collect' stage must have data_sovereignty_domain set
    collect = dag.get_stage("collect")
    assert collect.data_sovereignty_domain is not None
    assert collect.data_sovereignty_domain != ""


# ---------------------------------------------------------------------------
# test_anomaly_pipeline_structure
# ---------------------------------------------------------------------------

def test_anomaly_pipeline_structure():
    dag = anomaly_detection_pipeline()
    assert len(dag) == 3
    assert len(dag.edges) == 2
    assert dag.is_tree() is True
    # Verify the linear chain matches manuscript: collect -> feature_extract -> detect
    order = dag.topological_sort()
    assert order.index("collect") < order.index("feature_extract")
    assert order.index("feature_extract") < order.index("detect")


# ---------------------------------------------------------------------------
# test_sensor_fusion_structure
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n_sensors", [1, 3, 5])
def test_sensor_fusion_structure(n_sensors):
    dag = sensor_fusion_pipeline(n_sensors)
    assert len(dag) == n_sensors + 2
    assert len(dag.edges) == n_sensors + 1
    # All sensor stages feed 'fuse'; 'fuse' feeds 'decide'
    assert set(dag.predecessors("fuse")) == {f"sensor_{i}" for i in range(n_sensors)}
    assert dag.successors("fuse") == ["decide"]
    # All sensor fusion pipelines are DP-eligible: no fan-out (each stage
    # has at most one successor), and there is exactly one sink ('decide').
    # Fan-in at 'fuse' is safe for DP because predecessor subtrees are independent.
    assert dag.is_tree() is True


# ---------------------------------------------------------------------------
# test_map_pipeline
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n", [1, 4, 7])
def test_map_pipeline(n):
    dag = map_pipeline(stage_type="transform", n_stages=n)
    assert len(dag) == n
    assert len(dag.edges) == n - 1
    # Check linearity: each stage has at most one predecessor and one successor
    order = dag.topological_sort()
    assert len(order) == n
    for i in range(n - 1):
        assert dag.successors(order[i]) == [order[i + 1]]


# ---------------------------------------------------------------------------
# test_funnel_pipeline
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n_inputs", [1, 3, 6])
def test_funnel_pipeline(n_inputs):
    dag = funnel_pipeline(n_inputs=n_inputs)
    assert len(dag) == n_inputs + 2
    assert len(dag.edges) == n_inputs + 1
    # Aggregate has n_inputs predecessors; output has 1 predecessor
    assert len(dag.predecessors("aggregate")) == n_inputs
    assert dag.predecessors("output") == ["aggregate"]
    assert dag.successors("output") == []
    # Funnel is DP-eligible: no fan-out, one sink
    assert dag.is_tree() is True


# ---------------------------------------------------------------------------
# test_sensor_fusion_invalid
# ---------------------------------------------------------------------------

def test_sensor_fusion_invalid():
    with pytest.raises(ValueError):
        sensor_fusion_pipeline(n_sensors=0)
