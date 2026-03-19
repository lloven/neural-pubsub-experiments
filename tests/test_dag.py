"""Unit tests for PipelineDAG (src/pipeline/dag.py)."""

import pytest

from src.pipeline.dag import Edge, PipelineDAG, Stage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stage(sid: str, demand: float = 0.3, rate: float = 5.0) -> Stage:
    return Stage(id=sid, stage_type="test", computational_demand=demand, output_data_rate=rate)


def _edge(src: str, tgt: str, bound: float = 10.0) -> Edge:
    return Edge(source_id=src, target_id=tgt, latency_bound=bound)


# ---------------------------------------------------------------------------
# test_add_stage
# ---------------------------------------------------------------------------

def test_add_stage():
    dag = PipelineDAG()
    dag.add_stage(_stage("s1"))
    dag.add_stage(_stage("s2"))
    dag.add_stage(_stage("s3"))
    assert len(dag) == 3
    assert "s1" in dag
    assert "s2" in dag
    assert "s3" in dag
    assert dag.get_stage("s1").id == "s1"


# ---------------------------------------------------------------------------
# test_add_edge
# ---------------------------------------------------------------------------

def test_add_edge():
    dag = PipelineDAG()
    dag.add_stage(_stage("a"))
    dag.add_stage(_stage("b"))
    dag.add_edge(_edge("a", "b"))
    assert dag.predecessors("b") == ["a"]
    assert dag.successors("a") == ["b"]
    assert dag.predecessors("a") == []
    assert dag.successors("b") == []
    assert len(dag.edges) == 1


# ---------------------------------------------------------------------------
# test_duplicate_stage_raises
# ---------------------------------------------------------------------------

def test_duplicate_stage_raises():
    """Adding a stage with an already-existing ID raises ValueError."""
    dag = PipelineDAG()
    dag.add_stage(_stage("dup"))
    with pytest.raises(ValueError, match="dup"):
        dag.add_stage(_stage("dup"))


# ---------------------------------------------------------------------------
# test_cycle_detection
# ---------------------------------------------------------------------------

def test_cycle_detection():
    """Adding an edge that would close a cycle (z->x in x->y->z) raises ValueError."""
    dag = PipelineDAG()
    dag.add_stage(_stage("x"))
    dag.add_stage(_stage("y"))
    dag.add_stage(_stage("z"))
    dag.add_edge(_edge("x", "y"))
    dag.add_edge(_edge("y", "z"))
    # Adding z -> x would create a cycle
    with pytest.raises(ValueError, match="cycle"):
        dag.add_edge(_edge("z", "x"))


# ---------------------------------------------------------------------------
# test_topological_sort
# ---------------------------------------------------------------------------

def test_topological_sort():
    """Topological order of a linear chain s1->s2->s3->s4 preserves predecessor ordering."""
    dag = PipelineDAG()
    for sid in ["s1", "s2", "s3", "s4"]:
        dag.add_stage(_stage(sid))
    dag.add_edge(_edge("s1", "s2"))
    dag.add_edge(_edge("s2", "s3"))
    dag.add_edge(_edge("s3", "s4"))

    order = dag.topological_sort()
    assert len(order) == 4
    # Each stage must appear before its successors
    assert order.index("s1") < order.index("s2")
    assert order.index("s2") < order.index("s3")
    assert order.index("s3") < order.index("s4")


# ---------------------------------------------------------------------------
# test_sources_and_sinks
# ---------------------------------------------------------------------------

def test_sources_and_sinks():
    """Funnel DAG (two inputs -> merge -> output) has two sources and one sink."""
    dag = PipelineDAG()
    for sid in ["in0", "in1", "merge", "out"]:
        dag.add_stage(_stage(sid))
    dag.add_edge(_edge("in0", "merge"))
    dag.add_edge(_edge("in1", "merge"))
    dag.add_edge(_edge("merge", "out"))

    sources = dag.sources()
    sinks = dag.sinks()
    assert set(sources) == {"in0", "in1"}
    assert sinks == ["out"]


# ---------------------------------------------------------------------------
# test_is_tree_true
# ---------------------------------------------------------------------------

def test_is_tree_linear_chain():
    """Linear chain (no fan-out, one sink) is classified as a tree (DP-eligible)."""
    dag = PipelineDAG()
    for sid in ["a", "b", "c", "d"]:
        dag.add_stage(_stage(sid))
    dag.add_edge(_edge("a", "b"))
    dag.add_edge(_edge("b", "c"))
    dag.add_edge(_edge("c", "d"))
    assert dag.is_tree() is True


def test_is_tree_funnel():
    """Fan-in (funnel) with one sink is still a tree; predecessor subtrees are independent."""
    dag = PipelineDAG()
    for sid in ["in0", "in1", "merge", "out"]:
        dag.add_stage(_stage(sid))
    dag.add_edge(_edge("in0", "merge"))
    dag.add_edge(_edge("in1", "merge"))
    dag.add_edge(_edge("merge", "out"))
    assert dag.is_tree() is True


def test_is_tree_false_fanout():
    """Fan-out (root -> left, root -> right) is not a tree; root placement couples both branches."""
    dag = PipelineDAG()
    for sid in ["root", "left", "right"]:
        dag.add_stage(_stage(sid))
    dag.add_edge(_edge("root", "left"))
    dag.add_edge(_edge("root", "right"))
    assert dag.is_tree() is False  # fan-out + two sinks


# ---------------------------------------------------------------------------
# test_is_tree_false_diamond
# ---------------------------------------------------------------------------

def test_is_tree_false_diamond():
    """Diamond DAG (root fans out then merges) is not a tree due to fan-out at root."""
    dag = PipelineDAG()
    for sid in ["root", "left", "right", "sink"]:
        dag.add_stage(_stage(sid))
    dag.add_edge(_edge("root", "left"))
    dag.add_edge(_edge("root", "right"))
    dag.add_edge(_edge("left", "sink"))
    dag.add_edge(_edge("right", "sink"))
    assert dag.is_tree() is False
