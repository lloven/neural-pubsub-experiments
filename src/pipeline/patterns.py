"""Factory functions for common Neural Pub/Sub pipeline patterns.

Provides ready-made PipelineDAG instances for the map and funnel topologies
described in paper Section 4.1, as well as concrete application pipelines used
in the evaluation (Section 6):

* **Map pipeline**: linear chain of identical stages (e.g. per-sample transforms).
* **Funnel pipeline**: N parallel input stages converging into an aggregation
  stage followed by a single output stage.
* **CQI prediction**: 3-stage chain for Channel Quality Indicator prediction
  with data-sovereignty constraints on raw radio measurements.
* **Anomaly detection**: 3-stage chain for streaming anomaly detection.
* **Sensor fusion**: N-sensor funnel into fusion and decision stages.
"""

from __future__ import annotations

from typing import Optional

from src.pipeline.dag import Edge, PipelineDAG, Stage


def map_pipeline(
    stage_type: str,
    n_stages: int,
    computational_demand: float = 0.5,
    output_data_rate: float = 5.0,
    latency_bound: float = 10.0,
    slice_requirement: Optional[str] = None,
) -> PipelineDAG:
    """Create a linear chain (map-map-...-map) pipeline.

    Each stage has the same type and resource profile. Edges connect
    consecutive stages with identical latency bounds.

    Args:
        stage_type: Semantic type assigned to every stage (e.g. "transform").
        n_stages: Number of stages in the chain. Must be >= 1.
        computational_demand: rho_v for each stage (Eq. 1).
        output_data_rate: omega_v for each stage.
        latency_bound: L_{v,v'} for each edge (Eq. 2).
        slice_requirement: Optional slice constraint applied to all stages.

    Returns:
        A PipelineDAG with ``n_stages`` stages connected in sequence.

    Raises:
        ValueError: If ``n_stages`` < 1.
    """
    if n_stages < 1:
        raise ValueError("n_stages must be >= 1.")

    dag = PipelineDAG()
    stage_ids: list[str] = []

    for i in range(n_stages):
        sid = f"map_{i}"
        dag.add_stage(
            Stage(
                id=sid,
                stage_type=stage_type,
                computational_demand=computational_demand,
                output_data_rate=output_data_rate,
                slice_requirement=slice_requirement,
            )
        )
        stage_ids.append(sid)

    for i in range(len(stage_ids) - 1):
        dag.add_edge(
            Edge(
                source_id=stage_ids[i],
                target_id=stage_ids[i + 1],
                latency_bound=latency_bound,
            )
        )

    return dag


def funnel_pipeline(
    n_inputs: int,
    input_type: str = "ingest",
    input_demand: float = 0.3,
    input_data_rate: float = 8.0,
    agg_demand: float = 0.6,
    agg_data_rate: float = 2.0,
    output_demand: float = 0.2,
    output_data_rate: float = 1.0,
    latency_bound_in: float = 10.0,
    latency_bound_out: float = 5.0,
    slice_requirement: Optional[str] = None,
) -> PipelineDAG:
    """Create a funnel pipeline: N inputs -> 1 aggregation -> 1 output.

    This is the canonical fan-in pattern where multiple data streams are
    merged before a final processing step. The resulting DAG is a tree
    (each node has at most one predecessor) when viewed from aggregation
    toward inputs, enabling DP placement.

    Args:
        n_inputs: Number of parallel input stages. Must be >= 1.
        input_type: Semantic type for input stages.
        input_demand: rho_v for each input stage.
        input_data_rate: omega_v for each input stage.
        agg_demand: rho_v for the aggregation stage.
        agg_data_rate: omega_v for the aggregation stage.
        output_demand: rho_v for the output stage.
        output_data_rate: omega_v for the output stage.
        latency_bound_in: L_{v,v'} from each input to aggregation (Eq. 2).
        latency_bound_out: L_{v,v'} from aggregation to output (Eq. 2).
        slice_requirement: Optional slice constraint applied to all stages.

    Returns:
        A PipelineDAG with ``n_inputs + 2`` stages.

    Raises:
        ValueError: If ``n_inputs`` < 1.
    """
    if n_inputs < 1:
        raise ValueError("n_inputs must be >= 1.")

    dag = PipelineDAG()

    # Input stages
    for i in range(n_inputs):
        dag.add_stage(
            Stage(
                id=f"input_{i}",
                stage_type=input_type,
                computational_demand=input_demand,
                output_data_rate=input_data_rate,
                slice_requirement=slice_requirement,
            )
        )

    # Aggregation stage
    dag.add_stage(
        Stage(
            id="aggregate",
            stage_type="aggregate",
            computational_demand=agg_demand,
            output_data_rate=agg_data_rate,
            slice_requirement=slice_requirement,
        )
    )

    # Output stage
    dag.add_stage(
        Stage(
            id="output",
            stage_type="output",
            computational_demand=output_demand,
            output_data_rate=output_data_rate,
            slice_requirement=slice_requirement,
        )
    )

    # Edges: inputs -> aggregate -> output
    for i in range(n_inputs):
        dag.add_edge(
            Edge(
                source_id=f"input_{i}",
                target_id="aggregate",
                latency_bound=latency_bound_in,
            )
        )
    dag.add_edge(
        Edge(
            source_id="aggregate",
            target_id="output",
            latency_bound=latency_bound_out,
        )
    )

    return dag


def cqi_prediction_pipeline() -> PipelineDAG:
    """Create a 3-stage CQI (Channel Quality Indicator) prediction pipeline.

    Pipeline topology::

        collect -> feature_extract -> predict

    The ``collect`` stage has ``data_sovereignty_domain`` set to ``"radio_local"``
    because raw radio measurement data must remain within the local domain
    (data sovereignty constraint, Eq. 5). Downstream stages may be placed
    anywhere that satisfies latency and capacity constraints.

    This pipeline is tree-structured, enabling optimal DP placement.

    Stage parameters are representative of a lightweight edge-inference
    workload:

    * **collect**: low compute, high output rate (raw IQ samples / CSI).
    * **feature_extract**: moderate compute, moderate output rate.
    * **predict**: higher compute (ML inference), low output rate (CQI value).

    Returns:
        A PipelineDAG with 3 stages and URLLC slice requirement.
    """
    dag = PipelineDAG()

    dag.add_stage(
        Stage(
            id="collect",
            stage_type="collect",
            computational_demand=0.1,
            output_data_rate=50.0,  # raw radio data: high rate
            slice_requirement="URLLC",
            data_sovereignty_domain="__local__",
        )
    )
    dag.add_stage(
        Stage(
            id="feature_extract",
            stage_type="feature_extract",
            computational_demand=0.4,
            output_data_rate=5.0,
            slice_requirement="URLLC",
        )
    )
    dag.add_stage(
        Stage(
            id="predict",
            stage_type="predict",
            computational_demand=0.7,
            output_data_rate=0.1,  # single CQI value per TTI
            slice_requirement="URLLC",
        )
    )

    dag.add_edge(Edge("collect", "feature_extract", latency_bound=2.0))
    dag.add_edge(Edge("feature_extract", "predict", latency_bound=3.0))

    return dag


def anomaly_detection_pipeline() -> PipelineDAG:
    """Create a 3-stage streaming anomaly detection pipeline.

    Pipeline topology::

        collect -> feature_extract -> detect

    Mirrors the manuscript's anomaly detection use case (Section 6):
    a data collection stage feeds feature extraction, which feeds the
    anomaly detection model. All stages use the eMBB slice.

    Returns:
        A PipelineDAG with 3 stages.
    """
    dag = PipelineDAG()

    stages = [
        Stage("collect", "collect", computational_demand=0.1, output_data_rate=20.0, slice_requirement="eMBB"),
        Stage("feature_extract", "feature_extract", computational_demand=0.3, output_data_rate=10.0, slice_requirement="eMBB"),
        Stage("detect", "detect", computational_demand=0.8, output_data_rate=1.0, slice_requirement="eMBB"),
    ]

    for s in stages:
        dag.add_stage(s)

    edges = [
        Edge("collect", "feature_extract", latency_bound=5.0),
        Edge("feature_extract", "detect", latency_bound=10.0),
    ]

    for e in edges:
        dag.add_edge(e)

    return dag


def sensor_fusion_pipeline(n_sensors: int) -> PipelineDAG:
    """Create an (N+2)-stage sensor fusion funnel pipeline.

    Pipeline topology::

        sensor_0 ─┐
        sensor_1 ─┤
           ...    ├─> fuse -> decide
        sensor_N ─┘

    Each sensor stage represents a data source (e.g. accelerometer, gyroscope,
    camera feed). The ``fuse`` stage aggregates all sensor streams, and the
    ``decide`` stage runs a classification or decision model.

    This is a tree DAG (each node has at most one predecessor when viewed
    from fuse toward sensors), enabling optimal DP placement.

    Args:
        n_sensors: Number of sensor input stages. Must be >= 1.

    Returns:
        A PipelineDAG with ``n_sensors + 2`` stages.

    Raises:
        ValueError: If ``n_sensors`` < 1.
    """
    if n_sensors < 1:
        raise ValueError("n_sensors must be >= 1.")

    dag = PipelineDAG()

    # Sensor stages
    for i in range(n_sensors):
        dag.add_stage(
            Stage(
                id=f"sensor_{i}",
                stage_type="sensor",
                computational_demand=0.15,
                output_data_rate=12.0,
            )
        )

    # Fusion stage
    dag.add_stage(
        Stage(
            id="fuse",
            stage_type="fuse",
            computational_demand=0.5 + 0.05 * n_sensors,  # scales with inputs
            output_data_rate=3.0,
        )
    )

    # Decision stage
    dag.add_stage(
        Stage(
            id="decide",
            stage_type="decide",
            computational_demand=0.6,
            output_data_rate=0.5,
        )
    )

    # Edges
    for i in range(n_sensors):
        dag.add_edge(
            Edge(f"sensor_{i}", "fuse", latency_bound=8.0)
        )
    dag.add_edge(Edge("fuse", "decide", latency_bound=5.0))

    return dag
