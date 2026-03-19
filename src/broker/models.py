"""Shared Pydantic models and dataclasses for all broker implementations.

Defines the HTTP API contract (request/response models) and internal state
dataclasses used by NeuralBroker, StaticBroker, and KafkaBroker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pydantic import BaseModel

from src.pipeline.dag import PipelineDAG


# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------


class PublishRequest(BaseModel):
    """Body for POST /publish."""

    pipeline_type: str
    config: dict = {}


class PublishResponse(BaseModel):
    """Response for POST /publish."""

    pipeline_id: str
    placement: dict[str, str]
    status: str


class RegisterRequest(BaseModel):
    """Body for POST /register (sent by workers)."""

    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    url: str = ""  # Optional; workers may omit if they call from their own URL


class RegisterResponse(BaseModel):
    status: str
    node_id: str


class StageResultRequest(BaseModel):
    """Body for POST /result (sent by workers after stage completion)."""

    pipeline_id: str
    stage_id: str
    node_id: str
    start_time: float
    end_time: float
    processing_time_ms: float
    output_data: str = ""
    success: bool = True
    error: Optional[str] = None


class HealthResponse(BaseModel):
    broker_id: str
    domain_id: str
    workers: int
    active_pipelines: int
    status: str


# ---------------------------------------------------------------------------
# Internal state dataclasses
# ---------------------------------------------------------------------------


@dataclass
class WorkerInfo:
    """Registration record for a connected worker node.

    Attributes:
        node_id: Unique identifier (matches ExecutionUnit.node_id).
        domain_id: Data-sovereignty domain this worker belongs to.
        slice_id: Network slice this worker is part of.
        capacity: Maximum processing capacity (normalised, Eq. 1).
        current_load: Current consumed capacity on this worker.
        url: HTTP base URL for dispatching stage assignments to this worker.
    """

    node_id: str
    domain_id: str
    slice_id: str
    capacity: float
    url: str
    current_load: float = 0.0


@dataclass
class PipelineState:
    """Tracks an in-flight pipeline execution.

    Attributes:
        pipeline_id: Unique identifier for this pipeline instance.
        pipeline_type: Template name (e.g. "cqi_prediction").
        dag: The pipeline DAG for this instance.
        placement: Mapping from stage_id to the node_id it is assigned to.
        completed_stages: Set of stage_ids that have reported completion.
        failed: True if any stage has failed and the pipeline is aborted.
        error: Human-readable failure description if failed is True.
        all_stages: Cached set of all stage_ids (populated in __post_init__).
    """

    pipeline_id: str
    pipeline_type: str
    dag: PipelineDAG
    placement: dict[str, str]
    completed_stages: set[str] = field(default_factory=set)
    failed: bool = False
    error: Optional[str] = None
    all_stages: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self.all_stages = set(self.dag.stages.keys())
