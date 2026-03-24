"""Measurement harness for Neural Pub/Sub experiments.

Collects per-pipeline and aggregate metrics:
- End-to-end latency decomposition (network + compute per stage)
- Throughput (pipelines/sec completed)
- Routing accuracy (F1 score for subscription matching)
- Adaptation time (time to re-place after failure)
- Federation overhead (bandwidth used by summary propagation)

Timestamps are injected at each stage boundary for precise decomposition.
"""

from __future__ import annotations

import asyncio
import csv
import json
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_EVENTS = frozenset(
    {
        "created",
        "dispatched",
        "placement_complete",
        "stage_start",
        "stage_end",
        "stage_result_received",
        "delivered",
    }
)


# ---------------------------------------------------------------------------
# TimestampRecord
# ---------------------------------------------------------------------------


@dataclass
class TimestampRecord:
    """A single timestamped event at a stage boundary.

    Attributes:
        pipeline_id: Identifier of the pipeline this event belongs to.
        stage_id:    Identifier of the pipeline stage (e.g. "s0", "s1").
        event:       One of 'created', 'dispatched', 'stage_start',
                     'stage_end', 'delivered'.
        timestamp:   Wall-clock time from time.time().
        node_id:     The compute node where the event occurred (optional).
        metadata:    Arbitrary extra fields, e.g. {'data_size_bytes': 1024}.
    """

    pipeline_id: str
    stage_id: str
    event: str
    timestamp: float
    node_id: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.event not in VALID_EVENTS:
            raise ValueError(
                f"Invalid event '{self.event}'. Must be one of {sorted(VALID_EVENTS)}."
            )


# ---------------------------------------------------------------------------
# PipelineTrace
# ---------------------------------------------------------------------------


@dataclass
class PipelineTrace:
    """Accumulated trace for a single pipeline execution.

    Attributes:
        pipeline_id:   Identifier for this pipeline instance.
        pipeline_type: Logical type / template name of the pipeline.
        timestamps:    Ordered list of TimestampRecord objects.
        placement:     Map of stage_id to the node_id it ran on.
        success:       Whether the pipeline completed successfully.
        error:         Error message if success is False, else None.
    """

    pipeline_id: str
    pipeline_type: str
    timestamps: list[TimestampRecord] = field(default_factory=list)
    placement: dict[str, str] = field(default_factory=dict)
    success: bool = False
    error: Optional[str] = None
    partial: bool = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _records_for_event(self, event: str) -> list[TimestampRecord]:
        return [r for r in self.timestamps if r.event == event]

    def _first_ts(self, event: str) -> Optional[float]:
        records = self._records_for_event(event)
        return records[0].timestamp if records else None

    def _last_ts(self, event: str) -> Optional[float]:
        records = self._records_for_event(event)
        return records[-1].timestamp if records else None

    # ------------------------------------------------------------------
    # Public metrics
    # ------------------------------------------------------------------

    def end_to_end_latency_ms(self) -> Optional[float]:
        """Return total latency in ms from first 'created' to last 'delivered'.

        Returns None if either endpoint timestamp is missing.
        """
        # End-to-end latency spans the entire pipeline lifecycle: from the
        # moment the broker creates the pipeline request ('created') to the
        # moment the final stage result is delivered back ('delivered').
        # This includes queueing, network transfer, and compute at every stage.
        t_start = self._first_ts("created")
        t_end = self._last_ts("delivered")
        if t_start is None or t_end is None:
            return None
        return (t_end - t_start) * 1000.0

    def stage_latencies_ms(self) -> dict[str, float]:
        """Return per-stage compute latency in ms (stage_end - stage_start).

        Only stages with both a 'stage_start' and a 'stage_end' record are
        included. When multiple records exist for the same stage (retries),
        the first start and last end are used.
        """
        # Collect the first 'stage_start' and last 'stage_end' per stage.
        # Using setdefault for starts captures the first attempt; overwriting
        # ends captures the final completion (relevant when a stage is retried
        # after a dispatch failure).
        starts: dict[str, float] = {}
        ends: dict[str, float] = {}
        for r in self.timestamps:
            if r.event == "stage_start":
                starts.setdefault(r.stage_id, r.timestamp)
            elif r.event == "stage_end":
                ends[r.stage_id] = r.timestamp
        # Per-stage compute latency = stage_end - stage_start (in ms).
        # This isolates the time a worker spent executing the stage logic,
        # excluding network transfer and queueing delays.
        result: dict[str, float] = {}
        for stage_id in starts:
            if stage_id in ends:
                result[stage_id] = (ends[stage_id] - starts[stage_id]) * 1000.0
        return result

    def network_latencies_ms(self) -> dict[tuple[str, str], float]:
        """Return per-edge network latency in ms between consecutive stages.

        The network latency for edge (stage_i, stage_j) is defined as:
            stage_j.stage_start  -  stage_i.stage_end

        Stages are ordered by their first 'stage_start' timestamp. Only
        consecutive stage pairs where both endpoints exist are returned.
        """
        # Collect first stage_start per stage (for temporal ordering) and
        # the end/start timestamps needed for the inter-stage gap calculation.
        stage_first_start: dict[str, float] = {}
        stage_end_ts: dict[str, float] = {}
        stage_start_ts: dict[str, float] = {}

        for r in self.timestamps:
            if r.event == "stage_start":
                stage_first_start.setdefault(r.stage_id, r.timestamp)
                stage_start_ts.setdefault(r.stage_id, r.timestamp)
            elif r.event == "stage_end":
                stage_end_ts[r.stage_id] = r.timestamp

        # Sort stages by their first execution start time so that
        # consecutive pairs reflect the actual pipeline data flow.
        ordered = sorted(stage_first_start, key=lambda s: stage_first_start[s])

        # Network latency for edge (stage_i -> stage_j) is the gap between
        # stage_i finishing and stage_j starting.  This captures serialisation,
        # network transfer, and any queueing delay between the two workers.
        result: dict[tuple[str, str], float] = {}
        for i in range(len(ordered) - 1):
            src = ordered[i]
            dst = ordered[i + 1]
            if src in stage_end_ts and dst in stage_start_ts:
                latency = (stage_start_ts[dst] - stage_end_ts[src]) * 1000.0
                result[(src, dst)] = latency
        return result

    def domain_crossings(self, topology: Optional[dict[str, str]] = None) -> int:
        """Count stage-to-stage edges that cross domain boundaries.

        Args:
            topology: Optional map of node_id to domain_id. If None, uses the
                      'domain' key from TimestampRecord.metadata as a fallback,
                      or the placement dict combined with a flat domain lookup.

        Returns:
            Number of consecutive stage pairs assigned to different domains.
            Returns 0 if placement information is insufficient.
        """
        if not self.placement:
            return 0

        # Build node -> domain map from provided topology or metadata
        node_domain: dict[str, str] = {}
        if topology:
            node_domain.update(topology)
        else:
            for r in self.timestamps:
                if r.node_id and "domain" in r.metadata:
                    node_domain[r.node_id] = r.metadata["domain"]

        # Collect ordered stages (by first stage_start)
        stage_first_start: dict[str, float] = {}
        for r in self.timestamps:
            if r.event == "stage_start":
                stage_first_start.setdefault(r.stage_id, r.timestamp)

        ordered = sorted(stage_first_start, key=lambda s: stage_first_start[s])

        crossings = 0
        for i in range(len(ordered) - 1):
            src_stage = ordered[i]
            dst_stage = ordered[i + 1]
            src_node = self.placement.get(src_stage)
            dst_node = self.placement.get(dst_stage)
            if src_node is None or dst_node is None:
                continue
            src_domain = node_domain.get(src_node)
            dst_domain = node_domain.get(dst_node)
            if src_domain is not None and dst_domain is not None:
                if src_domain != dst_domain:
                    crossings += 1
        return crossings


# ---------------------------------------------------------------------------
# AggregateMetrics
# ---------------------------------------------------------------------------


@dataclass
class AggregateMetrics:
    """Aggregate statistics over all completed pipeline traces.

    Attributes:
        total_pipelines:           Total number of pipelines recorded.
        completed:                 Pipelines that finished successfully.
        failed:                    Pipelines that finished with an error.
        throughput_per_sec:        completed / wall_clock_duration.
        latency_mean_ms:           Mean end-to-end latency across completed runs.
        latency_p50_ms:            Median end-to-end latency.
        latency_p95_ms:            95th-percentile end-to-end latency.
        latency_p99_ms:            99th-percentile end-to-end latency.
        per_stage_latency_mean:    Mean stage compute latency per stage_id.
        federation_bandwidth_bytes: Total bytes used by summary propagation.
        adaptation_events:         Total number of failure/recovery pairs tracked.
        adaptation_time_mean_ms:   Mean time from failure to recovery.
        domain_crossings_mean:     Mean number of domain crossings per pipeline.
    """

    total_pipelines: int = 0
    completed: int = 0
    failed: int = 0
    throughput_per_sec: float = 0.0
    latency_mean_ms: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    per_stage_latency_mean: dict[str, float] = field(default_factory=dict)
    federation_bandwidth_bytes: int = 0
    adaptation_events: int = 0
    adaptation_time_mean_ms: float = 0.0
    domain_crossings_mean: float = 0.0
    governance_violations: int = 0
    routing_accuracy_f1: float = 0.0


# ---------------------------------------------------------------------------
# MetricsCollector
# ---------------------------------------------------------------------------


class MetricsCollector:
    """Thread-safe collector for pipeline traces and aggregate metrics.

    Uses an asyncio.Lock for coroutine-safe access. For multi-threaded
    (non-async) usage, wrap calls in asyncio.run() or use run_sync().

    Example usage (async)::

        collector = MetricsCollector()
        await collector.record(TimestampRecord(...))
        await collector.complete_pipeline("p1", success=True)
        metrics = await collector.compute_aggregate()
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # pipeline_id -> PipelineTrace
        self._traces: dict[str, PipelineTrace] = {}
        # pipeline_id -> pipeline_type (set on first record)
        self._pipeline_types: dict[str, str] = {}
        self._wall_start: Optional[float] = None
        self._wall_end: Optional[float] = None
        # Phase 2 additions
        self._governance_violations: list[dict] = []
        self._routing_accuracies: list[float] = []
        self._federation_monitor: Optional["FederationMonitor"] = None

    # ------------------------------------------------------------------
    # Phase 2: Governance, routing, and federation accessors
    # ------------------------------------------------------------------

    def set_federation_monitor(self, monitor: "FederationMonitor") -> None:
        """Attach a FederationMonitor for bandwidth tracking in CSV export."""
        self._federation_monitor = monitor

    def record_governance_violation(
        self, pipeline_id: str, stage_id: str, description: str
    ) -> None:
        """Record a governance constraint violation for post-hoc analysis."""
        self._governance_violations.append(
            {
                "pipeline_id": pipeline_id,
                "stage_id": stage_id,
                "description": description,
                "timestamp": time.time(),
            }
        )

    def record_routing_accuracy(self, pipeline_id: str, f1_score: float) -> None:
        """Record the F1 routing accuracy for a single pipeline dispatch."""
        self._routing_accuracies.append(f1_score)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    async def record(self, record: TimestampRecord) -> None:
        """Append a timestamped event record to the appropriate trace.

        Creates a new PipelineTrace if this is the first event for the
        given pipeline_id.
        """
        async with self._lock:
            if self._wall_start is None:
                self._wall_start = record.timestamp
            if record.pipeline_id not in self._traces:
                pipeline_type = record.metadata.get("pipeline_type", "unknown")
                self._traces[record.pipeline_id] = PipelineTrace(
                    pipeline_id=record.pipeline_id,
                    pipeline_type=pipeline_type,
                )
            trace = self._traces[record.pipeline_id]
            trace.timestamps.append(record)
            if record.node_id and record.stage_id:
                trace.placement[record.stage_id] = record.node_id

    async def complete_pipeline(
        self,
        pipeline_id: str,
        success: bool,
        error: Optional[str] = None,
        partial: bool = False,
    ) -> None:
        """Mark a pipeline as complete and record the wall-clock end time.

        Args:
            pipeline_id: The pipeline to mark.
            success:     True if it completed without error.
            error:       Optional error message for failed pipelines.
            partial:     True if the pipeline completed with partial inputs
                         (funnel proceed mode).
        """
        async with self._lock:
            self._wall_end = time.time()
            if pipeline_id not in self._traces:
                # Create a minimal trace so the completion is recorded
                self._traces[pipeline_id] = PipelineTrace(
                    pipeline_id=pipeline_id,
                    pipeline_type="unknown",
                )
            trace = self._traces[pipeline_id]
            trace.success = success
            trace.error = error
            trace.partial = partial

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    async def get_trace(self, pipeline_id: str) -> Optional[PipelineTrace]:
        """Return the trace for a given pipeline, or None if not found."""
        async with self._lock:
            return self._traces.get(pipeline_id)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    async def compute_aggregate(self) -> AggregateMetrics:
        """Compute aggregate statistics over all completed pipeline traces.

        Returns an AggregateMetrics dataclass populated with latency
        percentiles (via numpy), throughput, and any federation / adaptation
        data accumulated via companion monitors.
        """
        # Snapshot all traces under lock, then release the lock for the
        # (potentially expensive) numpy computations.
        async with self._lock:
            traces = list(self._traces.values())

        # Partition traces into completed (success=True) and failed.
        total = len(traces)
        completed_traces = [t for t in traces if t.success]
        failed_traces = [t for t in traces if not t.success]
        completed = len(completed_traces)
        failed = len(failed_traces)

        # --- Throughput ---
        # Defined as completed pipelines divided by the wall-clock duration
        # of the entire experiment (first record to last completion).
        wall_start = self._wall_start
        wall_end = self._wall_end
        if wall_start and wall_end and (wall_end - wall_start) > 0:
            throughput = completed / (wall_end - wall_start)
        else:
            throughput = 0.0

        # --- Latency distribution (percentiles via numpy) ---
        # Collect end-to-end latencies from all completed traces, then
        # compute mean, p50 (median), p95, and p99.  These percentiles
        # characterise the tail-latency behaviour of the placement and
        # dispatch pipeline (reported in Table 2 of the paper).
        latencies = [
            lat
            for t in completed_traces
            for lat in [t.end_to_end_latency_ms()]
            if lat is not None
        ]
        if latencies:
            arr = np.array(latencies, dtype=float)
            lat_mean = float(np.mean(arr))
            lat_p50 = float(np.percentile(arr, 50))
            lat_p95 = float(np.percentile(arr, 95))
            lat_p99 = float(np.percentile(arr, 99))
        else:
            lat_mean = lat_p50 = lat_p95 = lat_p99 = 0.0

        # --- Per-stage mean latency breakdown ---
        # For each stage_id that appears across completed traces, accumulate
        # all its compute-latency samples and take the mean.  This enables a
        # per-stage bottleneck analysis in the results section.
        stage_latency_accum: dict[str, list[float]] = {}
        for t in completed_traces:
            for stage_id, lat_ms in t.stage_latencies_ms().items():
                stage_latency_accum.setdefault(stage_id, []).append(lat_ms)
        per_stage_mean = {
            sid: float(np.mean(vals))
            for sid, vals in stage_latency_accum.items()
        }

        # --- Domain crossings ---
        # Mean number of sovereignty-domain boundaries crossed per pipeline.
        crossings = [t.domain_crossings() for t in completed_traces]
        crossings_mean = float(np.mean(crossings)) if crossings else 0.0

        # --- Governance violations ---
        gov_violations = len(self._governance_violations)

        # --- Routing accuracy (mean F1) ---
        routing_f1 = (
            float(np.mean(self._routing_accuracies))
            if self._routing_accuracies
            else 0.0
        )

        # --- Federation bandwidth ---
        fed_bytes = (
            self._federation_monitor.total_bytes()
            if self._federation_monitor is not None
            else 0
        )

        return AggregateMetrics(
            total_pipelines=total,
            completed=completed,
            failed=failed,
            throughput_per_sec=throughput,
            latency_mean_ms=lat_mean,
            latency_p50_ms=lat_p50,
            latency_p95_ms=lat_p95,
            latency_p99_ms=lat_p99,
            per_stage_latency_mean=per_stage_mean,
            domain_crossings_mean=crossings_mean,
            governance_violations=gov_violations,
            routing_accuracy_f1=routing_f1,
            federation_bandwidth_bytes=fed_bytes,
        )

    # ------------------------------------------------------------------
    # Reset / export
    # ------------------------------------------------------------------

    async def reset(self) -> None:
        """Clear all recorded traces and reset wall-clock timers."""
        async with self._lock:
            self._traces.clear()
            self._wall_start = None
            self._wall_end = None

    async def export_csv(self, path: str) -> None:
        """Write per-pipeline summary metrics to a CSV file.

        Each row contains: pipeline_id, pipeline_type, success, error,
        end_to_end_latency_ms, aggregate metrics, and one column per
        unique stage_id latency.
        """
        async with self._lock:
            traces = list(self._traces.values())

        # Compute aggregate once for summary-level columns
        agg = await self.compute_aggregate()

        # Build the union of all stage_ids seen across every trace so that
        # the CSV has a consistent set of columns regardless of which stages
        # each individual pipeline executed.
        all_stage_ids: list[str] = sorted(
            {
                sid
                for t in traces
                for sid in t.stage_latencies_ms()
            }
        )

        # CSV column layout (expanded for Phase 2)
        fieldnames = (
            [
                "pipeline_id",
                "pipeline_type",
                "success",
                "partial",
                "error",
                "e2e_latency_ms",
                "throughput_pps",
                "completion_rate",
                "governance_violations",
                "federation_bytes_sent",
                "routing_accuracy_f1",
            ]
            + [f"stage_{sid}_ms" for sid in all_stage_ids]
        )

        # Compute completion rate
        total = agg.total_pipelines
        completion_rate = agg.completed / total if total > 0 else 0.0

        # Federation bytes
        fed_bytes = (
            self._federation_monitor.total_bytes()
            if self._federation_monitor is not None
            else 0
        )

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for t in traces:
                stage_lats = t.stage_latencies_ms()
                row: dict = {
                    "pipeline_id": t.pipeline_id,
                    "pipeline_type": t.pipeline_type,
                    "success": t.success,
                    "partial": t.partial,
                    "error": t.error or "",
                    "e2e_latency_ms": t.end_to_end_latency_ms() or "",
                    "throughput_pps": f"{agg.throughput_per_sec:.4f}",
                    "completion_rate": f"{completion_rate:.4f}",
                    "governance_violations": len(self._governance_violations),
                    "federation_bytes_sent": fed_bytes,
                    "routing_accuracy_f1": (
                        f"{agg.routing_accuracy_f1:.4f}"
                        if self._routing_accuracies
                        else ""
                    ),
                }
                for sid in all_stage_ids:
                    row[f"stage_{sid}_ms"] = stage_lats.get(sid, "")
                writer.writerow(row)

    async def export_json(self, path: str) -> None:
        """Write full trace data (all TimestampRecords) to a JSON file."""
        async with self._lock:
            traces = list(self._traces.values())

        data = []
        for t in traces:
            data.append(
                {
                    "pipeline_id": t.pipeline_id,
                    "pipeline_type": t.pipeline_type,
                    "success": t.success,
                    "error": t.error,
                    "placement": t.placement,
                    "timestamps": [
                        {
                            "pipeline_id": r.pipeline_id,
                            "stage_id": r.stage_id,
                            "event": r.event,
                            "timestamp": r.timestamp,
                            "node_id": r.node_id,
                            "metadata": r.metadata,
                        }
                        for r in t.timestamps
                    ],
                }
            )

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# FederationMonitor
# ---------------------------------------------------------------------------


class FederationMonitor:
    """Tracks bandwidth consumed by federation summary propagation.

    Records bytes sent and received between domain pairs for post-hoc
    analysis of federation overhead (Section 5 of the paper).

    Example usage::

        monitor = FederationMonitor()
        monitor.record_summary_sent(512, "domain-A", "domain-B")
        print(monitor.total_bytes())  # 512
    """

    def __init__(self) -> None:
        # (from_domain, to_domain) -> total bytes
        self._sent: dict[tuple[str, str], int] = {}
        self._received: dict[tuple[str, str], int] = {}

    def record_summary_sent(
        self, size_bytes: int, from_domain: str, to_domain: str
    ) -> None:
        """Record bytes sent from from_domain to to_domain.

        Args:
            size_bytes:  Number of bytes in the summary message.
            from_domain: Originating domain identifier.
            to_domain:   Destination domain identifier.
        """
        key = (from_domain, to_domain)
        self._sent[key] = self._sent.get(key, 0) + size_bytes

    def record_summary_received(
        self, size_bytes: int, from_domain: str, to_domain: str
    ) -> None:
        """Record bytes received at to_domain from from_domain.

        Args:
            size_bytes:  Number of bytes in the summary message.
            from_domain: Originating domain identifier.
            to_domain:   Receiving domain identifier.
        """
        key = (from_domain, to_domain)
        self._received[key] = self._received.get(key, 0) + size_bytes

    def total_bytes(self) -> int:
        """Return the combined total of all bytes sent and received."""
        return sum(self._sent.values()) + sum(self._received.values())

    def bytes_by_domain_pair(self) -> dict[tuple[str, str], int]:
        """Return total bytes (sent + received) keyed by (from, to) domain pair.

        Pairs that appear only in sent or only in received are still included.
        """
        all_pairs = set(self._sent) | set(self._received)
        return {
            pair: self._sent.get(pair, 0) + self._received.get(pair, 0)
            for pair in all_pairs
        }


# ---------------------------------------------------------------------------
# FailureEvent + AdaptationTracker
# ---------------------------------------------------------------------------


@dataclass
class FailureEvent:
    """Records a single failure or recovery event for adaptation tracking.

    Attributes:
        failure_type: Category of failure (e.g. 'node_crash', 'link_drop').
        target_id:    Identifier of the failed/recovered resource.
        timestamp:    Wall-clock time of the event.
        is_recovery:  True if this is a recovery event, False if failure.
        is_detection_complete: True if this marks the end of the detection
            phase (before re-placement begins).
    """

    failure_type: str
    target_id: str
    timestamp: float
    is_recovery: bool = False
    is_detection_complete: bool = False


class AdaptationTracker:
    """Measures adaptation time from failure detection to successful recovery.

    A failure event and a subsequent matching recovery event (same
    failure_type and target_id) form a pair. The adaptation time is the
    elapsed time between them.

    Example usage::

        tracker = AdaptationTracker()
        tracker.record_failure("node_crash", "node-3", time.time())
        # ... some time passes ...
        tracker.record_recovery("node_crash", "node-3", time.time())
        print(tracker.adaptation_times_ms())
    """

    def __init__(self) -> None:
        self._events: list[FailureEvent] = []

    def record_failure(
        self, failure_type: str, target_id: str, timestamp: float
    ) -> None:
        """Record a failure detection event.

        Args:
            failure_type: Category label for the failure.
            target_id:    Identifier of the affected resource.
            timestamp:    Wall-clock time of detection.
        """
        self._events.append(
            FailureEvent(
                failure_type=failure_type,
                target_id=target_id,
                timestamp=timestamp,
                is_recovery=False,
            )
        )

    def record_detection_complete(
        self, failure_type: str, target_id: str, timestamp: float
    ) -> None:
        """Record the moment failure detection completes (before re-placement).

        This separates the detection phase from the re-placement phase,
        enabling independent analysis of each.

        Args:
            failure_type: Must match the failure_type of the paired failure.
            target_id:    Must match the target_id of the paired failure.
            timestamp:    Wall-clock time when detection concluded.
        """
        self._events.append(
            FailureEvent(
                failure_type=failure_type,
                target_id=target_id,
                timestamp=timestamp,
                is_detection_complete=True,
            )
        )

    def record_recovery(
        self, failure_type: str, target_id: str, timestamp: float
    ) -> None:
        """Record a recovery event corresponding to a prior failure.

        Args:
            failure_type: Must match the failure_type of the paired failure.
            target_id:    Must match the target_id of the paired failure.
            timestamp:    Wall-clock time of recovery confirmation.
        """
        self._events.append(
            FailureEvent(
                failure_type=failure_type,
                target_id=target_id,
                timestamp=timestamp,
                is_recovery=True,
            )
        )

    def adaptation_times_ms(self) -> list[float]:
        """Return a list of adaptation times in ms for all matched pairs.

        Pairs are matched greedily in chronological order: for each failure
        event, the earliest subsequent recovery with the same (failure_type,
        target_id) is used. Unmatched failures (no recovery yet) are skipped.

        Returns:
            List of adaptation times in milliseconds (may be empty).
        """
        sorted_events = sorted(self._events, key=lambda e: e.timestamp)

        pending: dict[tuple[str, str], list[float]] = {}
        times: list[float] = []

        for event in sorted_events:
            key = (event.failure_type, event.target_id)
            if not event.is_recovery:
                pending.setdefault(key, []).append(event.timestamp)
            else:
                if pending.get(key):
                    failure_ts = pending[key].pop(0)
                    times.append((event.timestamp - failure_ts) * 1000.0)

        return times

    def detection_times_ms(self) -> list[float]:
        """Return detection times in ms (failure -> detection_complete) for matched pairs.

        Returns:
            List of detection durations in milliseconds (may be empty).
        """
        sorted_events = sorted(self._events, key=lambda e: e.timestamp)
        pending: dict[tuple[str, str], list[float]] = {}
        times: list[float] = []

        for event in sorted_events:
            key = (event.failure_type, event.target_id)
            if not event.is_recovery and not event.is_detection_complete:
                pending.setdefault(key, []).append(event.timestamp)
            elif event.is_detection_complete:
                if pending.get(key):
                    failure_ts = pending[key][0]  # peek, don't pop yet
                    times.append((event.timestamp - failure_ts) * 1000.0)

        return times

    def replacement_times_ms(self) -> list[float]:
        """Return re-placement times in ms (detection_complete -> recovery) for matched pairs.

        Returns:
            List of replacement durations in milliseconds (may be empty).
        """
        sorted_events = sorted(self._events, key=lambda e: e.timestamp)
        pending_detect: dict[tuple[str, str], list[float]] = {}
        times: list[float] = []

        for event in sorted_events:
            key = (event.failure_type, event.target_id)
            if event.is_detection_complete:
                pending_detect.setdefault(key, []).append(event.timestamp)
            elif event.is_recovery:
                if pending_detect.get(key):
                    detect_ts = pending_detect[key].pop(0)
                    times.append((event.timestamp - detect_ts) * 1000.0)

        return times

    def all_events(self) -> list[FailureEvent]:
        """Return all recorded failure and recovery events in insertion order."""
        return list(self._events)
