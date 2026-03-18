"""Unit tests for the measurement harness (src/measurement/harness.py)."""

import asyncio
import time

import pytest

from src.measurement.harness import (
    AdaptationTracker,
    AggregateMetrics,
    FederationMonitor,
    MetricsCollector,
    PipelineTrace,
    TimestampRecord,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(pipeline_id: str, stage_id: str, event: str, t: float, node_id=None) -> TimestampRecord:
    return TimestampRecord(
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        event=event,
        timestamp=t,
        node_id=node_id,
    )


def _make_trace_with_timestamps(
    pipeline_id: str = "p0",
    t_created: float = 0.0,
    t_stage_start_s0: float = 0.005,
    t_stage_end_s0: float = 0.010,
    t_stage_start_s1: float = 0.015,
    t_stage_end_s1: float = 0.025,
    t_delivered: float = 0.030,
) -> PipelineTrace:
    trace = PipelineTrace(pipeline_id=pipeline_id, pipeline_type="test")
    trace.timestamps = [
        _ts(pipeline_id, "s0", "created", t_created),
        _ts(pipeline_id, "s0", "stage_start", t_stage_start_s0),
        _ts(pipeline_id, "s0", "stage_end", t_stage_end_s0),
        _ts(pipeline_id, "s1", "stage_start", t_stage_start_s1),
        _ts(pipeline_id, "s1", "stage_end", t_stage_end_s1),
        _ts(pipeline_id, "s1", "delivered", t_delivered),
    ]
    return trace


# ---------------------------------------------------------------------------
# test_pipeline_trace_latency
# ---------------------------------------------------------------------------

def test_pipeline_trace_latency():
    # created at t=0, delivered at t=0.030 -> 30 ms
    trace = _make_trace_with_timestamps(t_created=0.0, t_delivered=0.030)
    trace.success = True
    latency = trace.end_to_end_latency_ms()
    assert latency is not None
    assert abs(latency - 30.0) < 1e-6


def test_pipeline_trace_latency_missing_delivered():
    trace = PipelineTrace(pipeline_id="p1", pipeline_type="test")
    trace.timestamps = [
        _ts("p1", "s0", "created", 0.0),
        _ts("p1", "s0", "stage_start", 0.005),
        # No 'delivered' event
    ]
    assert trace.end_to_end_latency_ms() is None


# ---------------------------------------------------------------------------
# test_stage_latencies
# ---------------------------------------------------------------------------

def test_stage_latencies():
    # s0 runs from 0.005 to 0.010 -> 5 ms
    # s1 runs from 0.015 to 0.025 -> 10 ms
    trace = _make_trace_with_timestamps(
        t_stage_start_s0=0.005,
        t_stage_end_s0=0.010,
        t_stage_start_s1=0.015,
        t_stage_end_s1=0.025,
    )
    lats = trace.stage_latencies_ms()
    assert abs(lats["s0"] - 5.0) < 1e-6
    assert abs(lats["s1"] - 10.0) < 1e-6


# ---------------------------------------------------------------------------
# test_aggregate_metrics
# ---------------------------------------------------------------------------

def test_aggregate_metrics():
    collector = MetricsCollector()

    # Create 3 completed pipelines with known latencies: 10 ms, 20 ms, 30 ms
    async def _run():
        for i, latency_s in enumerate([0.010, 0.020, 0.030]):
            pid = f"p{i}"
            await collector.record(_ts(pid, "s0", "created", 0.0))
            await collector.record(_ts(pid, "s0", "stage_start", 0.001))
            await collector.record(_ts(pid, "s0", "stage_end", 0.002))
            await collector.record(_ts(pid, "s0", "delivered", latency_s))
            await collector.complete_pipeline(pid, success=True)
        return await collector.compute_aggregate()

    metrics = asyncio.run(_run())

    assert metrics.total_pipelines == 3
    assert metrics.completed == 3
    assert metrics.failed == 0

    # Latencies are 10, 20, 30 ms
    assert abs(metrics.latency_mean_ms - 20.0) < 1e-3
    assert abs(metrics.latency_p50_ms - 20.0) < 1.0   # median
    assert abs(metrics.latency_p95_ms - 30.0) < 2.0   # 95th percentile of [10, 20, 30]


# ---------------------------------------------------------------------------
# test_federation_monitor
# ---------------------------------------------------------------------------

def test_federation_monitor():
    monitor = FederationMonitor()
    monitor.record_summary_sent(512, "domain-A", "domain-B")
    monitor.record_summary_sent(256, "domain-A", "domain-B")
    monitor.record_summary_received(128, "domain-B", "domain-A")

    total = monitor.total_bytes()
    assert total == 512 + 256 + 128

    by_pair = monitor.bytes_by_domain_pair()
    # Sent pair: (domain-A, domain-B) = 768 bytes
    assert by_pair[("domain-A", "domain-B")] == 512 + 256
    # Received pair: (domain-B, domain-A) = 128 bytes
    assert by_pair[("domain-B", "domain-A")] == 128


# ---------------------------------------------------------------------------
# test_adaptation_tracker
# ---------------------------------------------------------------------------

def test_adaptation_tracker():
    tracker = AdaptationTracker()

    t_fail = 1000.0
    t_recover = 1000.250  # 250 ms later

    tracker.record_failure("node_crash", "node-3", t_fail)
    tracker.record_recovery("node_crash", "node-3", t_recover)

    times = tracker.adaptation_times_ms()
    assert len(times) == 1
    assert abs(times[0] - 250.0) < 1e-3

    # No unmatched failure should contribute
    tracker.record_failure("link_drop", "link-5", 2000.0)
    times2 = tracker.adaptation_times_ms()
    assert len(times2) == 1  # still only the one matched pair
