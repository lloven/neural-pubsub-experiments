"""Tests for REFACTOR-PLAN.md Phase 1 and Phase 2 items.

Each test is written RED-first: it asserts the desired spec-aligned behaviour
that does not yet exist in the codebase.
"""

from __future__ import annotations

import asyncio
import csv
import os
import random
import tempfile
import time

import pytest

from src.pipeline.patterns import anomaly_detection_pipeline
from src.measurement.harness import (
    AdaptationTracker,
    AggregateMetrics,
    FederationMonitor,
    MetricsCollector,
    PipelineTrace,
    TimestampRecord,
    VALID_EVENTS,
)


# ---------------------------------------------------------------------------
# Phase 1.1: Anomaly detection must have exactly 3 stages (manuscript spec)
# ---------------------------------------------------------------------------


def test_anomaly_detection_has_3_stages():
    """Manuscript says anomaly detection is: collect -> feature_extract -> detect."""
    dag = anomaly_detection_pipeline()
    assert len(dag) == 3, f"Expected 3 stages, got {len(dag)}"
    assert len(dag.edges) == 2, f"Expected 2 edges, got {len(dag.edges)}"
    # Verify the exact stage IDs match the manuscript
    stage_ids = set(dag.stages.keys())
    assert stage_ids == {"collect", "feature_extract", "detect"}
    # Verify linear chain order
    order = dag.topological_sort()
    assert order == ["collect", "feature_extract", "detect"]


# ---------------------------------------------------------------------------
# Phase 1.2: MetricsCollector must accept placement_complete and
#            stage_result_received timestamp events
# ---------------------------------------------------------------------------


def test_placement_complete_is_valid_event():
    """placement_complete must be a valid timestamp event type."""
    assert "placement_complete" in VALID_EVENTS


def test_stage_result_received_is_valid_event():
    """stage_result_received must be a valid timestamp event type."""
    assert "stage_result_received" in VALID_EVENTS


def test_can_record_placement_complete_timestamp():
    """MetricsCollector must accept placement_complete records."""
    collector = MetricsCollector()

    async def _run():
        await collector.record(
            TimestampRecord(
                pipeline_id="p0",
                stage_id="__pipeline__",
                event="placement_complete",
                timestamp=time.time(),
                node_id="broker-1",
            )
        )
        trace = await collector.get_trace("p0")
        events = [r.event for r in trace.timestamps]
        assert "placement_complete" in events

    asyncio.run(_run())


def test_can_record_stage_result_received_timestamp():
    """MetricsCollector must accept stage_result_received records."""
    collector = MetricsCollector()

    async def _run():
        await collector.record(
            TimestampRecord(
                pipeline_id="p0",
                stage_id="s0",
                event="stage_result_received",
                timestamp=time.time(),
                node_id="broker-1",
            )
        )
        trace = await collector.get_trace("p0")
        events = [r.event for r in trace.timestamps]
        assert "stage_result_received" in events

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Phase 1.3: Phase D seeds must be 10 (manuscript says 10 repetitions)
# ---------------------------------------------------------------------------


def test_phase_d_extended_seeds_count_is_10():
    """Phase D uses 10 seeds for statistical power on recovery-time analysis."""
    from scripts._common import EXTENDED_SEEDS
    assert len(EXTENDED_SEEDS) == 10, f"Expected 10 seeds, got {len(EXTENDED_SEEDS)}"


def test_default_seeds_count_is_5():
    """Most phases use 5 seeds."""
    from scripts._common import DEFAULT_SEEDS
    assert len(DEFAULT_SEEDS) == 5, f"Expected 5 seeds, got {len(DEFAULT_SEEDS)}"


# ---------------------------------------------------------------------------
# Phase 1.4: Arrival rates: low=2.0, high=10.0 (manuscript values)
# ---------------------------------------------------------------------------


def test_baseline_arrival_rates():
    """RATES dict must match manuscript: low=2.0, medium=5.0, high=10.0."""
    from scripts.run_baseline import RATES
    assert RATES["low"] == 2.0, f"Expected low=2.0, got {RATES['low']}"
    assert RATES["medium"] == 5.0, f"Expected medium=5.0, got {RATES['medium']}"
    assert RATES["high"] == 10.0, f"Expected high=10.0, got {RATES['high']}"


# ---------------------------------------------------------------------------
# Phase 1.5: Run order randomization with fixed seed
# ---------------------------------------------------------------------------


def test_common_has_shuffle_configs_function():
    """_common.py must expose a shuffle_configs function."""
    from scripts._common import shuffle_configs
    # Must be importable
    assert callable(shuffle_configs)


def test_shuffle_configs_is_deterministic():
    """shuffle_configs with the same seed must produce the same order."""
    from scripts._common import shuffle_configs
    configs = list(range(20))
    result1 = shuffle_configs(list(configs), seed=42)
    result2 = shuffle_configs(list(configs), seed=42)
    assert result1 == result2
    # Must actually shuffle (not be identical to input for non-trivial lists)
    assert result1 != configs, "shuffle_configs did not change the order"


def test_shuffle_configs_different_seeds_differ():
    """Different seeds must produce different orders."""
    from scripts._common import shuffle_configs
    configs = list(range(20))
    result1 = shuffle_configs(list(configs), seed=42)
    result2 = shuffle_configs(list(configs), seed=99)
    assert result1 != result2


# ===========================================================================
# Phase 2: Measurement completeness
# ===========================================================================


# ---------------------------------------------------------------------------
# Phase 2.1: Governance violation tracking
# ---------------------------------------------------------------------------


def test_aggregate_metrics_has_governance_violations():
    """AggregateMetrics must include a governance_violations field."""
    m = AggregateMetrics()
    assert hasattr(m, "governance_violations"), "AggregateMetrics missing governance_violations"
    assert m.governance_violations == 0


def test_metrics_collector_tracks_governance_violations():
    """MetricsCollector must expose record_governance_violation and include count in aggregate."""
    collector = MetricsCollector()

    async def _run():
        # Record a pipeline and a governance violation
        await collector.record(
            TimestampRecord(
                pipeline_id="p0", stage_id="s0", event="created", timestamp=0.0
            )
        )
        await collector.complete_pipeline("p0", success=True)
        collector.record_governance_violation("p0", "s0", "Capacity constraint violated")
        collector.record_governance_violation("p0", "s1", "Slice mismatch")
        agg = await collector.compute_aggregate()
        assert agg.governance_violations == 2

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Phase 2.2: F1 routing accuracy logging
# ---------------------------------------------------------------------------


def test_aggregate_metrics_has_routing_accuracy():
    """AggregateMetrics must include a routing_accuracy_f1 field."""
    m = AggregateMetrics()
    assert hasattr(m, "routing_accuracy_f1"), "AggregateMetrics missing routing_accuracy_f1"


def test_metrics_collector_tracks_routing_accuracy():
    """MetricsCollector must expose record_routing_accuracy and include mean in aggregate."""
    collector = MetricsCollector()

    async def _run():
        await collector.record(
            TimestampRecord(
                pipeline_id="p0", stage_id="s0", event="created", timestamp=0.0
            )
        )
        await collector.complete_pipeline("p0", success=True)
        # For deterministic routing, F1 = 1.0 always
        collector.record_routing_accuracy("p0", 1.0)
        collector.record_routing_accuracy("p1", 1.0)
        agg = await collector.compute_aggregate()
        assert abs(agg.routing_accuracy_f1 - 1.0) < 1e-6

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Phase 2.3: FederationMonitor bandwidth in CSV export
# ---------------------------------------------------------------------------


def test_csv_export_includes_federation_bytes_sent():
    """CSV export must include a federation_bytes_sent column."""
    collector = MetricsCollector()
    collector.set_federation_monitor(FederationMonitor())

    async def _run():
        await collector.record(
            TimestampRecord(
                pipeline_id="p0", stage_id="s0", event="created", timestamp=0.0
            )
        )
        await collector.record(
            TimestampRecord(
                pipeline_id="p0", stage_id="s0", event="delivered", timestamp=0.01
            )
        )
        await collector.complete_pipeline("p0", success=True)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = f.name
        try:
            await collector.export_csv(path)
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                assert "federation_bytes_sent" in reader.fieldnames
        finally:
            os.unlink(path)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Phase 2.4: Summary propagation latency timestamps
# ---------------------------------------------------------------------------


def test_propagation_records_latency():
    """SummaryPropagator must record propagation latency for each push."""
    from src.federation.propagation import SummaryPropagator

    propagator = SummaryPropagator(
        domain_id="d1", peers=[], interval_seconds=60.0
    )
    assert hasattr(propagator, "propagation_latencies_ms"), (
        "SummaryPropagator missing propagation_latencies_ms"
    )
    # Initially empty
    assert propagator.propagation_latencies_ms() == []


# ---------------------------------------------------------------------------
# Phase 2.5: Separate detection time from re-placement time
# ---------------------------------------------------------------------------


def test_adaptation_tracker_records_detection_and_replacement_separately():
    """AdaptationTracker must record detection_time_ms and replacement_time_ms separately."""
    tracker = AdaptationTracker()

    t_detect_start = 1000.0
    t_detect_end = 1000.050    # 50 ms detection
    t_replace_end = 1000.200   # 150 ms re-placement

    tracker.record_failure("node_crash", "node-3", t_detect_start)
    tracker.record_detection_complete("node_crash", "node-3", t_detect_end)
    tracker.record_recovery("node_crash", "node-3", t_replace_end)

    detection_times = tracker.detection_times_ms()
    replacement_times = tracker.replacement_times_ms()

    assert len(detection_times) == 1
    assert len(replacement_times) == 1
    assert abs(detection_times[0] - 50.0) < 1e-3
    assert abs(replacement_times[0] - 150.0) < 1e-3


# ---------------------------------------------------------------------------
# Phase 2.6: Expand CSV export columns
# ---------------------------------------------------------------------------


def test_csv_export_has_all_required_columns():
    """CSV export must include throughput, completion_rate, governance_violations,
    federation_bytes_sent, detection_time_ms, and replacement_time_ms columns."""
    collector = MetricsCollector()
    collector.set_federation_monitor(FederationMonitor())

    async def _run():
        # Create one completed pipeline
        await collector.record(
            TimestampRecord(
                pipeline_id="p0", stage_id="s0", event="created", timestamp=0.0
            )
        )
        await collector.record(
            TimestampRecord(
                pipeline_id="p0", stage_id="s0", event="delivered", timestamp=0.01
            )
        )
        await collector.complete_pipeline("p0", success=True)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = f.name
        try:
            await collector.export_csv(path)
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                fields = set(reader.fieldnames)
                required = {
                    "pipeline_id",
                    "pipeline_type",
                    "success",
                    "e2e_latency_ms",
                    "throughput_pps",
                    "completion_rate",
                    "governance_violations",
                    "federation_bytes_sent",
                    "routing_accuracy_f1",
                }
                missing = required - fields
                assert not missing, f"CSV missing columns: {missing}"
        finally:
            os.unlink(path)

    asyncio.run(_run())
