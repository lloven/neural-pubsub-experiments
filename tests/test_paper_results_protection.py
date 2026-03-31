"""Paper results protection tests — Priority 1 critical gaps.

These tests guard the measurement harness computations that feed directly
into paper tables and figures.  A silent change in any of these would
corrupt reported results.

No Docker required — tests exercise the Python-level measurement pipeline.
"""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from src.measurement.harness import (
    AdaptationTracker,
    MetricsCollector,
    PipelineTrace,
    TimestampRecord,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts(
    pipeline_id: str,
    stage_id: str,
    event: str,
    t: float,
    node_id: str | None = None,
    metadata: dict | None = None,
) -> TimestampRecord:
    """Create a TimestampRecord with optional metadata."""
    return TimestampRecord(
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        event=event,
        timestamp=t,
        node_id=node_id,
        metadata=metadata or {},
    )


def _make_trace(
    pipeline_id: str = "p1",
    stages: list[tuple[str, float, float]] | None = None,
    t_created: float = 1.0,
    t_delivered: float = 10.0,
    placement: dict[str, str] | None = None,
) -> PipelineTrace:
    """Build a PipelineTrace from a compact stage spec.

    Args:
        stages: list of (stage_id, start_sec, end_sec) tuples.
        placement: stage_id -> node_id map.
    """
    if stages is None:
        stages = [("s0", 2.0, 4.0), ("s1", 5.0, 8.0)]
    trace = PipelineTrace(
        pipeline_id=pipeline_id,
        pipeline_type="test",
        placement=placement or {},
        success=True,
    )
    trace.timestamps.append(_ts(pipeline_id, stages[0][0], "created", t_created))
    for sid, t_start, t_end in stages:
        trace.timestamps.append(
            _ts(pipeline_id, sid, "stage_start", t_start, node_id=f"node-{sid}")
        )
        trace.timestamps.append(
            _ts(pipeline_id, sid, "stage_end", t_end, node_id=f"node-{sid}")
        )
    trace.timestamps.append(
        _ts(pipeline_id, stages[-1][0], "delivered", t_delivered)
    )
    return trace


# ---------------------------------------------------------------------------
# Test 1: network_latencies_ms decomposes correctly
# ---------------------------------------------------------------------------


class TestNetworkLatenciesDecomposition:
    """Verify that network_latencies_ms returns the correct inter-stage gaps
    and that total E2E = sum(stage compute) + sum(network gaps) + bookend gaps.
    """

    def test_two_stage_pipeline(self):
        """Known 3-stage pipeline: check per-edge network latencies."""
        # Timeline (seconds):
        #   created=1.0
        #   s0: start=2.0, end=4.0  (compute = 2s)
        #   s1: start=5.0, end=8.0  (compute = 3s)
        #   s2: start=8.5, end=9.0  (compute = 0.5s)
        #   delivered=10.0
        #
        # Network gaps:
        #   s0->s1 = 5.0 - 4.0 = 1.0s = 1000ms
        #   s1->s2 = 8.5 - 8.0 = 0.5s = 500ms
        trace = _make_trace(
            stages=[("s0", 2.0, 4.0), ("s1", 5.0, 8.0), ("s2", 8.5, 9.0)],
            t_created=1.0,
            t_delivered=10.0,
        )

        net_lats = trace.network_latencies_ms()

        assert ("s0", "s1") in net_lats
        assert ("s1", "s2") in net_lats
        assert abs(net_lats[("s0", "s1")] - 1000.0) < 0.01
        assert abs(net_lats[("s1", "s2")] - 500.0) < 0.01

    def test_decomposition_identity(self):
        """E2E latency = bookend gaps + stage compute + network gaps."""
        trace = _make_trace(
            stages=[("s0", 2.0, 4.0), ("s1", 5.0, 8.0)],
            t_created=1.0,
            t_delivered=10.0,
        )

        e2e = trace.end_to_end_latency_ms()
        stage_lats = trace.stage_latencies_ms()
        net_lats = trace.network_latencies_ms()

        total_compute = sum(stage_lats.values())    # 2000 + 3000 = 5000 ms
        total_network = sum(net_lats.values())       # 1000 ms (s0->s1)
        # Bookend gaps: created->s0_start (1s) + s1_end->delivered (2s) = 3000ms
        bookend_start = (2.0 - 1.0) * 1000.0       # 1000 ms
        bookend_end = (10.0 - 8.0) * 1000.0         # 2000 ms

        reconstructed = total_compute + total_network + bookend_start + bookend_end
        assert abs(e2e - reconstructed) < 0.01, (
            f"E2E={e2e}, reconstructed={reconstructed}"
        )


# ---------------------------------------------------------------------------
# Test 2: AdaptationTracker sub-phases
# ---------------------------------------------------------------------------


class TestAdaptationTrackerSubPhases:
    """Verify that detection_times_ms and replacement_times_ms decompose
    the full adaptation time correctly.
    """

    def test_detection_and_replacement_decomposition(self):
        """failure->detection_complete = detection; detection_complete->recovery = replacement."""
        tracker = AdaptationTracker()

        # Failure detected at t=1.0
        tracker.record_failure("node_crash", "node-3", 1.0)
        # Detection phase complete at t=1.5 (500ms detection)
        tracker.record_detection_complete("node_crash", "node-3", 1.5)
        # Recovery (re-placement) complete at t=3.0 (1500ms replacement)
        tracker.record_recovery("node_crash", "node-3", 3.0)

        detection = tracker.detection_times_ms()
        replacement = tracker.replacement_times_ms()
        total_adapt = tracker.adaptation_times_ms()

        assert len(detection) == 1
        assert len(replacement) == 1
        assert len(total_adapt) == 1

        assert abs(detection[0] - 500.0) < 0.01
        assert abs(replacement[0] - 1500.0) < 0.01
        assert abs(total_adapt[0] - 2000.0) < 0.01

        # Identity: detection + replacement == total adaptation
        assert abs(detection[0] + replacement[0] - total_adapt[0]) < 0.01

    def test_multiple_failures(self):
        """Two independent failures produce two matched pairs each."""
        tracker = AdaptationTracker()

        tracker.record_failure("node_crash", "node-A", 1.0)
        tracker.record_detection_complete("node_crash", "node-A", 1.2)
        tracker.record_failure("link_drop", "link-1", 1.5)
        tracker.record_detection_complete("link_drop", "link-1", 1.8)
        tracker.record_recovery("node_crash", "node-A", 2.0)
        tracker.record_recovery("link_drop", "link-1", 2.5)

        assert len(tracker.detection_times_ms()) == 2
        assert len(tracker.replacement_times_ms()) == 2
        assert len(tracker.adaptation_times_ms()) == 2


# ---------------------------------------------------------------------------
# Test 3: domain_crossings counts correctly
# ---------------------------------------------------------------------------


class TestDomainCrossings:
    """Verify that domain_crossings counts cross-domain edges correctly."""

    def test_two_domains_one_crossing(self):
        """Two stages in different domains produce exactly 1 crossing."""
        trace = _make_trace(
            stages=[("s0", 2.0, 4.0), ("s1", 5.0, 8.0)],
            placement={"s0": "node-A", "s1": "node-B"},
        )
        topology = {"node-A": "domain-EU", "node-B": "domain-US"}

        crossings = trace.domain_crossings(topology=topology)
        assert crossings == 1

    def test_same_domain_zero_crossings(self):
        """Two stages in the same domain produce 0 crossings."""
        trace = _make_trace(
            stages=[("s0", 2.0, 4.0), ("s1", 5.0, 8.0)],
            placement={"s0": "node-A", "s1": "node-B"},
        )
        topology = {"node-A": "domain-EU", "node-B": "domain-EU"}

        crossings = trace.domain_crossings(topology=topology)
        assert crossings == 0

    def test_three_stages_mixed_domains(self):
        """Three stages: EU->US->EU produces 2 crossings."""
        trace = _make_trace(
            stages=[("s0", 2.0, 3.0), ("s1", 4.0, 5.0), ("s2", 6.0, 7.0)],
            placement={"s0": "node-A", "s1": "node-B", "s2": "node-C"},
        )
        topology = {
            "node-A": "domain-EU",
            "node-B": "domain-US",
            "node-C": "domain-EU",
        }

        crossings = trace.domain_crossings(topology=topology)
        assert crossings == 2

    def test_metadata_fallback(self):
        """domain_crossings uses metadata['domain'] when no topology is given."""
        trace = PipelineTrace(
            pipeline_id="p1",
            pipeline_type="test",
            placement={"s0": "node-A", "s1": "node-B"},
            success=True,
        )
        trace.timestamps = [
            _ts("p1", "s0", "stage_start", 2.0, node_id="node-A",
                metadata={"domain": "domain-EU"}),
            _ts("p1", "s0", "stage_end", 3.0, node_id="node-A",
                metadata={"domain": "domain-EU"}),
            _ts("p1", "s1", "stage_start", 4.0, node_id="node-B",
                metadata={"domain": "domain-US"}),
            _ts("p1", "s1", "stage_end", 5.0, node_id="node-B",
                metadata={"domain": "domain-US"}),
        ]

        crossings = trace.domain_crossings()  # no topology arg
        assert crossings == 1


# ---------------------------------------------------------------------------
# Test 4: MetricsCollector JSON export round-trip
# ---------------------------------------------------------------------------


class TestMetricsExportJsonRoundtrip:
    """Verify that export_json produces data that can be read back with
    all fields intact.
    """

    def test_roundtrip(self):
        """Record pipelines, export JSON, read back, verify all fields."""
        collector = MetricsCollector()

        async def _run():
            # Record two pipelines
            await collector.record(
                _ts("p1", "s0", "created", 1.0,
                    metadata={"pipeline_type": "filter"})
            )
            await collector.record(
                _ts("p1", "s0", "stage_start", 2.0, node_id="node-A")
            )
            await collector.record(
                _ts("p1", "s0", "stage_end", 3.0, node_id="node-A")
            )
            await collector.record(_ts("p1", "s0", "delivered", 4.0))
            await collector.complete_pipeline("p1", success=True)

            await collector.record(
                _ts("p2", "s0", "created", 5.0,
                    metadata={"pipeline_type": "agg"})
            )
            await collector.record(
                _ts("p2", "s0", "stage_start", 6.0, node_id="node-B")
            )
            await collector.record(
                _ts("p2", "s0", "stage_end", 7.0, node_id="node-B")
            )
            await collector.record(_ts("p2", "s0", "delivered", 8.0))
            await collector.complete_pipeline("p2", success=False, error="timeout")

            with tempfile.NamedTemporaryFile(
                suffix=".json", delete=False, mode="w"
            ) as f:
                json_path = f.name

            await collector.export_json(json_path)
            return json_path

        json_path = asyncio.run(_run())

        try:
            with open(json_path) as f:
                data = json.load(f)

            assert len(data) == 2

            # Check pipeline p1
            p1 = next(d for d in data if d["pipeline_id"] == "p1")
            assert p1["pipeline_type"] == "filter"
            assert p1["success"] is True
            assert p1["error"] is None
            assert p1["placement"]["s0"] == "node-A"
            assert len(p1["timestamps"]) == 4

            # Check pipeline p2
            p2 = next(d for d in data if d["pipeline_id"] == "p2")
            assert p2["pipeline_type"] == "agg"
            assert p2["success"] is False
            assert p2["error"] == "timeout"
            assert p2["placement"]["s0"] == "node-B"

            # Verify timestamp fields are preserved
            created_rec = next(
                r for r in p1["timestamps"] if r["event"] == "created"
            )
            assert created_rec["timestamp"] == 1.0
            assert created_rec["metadata"]["pipeline_type"] == "filter"

        finally:
            Path(json_path).unlink(missing_ok=True)
