"""Integration tests for CSV round-trip: MetricsCollector -> export_csv -> read back.

GAP-1 from test-completeness-integration.md: CSV output was never verified
end-to-end. These tests confirm that MetricsCollector.export_csv() produces
a CSV file with correct schema, values, and semantics.

No Docker required — tests exercise the Python-level measurement pipeline.
"""

import asyncio
import csv
import os
import tempfile
from pathlib import Path

import pytest

from src.measurement.harness import (
    FederationMonitor,
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


async def _record_pipeline(
    collector: MetricsCollector,
    pipeline_id: str,
    pipeline_type: str,
    t_created: float,
    stages: list[tuple[str, float, float]],  # (stage_id, t_start, t_end)
    t_delivered: float,
    success: bool = True,
    partial: bool = False,
    error: str | None = None,
) -> None:
    """Record a complete pipeline lifecycle in the collector."""
    await collector.record(
        _ts(pipeline_id, stages[0][0], "created", t_created,
            metadata={"pipeline_type": pipeline_type})
    )
    for stage_id, t_start, t_end in stages:
        await collector.record(
            _ts(pipeline_id, stage_id, "stage_start", t_start, node_id=f"node-{stage_id}")
        )
        await collector.record(
            _ts(pipeline_id, stage_id, "stage_end", t_end, node_id=f"node-{stage_id}")
        )
    await collector.record(
        _ts(pipeline_id, stages[-1][0], "delivered", t_delivered)
    )
    await collector.complete_pipeline(
        pipeline_id, success=success, partial=partial, error=error
    )


def _read_csv(path: str) -> tuple[list[str], list[dict[str, str]]]:
    """Read CSV and return (fieldnames, rows)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


# ---------------------------------------------------------------------------
# Test 1: Schema verification — all expected columns present
# ---------------------------------------------------------------------------


class TestCSVSchema:
    """Verify exported CSV has the correct column structure."""

    def test_csv_has_all_required_columns(self, tmp_path: Path) -> None:
        """export_csv produces a CSV with all expected base columns."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector,
                pipeline_id="p0",
                pipeline_type="cqi_prediction",
                t_created=100.0,
                stages=[("s0", 100.005, 100.010), ("s1", 100.015, 100.025)],
                t_delivered=100.030,
                success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        fieldnames, rows = _read_csv(csv_path)

        required_base_columns = [
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
        for col in required_base_columns:
            assert col in fieldnames, f"Missing required column: {col}"

    def test_csv_has_per_stage_columns(self, tmp_path: Path) -> None:
        """export_csv includes stage_<id>_ms columns for each stage seen."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector,
                pipeline_id="p0",
                pipeline_type="test",
                t_created=0.0,
                stages=[("s0", 0.005, 0.010), ("s1", 0.015, 0.025), ("s2", 0.030, 0.040)],
                t_delivered=0.045,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        fieldnames, _ = _read_csv(csv_path)
        assert "stage_s0_ms" in fieldnames
        assert "stage_s1_ms" in fieldnames
        assert "stage_s2_ms" in fieldnames


# ---------------------------------------------------------------------------
# Test 2: Latency values are positive
# ---------------------------------------------------------------------------


class TestCSVLatencyValues:
    """Verify latency values in the exported CSV are physically valid."""

    def test_e2e_latency_is_positive(self, tmp_path: Path) -> None:
        """End-to-end latency must be positive for successful pipelines."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector,
                pipeline_id="p0",
                pipeline_type="test",
                t_created=1000.0,
                stages=[("s0", 1000.005, 1000.010)],
                t_delivered=1000.020,
                success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        assert len(rows) == 1
        latency = float(rows[0]["e2e_latency_ms"])
        assert latency > 0, f"e2e_latency_ms should be positive, got {latency}"

    def test_stage_latencies_are_positive(self, tmp_path: Path) -> None:
        """Per-stage latencies must be positive."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector,
                pipeline_id="p0",
                pipeline_type="test",
                t_created=0.0,
                stages=[("s0", 0.005, 0.010), ("s1", 0.015, 0.030)],
                t_delivered=0.035,
                success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        s0_ms = float(rows[0]["stage_s0_ms"])
        s1_ms = float(rows[0]["stage_s1_ms"])
        assert s0_ms > 0, f"stage_s0_ms should be positive, got {s0_ms}"
        assert s1_ms > 0, f"stage_s1_ms should be positive, got {s1_ms}"

    def test_e2e_latency_numeric_accuracy(self, tmp_path: Path) -> None:
        """e2e_latency_ms = (t_delivered - t_created) * 1000."""
        csv_path = str(tmp_path / "metrics.csv")
        t_created = 500.0
        t_delivered = 500.042  # expect 42.0 ms

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector,
                pipeline_id="p0",
                pipeline_type="test",
                t_created=t_created,
                stages=[("s0", 500.005, 500.035)],
                t_delivered=t_delivered,
                success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        latency = float(rows[0]["e2e_latency_ms"])
        expected = (t_delivered - t_created) * 1000.0
        assert abs(latency - expected) < 0.1, (
            f"Expected ~{expected:.1f} ms, got {latency:.1f} ms"
        )


# ---------------------------------------------------------------------------
# Test 3: Success/failure flags match recorded state
# ---------------------------------------------------------------------------


class TestCSVSuccessFailureFlags:
    """Verify success and error columns reflect the pipeline completion state."""

    def test_successful_pipeline_flags(self, tmp_path: Path) -> None:
        """Successful pipeline: success=True, error empty."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector, "p0", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015,
                success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        assert rows[0]["success"] == "True"
        assert rows[0]["error"] == ""

    def test_failed_pipeline_flags(self, tmp_path: Path) -> None:
        """Failed pipeline: success=False, error message preserved."""
        csv_path = str(tmp_path / "metrics.csv")
        error_msg = "Timeout exceeded at stage s1"

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector, "p0", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015,
                success=False, error=error_msg,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        assert rows[0]["success"] == "False"
        assert rows[0]["error"] == error_msg

    def test_mixed_success_failure(self, tmp_path: Path) -> None:
        """A mix of successful and failed pipelines produces correct flags."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector, "p_ok", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015,
                success=True,
            )
            await _record_pipeline(
                collector, "p_fail", "test", 0.020,
                [("s0", 0.025, 0.030)], 0.035,
                success=False, error="node crash",
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        row_map = {r["pipeline_id"]: r for r in rows}
        assert row_map["p_ok"]["success"] == "True"
        assert row_map["p_fail"]["success"] == "False"
        assert row_map["p_fail"]["error"] == "node crash"


# ---------------------------------------------------------------------------
# Test 4: Warmup flag correctness
# ---------------------------------------------------------------------------


class TestCSVWarmupFlag:
    """Verify warmup tagging in CSV (via WorkloadGenerator._tag_warmup_in_csv)."""

    def test_warmup_tagging_adds_column(self, tmp_path: Path) -> None:
        """After warmup tagging, CSV has a 'warmup' column."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector, "p0", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015, success=True,
            )
            await _record_pipeline(
                collector, "p1", "test", 10.0,
                [("s0", 10.005, 10.010)], 10.015, success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        # Simulate warmup tagging (what WorkloadGenerator._tag_warmup_in_csv does)
        from src.workload.generator import WorkloadGenerator, WorkloadConfig

        config = WorkloadConfig(
            arrival_rate=1.0,
            duration_s=60.0,
            pipeline_mix={"cqi_prediction": 1.0},
            broker_url="http://localhost:8080",
            warmup_s=5.0,
        )
        gen = WorkloadGenerator(config)
        gen._warmup_pipeline_ids = {"p0"}  # p0 is warmup, p1 is not
        gen._tag_warmup_in_csv(csv_path)

        fieldnames, rows = _read_csv(csv_path)
        assert "warmup" in fieldnames, "Warmup column missing after tagging"

        row_map = {r["pipeline_id"]: r for r in rows}
        assert row_map["p0"]["warmup"] == "True"
        assert row_map["p1"]["warmup"] == "False"


# ---------------------------------------------------------------------------
# Test 5: Partial flag (funnel integration)
# ---------------------------------------------------------------------------


class TestCSVPartialFlag:
    """Verify the partial flag appears in CSV for funnel-mode pipelines."""

    def test_partial_flag_true_in_csv(self, tmp_path: Path) -> None:
        """Pipeline completed with partial inputs: partial=True in CSV."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector, "p_partial", "sensor_fusion", 0.0,
                [("fuse", 0.005, 0.020)], 0.025,
                success=True, partial=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        assert rows[0]["partial"] == "True"

    def test_partial_flag_false_for_normal(self, tmp_path: Path) -> None:
        """Normal pipeline: partial=False in CSV."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            await _record_pipeline(
                collector, "p_normal", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015,
                success=True, partial=False,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        assert rows[0]["partial"] == "False"


# ---------------------------------------------------------------------------
# Test 6: Governance violations column semantics
# ---------------------------------------------------------------------------


class TestCSVGovernanceViolations:
    """Verify governance_violations column semantics.

    KNOWN QUESTION: Is governance_violations the cumulative check count or
    the actual number of violations? This test documents the ACTUAL behavior
    and verifies it is consistent.
    """

    def test_governance_violations_counts_recorded_violations(self, tmp_path: Path) -> None:
        """governance_violations in CSV reflects the number of recorded violations."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            # Record 2 governance violations
            collector.record_governance_violation("p0", "s0", "sovereignty breach: d2 -> d1")
            collector.record_governance_violation("p0", "s1", "latency SLA exceeded")

            await _record_pipeline(
                collector, "p0", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015, success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        gov_violations = int(rows[0]["governance_violations"])
        assert gov_violations == 2, (
            f"Expected 2 violations, got {gov_violations}. "
            "governance_violations should count actual recorded violations."
        )

    def test_governance_violations_is_global_not_per_pipeline(self, tmp_path: Path) -> None:
        """governance_violations is a GLOBAL count repeated on every row.

        This documents the current behavior: each CSV row gets the total
        count from len(self._governance_violations), not a per-pipeline count.
        This means every row has the same value, which is a design choice
        (not a bug) but should be documented clearly.
        """
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            # 1 violation on p0, 0 on p1
            collector.record_governance_violation("p0", "s0", "breach")

            await _record_pipeline(
                collector, "p0", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015, success=True,
            )
            await _record_pipeline(
                collector, "p1", "test", 1.0,
                [("s0", 1.005, 1.010)], 1.015, success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        # Both rows should have the SAME governance_violations value
        # because it's a global counter (len(self._governance_violations))
        v0 = int(rows[0]["governance_violations"])
        v1 = int(rows[1]["governance_violations"])
        assert v0 == v1 == 1, (
            f"governance_violations should be global (same on all rows): "
            f"p0={v0}, p1={v1}. Expected both to be 1."
        )


# ---------------------------------------------------------------------------
# Test 7: Throughput computation
# ---------------------------------------------------------------------------


class TestCSVThroughput:
    """Verify throughput_pps is computed from wall-clock duration.

    NOTE: complete_pipeline() sets _wall_end = time.time(), while record()
    uses the TimestampRecord.timestamp for _wall_start. In production these
    are both wall-clock values. Tests must use wall-clock timestamps to match
    this contract (synthetic timestamps like 0.0 would produce near-zero
    throughput because _wall_end would be ~1.7 billion seconds ahead of
    _wall_start=0.0).
    """

    def test_throughput_is_nonzero_for_completed_pipelines(self, tmp_path: Path) -> None:
        """throughput_pps should be positive when pipelines complete."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            # Use wall-clock timestamps so _wall_start and _wall_end are coherent.
            # complete_pipeline() uses time.time() for _wall_end, so _wall_start
            # (from the first record's timestamp) must also be wall-clock.
            import time as _time
            t_now = _time.time()
            for i in range(3):
                t_base = t_now + i * 0.001
                await _record_pipeline(
                    collector, f"p{i}", "test", t_base,
                    [("s0", t_base + 0.0001, t_base + 0.0002)],
                    t_base + 0.0003, success=True,
                )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        throughput = float(rows[0]["throughput_pps"])
        assert throughput > 0, f"throughput_pps should be positive, got {throughput}"

    def test_throughput_is_same_on_all_rows(self, tmp_path: Path) -> None:
        """throughput_pps is an aggregate metric, same for all rows."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            import time as _time
            t_now = _time.time()
            for i in range(3):
                t_base = t_now + i * 0.001
                await _record_pipeline(
                    collector, f"p{i}", "test", t_base,
                    [("s0", t_base + 0.0001, t_base + 0.0002)],
                    t_base + 0.0003, success=True,
                )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        throughputs = {float(r["throughput_pps"]) for r in rows}
        assert len(throughputs) == 1, (
            f"throughput_pps should be identical on all rows, got {throughputs}"
        )


# ---------------------------------------------------------------------------
# Test 8: Round-trip: N pipelines -> export -> verify N rows
# ---------------------------------------------------------------------------


class TestCSVRoundTrip:
    """Record N pipelines, export CSV, read back, verify N rows."""

    @pytest.mark.parametrize("n_pipelines", [1, 5, 20])
    def test_row_count_matches_pipeline_count(self, tmp_path: Path, n_pipelines: int) -> None:
        """CSV contains exactly one row per recorded pipeline."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            for i in range(n_pipelines):
                t_base = i * 0.100
                await _record_pipeline(
                    collector, f"p{i}", "test", t_base,
                    [("s0", t_base + 0.005, t_base + 0.010)],
                    t_base + 0.015, success=(i % 3 != 0),
                    error="injected failure" if (i % 3 == 0) else None,
                )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        assert len(rows) == n_pipelines, (
            f"Expected {n_pipelines} rows, got {len(rows)}"
        )

    def test_pipeline_ids_round_trip_correctly(self, tmp_path: Path) -> None:
        """All pipeline_ids survive the CSV round-trip."""
        csv_path = str(tmp_path / "metrics.csv")
        expected_ids = {f"pipeline-{i:04d}" for i in range(10)}

        async def _run():
            collector = MetricsCollector()
            for pid in sorted(expected_ids):
                await _record_pipeline(
                    collector, pid, "test", 0.0,
                    [("s0", 0.005, 0.010)], 0.015, success=True,
                )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        actual_ids = {r["pipeline_id"] for r in rows}
        assert actual_ids == expected_ids

    def test_completion_rate_is_correct(self, tmp_path: Path) -> None:
        """completion_rate = completed / total."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            # 3 success, 2 failure = 60% completion rate
            for i in range(5):
                t_base = i * 0.050
                await _record_pipeline(
                    collector, f"p{i}", "test", t_base,
                    [("s0", t_base + 0.005, t_base + 0.010)],
                    t_base + 0.015,
                    success=(i < 3),
                    error=None if (i < 3) else "fail",
                )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        completion_rate = float(rows[0]["completion_rate"])
        expected = 3.0 / 5.0  # 0.6
        assert abs(completion_rate - expected) < 0.001, (
            f"Expected completion_rate={expected:.4f}, got {completion_rate:.4f}"
        )

    def test_federation_bytes_in_csv(self, tmp_path: Path) -> None:
        """federation_bytes_sent reflects FederationMonitor state."""
        csv_path = str(tmp_path / "metrics.csv")

        async def _run():
            collector = MetricsCollector()
            monitor = FederationMonitor()
            monitor.record_summary_sent(1024, "d1", "d2")
            monitor.record_summary_received(512, "d2", "d1")
            collector.set_federation_monitor(monitor)

            await _record_pipeline(
                collector, "p0", "test", 0.0,
                [("s0", 0.005, 0.010)], 0.015, success=True,
            )
            await collector.export_csv(csv_path)

        asyncio.run(_run())

        _, rows = _read_csv(csv_path)
        fed_bytes = int(rows[0]["federation_bytes_sent"])
        assert fed_bytes == 1024 + 512, (
            f"Expected 1536 federation bytes, got {fed_bytes}"
        )
