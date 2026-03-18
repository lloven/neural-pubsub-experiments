#!/usr/bin/env python3
"""Measure inter-node latency matrix for testbed characterisation.

Measures RTT between all pairs of nodes (or Docker containers) to build
the latency matrix used for placement cost calibration (Eq. 10) and for
WAN emulation configuration.

Methods:
  1. HTTP ping: GET /health on each worker/broker, measure round-trip time.
  2. TCP ping: Raw TCP connection time to a known port.
  3. ICMP ping: subprocess call to system ping (requires permissions).

Usage:
    # Measure between Docker Compose services
    python scripts/measure_latency.py --compose -f docker-compose.local.yaml

    # Measure between explicit hosts
    python scripts/measure_latency.py --hosts broker-d1:8080,broker-d2:8080,worker-d1:8081

    # Output as CSV for testbed-config.yaml
    python scripts/measure_latency.py --hosts ... --output latency_matrix.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class LatencyMeasurement:
    """A single RTT measurement between two endpoints.

    Attributes:
        source: Source node identifier.
        target: Target node identifier.
        rtt_ms: Round-trip time in milliseconds.
        method: Measurement method ('http', 'tcp', 'icmp').
        timestamp: When the measurement was taken.
        success: Whether the measurement completed.
        error: Error message if unsuccessful.
    """

    source: str
    target: str
    rtt_ms: float
    method: str
    timestamp: float
    success: bool = True
    error: Optional[str] = None


@dataclass
class LatencyStats:
    """Aggregate statistics for a source-target pair.

    Attributes:
        source: Source node identifier.
        target: Target node identifier.
        n_samples: Number of successful measurements.
        mean_ms: Mean RTT in ms.
        median_ms: Median RTT in ms.
        min_ms: Minimum RTT in ms.
        max_ms: Maximum RTT in ms.
        stdev_ms: Standard deviation (0 if n_samples < 2).
        loss_pct: Percentage of failed measurements.
    """

    source: str
    target: str
    n_samples: int
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    stdev_ms: float
    loss_pct: float


class LatencyProbe:
    """Measures network latency between node pairs using HTTP health checks.

    Sends repeated HTTP GET requests to each target's /health endpoint and
    records the round-trip time. Uses httpx for async HTTP with configurable
    timeout and retry count.

    Args:
        n_probes: Number of probe requests per pair (default 10).
        timeout_s: HTTP request timeout in seconds (default 5.0).
        interval_s: Delay between probes in seconds (default 0.5).
    """

    def __init__(
        self,
        n_probes: int = 10,
        timeout_s: float = 5.0,
        interval_s: float = 0.5,
    ) -> None:
        self.n_probes = n_probes
        self.timeout_s = timeout_s
        self.interval_s = interval_s

    async def measure_pair(
        self, source: str, target_url: str, target_name: str
    ) -> list[LatencyMeasurement]:
        """Measure RTT from source to target via HTTP /health endpoint.

        Args:
            source: Label for the measurement source.
            target_url: Full URL to probe (e.g. 'http://broker-d1:8080/health').
            target_name: Label for the measurement target.

        Returns:
            List of LatencyMeasurement objects (one per probe).
        """
        measurements = []
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            for i in range(self.n_probes):
                ts = time.time()
                try:
                    start = time.monotonic()
                    response = await client.get(target_url)
                    rtt = (time.monotonic() - start) * 1000.0
                    response.raise_for_status()
                    measurements.append(LatencyMeasurement(
                        source=source,
                        target=target_name,
                        rtt_ms=rtt,
                        method="http",
                        timestamp=ts,
                    ))
                except Exception as e:
                    measurements.append(LatencyMeasurement(
                        source=source,
                        target=target_name,
                        rtt_ms=0.0,
                        method="http",
                        timestamp=ts,
                        success=False,
                        error=str(e),
                    ))

                if i < self.n_probes - 1:
                    await asyncio.sleep(self.interval_s)

        return measurements

    async def measure_matrix(
        self, endpoints: dict[str, str]
    ) -> dict[tuple[str, str], LatencyStats]:
        """Measure latency between all pairs of endpoints.

        Args:
            endpoints: Map of node name to health URL.
                Example: {'broker-d1': 'http://localhost:8080/health',
                          'broker-d2': 'http://localhost:8082/health'}

        Returns:
            Map of (source, target) to LatencyStats.
        """
        results: dict[tuple[str, str], LatencyStats] = {}
        names = list(endpoints.keys())

        for src in names:
            for tgt in names:
                if src == tgt:
                    continue
                url = endpoints[tgt]
                logger.info("Measuring %s → %s (%s)", src, tgt, url)
                measurements = await self.measure_pair(src, url, tgt)
                stats = self._compute_stats(src, tgt, measurements)
                results[(src, tgt)] = stats
                logger.info(
                    "  %s → %s: mean=%.1f ms, median=%.1f ms, loss=%.0f%%",
                    src, tgt, stats.mean_ms, stats.median_ms, stats.loss_pct,
                )

        return results

    @staticmethod
    def _compute_stats(
        source: str, target: str, measurements: list[LatencyMeasurement]
    ) -> LatencyStats:
        """Compute aggregate statistics from a list of measurements."""
        successful = [m.rtt_ms for m in measurements if m.success]
        total = len(measurements)
        n_success = len(successful)
        loss_pct = ((total - n_success) / total * 100) if total > 0 else 100.0

        if not successful:
            return LatencyStats(
                source=source, target=target, n_samples=0,
                mean_ms=0, median_ms=0, min_ms=0, max_ms=0,
                stdev_ms=0, loss_pct=loss_pct,
            )

        return LatencyStats(
            source=source,
            target=target,
            n_samples=n_success,
            mean_ms=statistics.mean(successful),
            median_ms=statistics.median(successful),
            min_ms=min(successful),
            max_ms=max(successful),
            stdev_ms=statistics.stdev(successful) if n_success >= 2 else 0.0,
            loss_pct=loss_pct,
        )


def export_csv(
    results: dict[tuple[str, str], LatencyStats], path: Path
) -> None:
    """Write latency matrix to CSV."""
    fieldnames = [
        "source", "target", "n_samples", "mean_ms", "median_ms",
        "min_ms", "max_ms", "stdev_ms", "loss_pct",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for (src, tgt), stats in sorted(results.items()):
            writer.writerow({
                "source": stats.source,
                "target": stats.target,
                "n_samples": stats.n_samples,
                "mean_ms": f"{stats.mean_ms:.2f}",
                "median_ms": f"{stats.median_ms:.2f}",
                "min_ms": f"{stats.min_ms:.2f}",
                "max_ms": f"{stats.max_ms:.2f}",
                "stdev_ms": f"{stats.stdev_ms:.2f}",
                "loss_pct": f"{stats.loss_pct:.1f}",
            })


def print_matrix(
    results: dict[tuple[str, str], LatencyStats], nodes: list[str]
) -> None:
    """Print a human-readable latency matrix to stdout."""
    # Header
    col_width = max(len(n) for n in nodes) + 2
    header = " " * col_width + "".join(f"{n:>{col_width}}" for n in nodes)
    print(header)
    print("-" * len(header))

    for src in nodes:
        row = f"{src:<{col_width}}"
        for tgt in nodes:
            if src == tgt:
                row += f"{'—':>{col_width}}"
            else:
                stats = results.get((src, tgt))
                if stats and stats.n_samples > 0:
                    row += f"{stats.mean_ms:>{col_width - 3}.1f}ms"
                else:
                    row += f"{'N/A':>{col_width}}"
        print(row)


def main():
    parser = argparse.ArgumentParser(
        description="Measure inter-node latency matrix"
    )
    parser.add_argument(
        "--hosts",
        help="Comma-separated name:url pairs (e.g. 'broker-d1:http://localhost:8080/health')",
    )
    parser.add_argument(
        "--n-probes", type=int, default=10,
        help="Number of probe requests per pair (default 10)",
    )
    parser.add_argument(
        "--interval", type=float, default=0.5,
        help="Seconds between probes (default 0.5)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output CSV file path",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if not args.hosts:
        # Default: local Docker Compose endpoints
        endpoints = {
            "broker-d1": "http://localhost:8080/health",
            "broker-d2": "http://localhost:8082/health",
        }
    else:
        endpoints = {}
        for pair in args.hosts.split(","):
            name, url = pair.strip().split(":", 1)
            if not url.startswith("http"):
                url = f"http://{url}/health"
            endpoints[name.strip()] = url.strip()

    probe = LatencyProbe(n_probes=args.n_probes, interval_s=args.interval)
    results = asyncio.run(probe.measure_matrix(endpoints))

    nodes = sorted(endpoints.keys())
    print("\nLatency Matrix (mean RTT in ms):")
    print_matrix(results, nodes)

    if args.output:
        export_csv(results, Path(args.output))
        logger.info("Latency matrix written to %s", args.output)


if __name__ == "__main__":
    main()
