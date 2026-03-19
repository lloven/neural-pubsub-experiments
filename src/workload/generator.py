"""Workload generator for Neural Pub/Sub experiments.

Generates pipeline execution requests with Poisson inter-arrival times,
using three pipeline templates from the paper's evaluation scenario:

1. CQI Prediction (collect → feature_extract → predict): tree-structured,
   URLLC slice, data-sovereignty constraint on the collect stage.
2. Anomaly Detection (ingest → preprocess → detect → alert): linear chain,
   eMBB slice.
3. Sensor Fusion (sensor_0..N → fuse → decide): funnel DAG, configurable
   sensor count (default 3, giving 5 stages total).

Inter-arrival times are drawn from an exponential distribution
(Poisson process) with rate ``arrival_rate`` events per second::

    inter_arrival = np.random.exponential(1.0 / arrival_rate)

Usage:
    python -m src.workload --config configs/workload.yaml --broker-url http://broker:8080
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import httpx
import numpy as np
import yaml

from src.pipeline.dag import PipelineDAG
from src.pipeline.patterns import (
    anomaly_detection_pipeline,
    cqi_prediction_pipeline,
    sensor_fusion_pipeline,
)

logger = logging.getLogger(__name__)

# Default number of sensors used when creating a sensor-fusion request.
# sensor_fusion_pipeline(3) produces 5 stages (3 sensors + fuse + decide).
_DEFAULT_N_SENSORS = 3


# ---------------------------------------------------------------------------
# Configuration and data classes
# ---------------------------------------------------------------------------


@dataclass
class WorkloadConfig:
    """Configuration for the workload generator.

    Attributes:
        arrival_rate: Mean number of pipeline requests generated per second
            (lambda in the Poisson process).
        duration_s: Total duration of the workload generation run in seconds.
        pipeline_mix: Mapping from pipeline template name to selection
            probability. Keys must be a subset of
            {"cqi_prediction", "anomaly_detection", "sensor_fusion"}.
            Values must be non-negative and sum to 1.0.
        broker_url: Base URL of the broker; requests are POSTed to
            ``{broker_url}/publish``.
        seed: Random seed for reproducibility. Passed to
            ``np.random.default_rng``.
    """

    arrival_rate: float
    duration_s: float
    pipeline_mix: dict[str, float]
    broker_url: str
    seed: int = 42
    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        total = sum(self.pipeline_mix.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"pipeline_mix probabilities must sum to 1.0, got {total:.6f}."
            )
        if self.arrival_rate <= 0:
            raise ValueError("arrival_rate must be positive.")
        if self.duration_s <= 0:
            raise ValueError("duration_s must be positive.")


@dataclass
class PipelineRequest:
    """A single pipeline execution request to be submitted to the broker.

    Attributes:
        request_id: UUID string uniquely identifying this request.
        pipeline_type: Name of the template used ("cqi_prediction",
            "anomaly_detection", or "sensor_fusion").
        dag: The fully-constructed PipelineDAG for this request.
        created_at: Unix timestamp (seconds) when this request was created.
        metadata: Arbitrary key-value annotations (e.g. sensor count,
            experiment label).
    """

    request_id: str
    pipeline_type: str
    dag: PipelineDAG
    created_at: float
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Workload generator
# ---------------------------------------------------------------------------


class WorkloadGenerator:
    """Generates Poisson-arrival pipeline requests and submits them to a broker.

    The generator maintains an internal numpy RNG seeded from
    ``config.seed`` so that runs are reproducible.

    Example::

        config = WorkloadConfig(
            arrival_rate=5.0,
            duration_s=60.0,
            pipeline_mix={"cqi_prediction": 0.5, "anomaly_detection": 0.3,
                          "sensor_fusion": 0.2},
            broker_url="http://localhost:8080",
            seed=0,
        )
        gen = WorkloadGenerator(config)
        await gen.run()
        print(gen.get_stats())
    """

    # Supported template names and their factory callables.
    _TEMPLATES = {
        "cqi_prediction": cqi_prediction_pipeline,
        "anomaly_detection": anomaly_detection_pipeline,
        "sensor_fusion": lambda: sensor_fusion_pipeline(_DEFAULT_N_SENSORS),
    }

    # Maximum number of concurrent in-flight publish calls.
    _MAX_CONCURRENT_PUBLISHES = 64

    def __init__(self, config: WorkloadConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.seed)
        self._total_generated: int = 0
        self._by_type: dict[str, int] = {k: 0 for k in self._TEMPLATES}
        self._run_start: Optional[float] = None
        self._publish_semaphore = asyncio.Semaphore(self._MAX_CONCURRENT_PUBLISHES)
        self._pending_tasks: set[asyncio.Task] = set()

        # Validate pipeline_mix keys
        unknown = set(config.pipeline_mix) - set(self._TEMPLATES)
        if unknown:
            raise ValueError(
                f"Unknown pipeline types in pipeline_mix: {unknown}. "
                f"Valid types: {set(self._TEMPLATES)}."
            )

    # ------------------------------------------------------------------
    # Request generation
    # ------------------------------------------------------------------

    def generate_request(self) -> PipelineRequest:
        """Generate a single pipeline request by sampling from the template mix.

        The template is chosen according to the weighted probabilities in
        ``config.pipeline_mix``. A fresh PipelineDAG is constructed for each
        request so that concurrent executions are independent.

        Returns:
            A PipelineRequest with a unique request_id and the current
            creation timestamp.
        """
        names = list(self.config.pipeline_mix.keys())
        probs = [self.config.pipeline_mix[n] for n in names]

        # numpy choice requires integer indices; use rng.choice on indices
        idx = int(self._rng.choice(len(names), p=probs))
        pipeline_type = names[idx]

        dag = self._TEMPLATES[pipeline_type]()

        request = PipelineRequest(
            request_id=str(uuid.uuid4()),
            pipeline_type=pipeline_type,
            dag=dag,
            created_at=time.time(),
            metadata={"pipeline_type": pipeline_type},
        )

        self._total_generated += 1
        self._by_type[pipeline_type] = self._by_type.get(pipeline_type, 0) + 1

        logger.debug(
            "Generated request %s (type=%s).", request.request_id, pipeline_type
        )
        return request

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the workload generator for ``config.duration_s`` seconds.

        Samples Poisson inter-arrival times and posts each generated request
        to ``{broker_url}/publish`` as a JSON payload. The loop exits after
        ``duration_s`` seconds regardless of how many requests have been sent.

        The JSON body sent to the broker contains:
            - ``request_id``: UUID string.
            - ``pipeline_type``: Template name.
            - ``stage_ids``: List of stage IDs in topological order.
            - ``created_at``: Creation timestamp.
            - ``metadata``: Metadata dict.
        """
        self._run_start = time.time()
        deadline = self._run_start + self.config.duration_s
        mean_inter_arrival = 1.0 / self.config.arrival_rate
        heartbeat_interval = 60.0  # seconds between progress reports
        next_heartbeat = self._run_start + heartbeat_interval

        logger.info(
            "Workload generator starting: rate=%.2f req/s, duration=%.1f s, "
            "broker=%s.",
            self.config.arrival_rate,
            self.config.duration_s,
            self.config.broker_url,
        )

        async with httpx.AsyncClient() as client:
            while True:
                now = time.time()
                remaining = deadline - now
                if remaining <= 0:
                    break

                # Periodic progress heartbeat
                if now >= next_heartbeat:
                    elapsed = now - self._run_start
                    actual_rate = self._total_generated / elapsed if elapsed > 0 else 0.0
                    logger.info(
                        "HEARTBEAT  elapsed=%.0fs  sent=%d  rate=%.2f req/s  "
                        "remaining=%.0fs",
                        elapsed,
                        self._total_generated,
                        actual_rate,
                        remaining,
                    )
                    next_heartbeat = now + heartbeat_interval

                # Poisson inter-arrival time
                inter_arrival = float(
                    self._rng.exponential(mean_inter_arrival)
                )

                # Clamp sleep to remaining time so we don't overshoot
                sleep_time = min(inter_arrival, remaining)
                await asyncio.sleep(sleep_time)

                if time.time() >= deadline:
                    break

                request = self.generate_request()

                # Fire publish as a background task so that broker response
                # time does not distort Poisson inter-arrival timing.
                task = asyncio.create_task(
                    self._guarded_publish(client, request)
                )
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)

            # Wait for any remaining in-flight publishes to complete.
            if self._pending_tasks:
                await asyncio.gather(*self._pending_tasks, return_exceptions=True)
                self._pending_tasks.clear()

        elapsed = time.time() - self._run_start
        stats = self.get_stats()
        logger.info(
            "Workload generator finished: %d requests in %.1f s (actual rate=%.2f req/s).",
            stats["total_generated"],
            elapsed,
            stats["actual_rate"],
        )

        # Request broker to export metrics CSV before we exit
        result_file = self.config.metadata.get("result_file", "")
        if result_file:
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.post(
                        f"{self.config.broker_url}/metrics/export",
                        json={"path": result_file},
                        timeout=10.0,
                    )
                    if resp.status_code == 200:
                        logger.info("Metrics exported to %s", result_file)
                    else:
                        logger.warning("Metrics export failed: HTTP %d", resp.status_code)
            except Exception as e:
                logger.warning("Could not export metrics: %s", e)

    async def _guarded_publish(
        self, client: httpx.AsyncClient, request: PipelineRequest
    ) -> None:
        """Publish with bounded concurrency via semaphore."""
        async with self._publish_semaphore:
            await self._publish(client, request)

    async def _publish(
        self, client: httpx.AsyncClient, request: PipelineRequest
    ) -> None:
        """POST a pipeline request to the broker's /publish endpoint.

        Args:
            client: Shared httpx async client.
            request: The pipeline request to publish.

        The DAG is serialised as a list of stage IDs in topological order
        plus a list of edges, so that the broker can reconstruct the
        dependency structure without importing the DAG class.
        """
        topo_order = request.dag.topological_sort()
        edges = [
            {
                "source_id": e.source_id,
                "target_id": e.target_id,
                "latency_bound": e.latency_bound,
            }
            for e in request.dag.edges
        ]
        stages = [
            {
                "id": s.id,
                "stage_type": s.stage_type,
                "computational_demand": s.computational_demand,
                "output_data_rate": s.output_data_rate,
                "slice_requirement": s.slice_requirement,
                "data_sovereignty_domain": s.data_sovereignty_domain,
            }
            for s in request.dag.stages.values()
        ]

        payload = {
            "request_id": request.request_id,
            "pipeline_type": request.pipeline_type,
            "stage_ids": topo_order,
            "stages": stages,
            "edges": edges,
            "created_at": request.created_at,
            "metadata": request.metadata,
        }

        try:
            response = await client.post(
                f"{self.config.broker_url}/publish",
                json=payload,
                timeout=5.0,
            )
            response.raise_for_status()
            logger.debug(
                "Published request %s (type=%s) to broker.",
                request.request_id,
                request.pipeline_type,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to publish request %s: %s", request.request_id, exc
            )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Return summary statistics for the current or completed run.

        Returns:
            Dictionary with keys:
                - ``total_generated`` (int): Total requests generated so far.
                - ``by_type`` (dict[str, int]): Per-template request counts.
                - ``actual_rate`` (float): Observed generation rate (req/s),
                  or 0.0 if the generator has not started yet.
        """
        elapsed = (time.time() - self._run_start) if self._run_start else 0.0
        actual_rate = (self._total_generated / elapsed) if elapsed > 0 else 0.0
        return {
            "total_generated": self._total_generated,
            "by_type": dict(self._by_type),
            "actual_rate": actual_rate,
        }


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def load_config(path: str) -> WorkloadConfig:
    """Load a WorkloadConfig from a YAML file.

    Expected YAML structure::

        arrival_rate: 5.0
        duration_s: 60.0
        pipeline_mix:
          cqi_prediction: 0.5
          anomaly_detection: 0.3
          sensor_fusion: 0.2
        broker_url: "http://localhost:8080"
        seed: 42

    Args:
        path: Filesystem path to the YAML config file.

    Returns:
        A WorkloadConfig instance with validated parameters.

    Raises:
        FileNotFoundError: If the file does not exist.
        KeyError: If required keys are missing.
        ValueError: If pipeline_mix probabilities do not sum to 1.0.
    """
    with open(path) as fh:
        raw = yaml.safe_load(fh)

    return WorkloadConfig(
        arrival_rate=float(raw["arrival_rate"]),
        duration_s=float(raw["duration_s"]),
        pipeline_mix={str(k): float(v) for k, v in raw["pipeline_mix"].items()},
        broker_url=str(raw["broker_url"]),
        seed=int(raw.get("seed", 42)),
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Neural Pub/Sub workload generator.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--config",
        metavar="PATH",
        help="Path to a YAML workload config file.",
    )
    parser.add_argument(
        "--broker-url",
        default="http://localhost:8080",
        help="Broker base URL (default: http://localhost:8080).",
    )
    parser.add_argument(
        "--arrival-rate",
        type=float,
        default=1.0,
        dest="arrival_rate",
        help="Mean requests per second (default 1.0). Ignored when --config is used.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        dest="duration_s",
        help="Run duration in seconds (default 60). Ignored when --config is used.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed (default 42). Ignored when --config is used.",
    )
    parser.add_argument(
        "--result-file",
        default="",
        dest="result_file",
        help="Path for metrics CSV export at end of run. If empty, no export.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point for the workload generator."""
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()

    if args.config:
        cfg = load_config(args.config)
    else:
        metadata = {}
        if args.result_file:
            metadata["result_file"] = args.result_file
        cfg = WorkloadConfig(
            arrival_rate=args.arrival_rate,
            duration_s=args.duration_s,
            pipeline_mix={
                "cqi_prediction": 0.4,
                "anomaly_detection": 0.4,
                "sensor_fusion": 0.2,
            },
            broker_url=args.broker_url,
            seed=args.seed,
            metadata=metadata,
        )

    gen = WorkloadGenerator(cfg)
    asyncio.run(gen.run())


if __name__ == "__main__":
    main()
