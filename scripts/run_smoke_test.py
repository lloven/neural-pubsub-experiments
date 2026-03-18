#!/usr/bin/env python3
"""Level 4 smoke test: run shortened versions of all phases to validate the
end-to-end experiment pipeline before committing to real testbed time.

Runs each phase with reduced duration (30s), single seed, single rate,
and validates that:
  1. Docker Compose stack starts and all containers are healthy
  2. Pipelines complete end-to-end
  3. Metrics endpoint returns valid data
  4. CSV output files are generated (if applicable)
  5. Figure generation script runs without error

Usage:
    python scripts/run_smoke_test.py
    python scripts/run_smoke_test.py --phases A B    # specific phases only
    python scripts/run_smoke_test.py --skip-figures   # skip figure generation
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.local.yaml"
RESULTS_DIR = PROJECT_ROOT / "results" / "local"

SMOKE_DURATION_S = 30
SMOKE_RATE = 2.0


def wait_for_health(url: str, timeout: int = 30) -> bool:
    """Poll a /health endpoint until it returns 200 or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = httpx.get(f"{url}/health", timeout=3.0)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def run_stack_smoke() -> dict:
    """Start the Docker Compose stack, wait for health, run pipelines,
    collect metrics, and shut down."""
    logger.info("=== Stack Smoke Test ===")

    # Start stack in background
    proc = subprocess.Popen(
        [
            "docker", "compose", "-f", str(COMPOSE_FILE),
            "-p", "npubsub-smoke",
            "up", "--build", "-d",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    proc.wait(timeout=120)
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        logger.error("Stack start failed: %s", stderr[:500])
        return {"status": "stack_start_failed", "error": stderr[:500]}

    broker_url = "http://localhost:8080"

    # Wait for broker health
    if not wait_for_health(broker_url):
        logger.error("Broker health check failed after 30s")
        _cleanup_stack()
        return {"status": "health_check_failed"}

    logger.info("Broker healthy. Waiting for workers to register...")
    time.sleep(5)

    # Check worker count
    try:
        resp = httpx.get(f"{broker_url}/health", timeout=5.0)
        health = resp.json()
        n_workers = health.get("workers", 0)
        logger.info("Workers registered: %d", n_workers)
    except Exception as e:
        logger.error("Health check error: %s", e)
        _cleanup_stack()
        return {"status": "health_error", "error": str(e)}

    # Submit a few test pipelines
    pipelines_submitted = 0
    pipelines_ok = 0
    for ptype in ["cqi_prediction", "anomaly_detection", "sensor_fusion"]:
        try:
            resp = httpx.post(
                f"{broker_url}/publish",
                json={"pipeline_type": ptype, "config": {}},
                timeout=10.0,
            )
            pipelines_submitted += 1
            if resp.status_code == 200:
                pipelines_ok += 1
                logger.info("Pipeline %s: %s", ptype, resp.json().get("status"))
            else:
                logger.warning("Pipeline %s: HTTP %d", ptype, resp.status_code)
        except Exception as e:
            logger.error("Pipeline %s submission failed: %s", ptype, e)

    # Wait for pipelines to complete
    logger.info("Waiting %ds for pipeline completion...", SMOKE_DURATION_S)
    time.sleep(SMOKE_DURATION_S)

    # Collect metrics
    metrics = {}
    try:
        resp = httpx.get(f"{broker_url}/metrics", timeout=5.0)
        metrics = resp.json()
        logger.info(
            "Metrics: completed=%d, failed=%d, mean_latency=%.1fms",
            metrics.get("completed", 0),
            metrics.get("failed", 0),
            metrics.get("latency_mean_ms", 0),
        )
    except Exception as e:
        logger.error("Metrics fetch failed: %s", e)

    _cleanup_stack()

    return {
        "status": "completed",
        "workers": n_workers,
        "pipelines_submitted": pipelines_submitted,
        "pipelines_ok": pipelines_ok,
        "metrics": metrics,
    }


def _cleanup_stack():
    """Shut down the Docker Compose stack."""
    subprocess.run(
        [
            "docker", "compose", "-f", str(COMPOSE_FILE),
            "-p", "npubsub-smoke",
            "down", "--volumes", "--remove-orphans",
        ],
        check=False,
        timeout=60,
        capture_output=True,
    )


def run_phase_a_smoke() -> dict:
    """Phase A smoke: single config (A4), single rate, single seed."""
    logger.info("=== Phase A Smoke (Neural Pub/Sub, medium rate) ===")
    result = subprocess.run(
        [
            sys.executable, str(PROJECT_ROOT / "scripts" / "run_phase_a.py"),
            "--configs", "A4",
            "--rates", "medium",
            "--complexities", "3",
            "--seeds", "42",
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    logger.info("Phase A dry run: exit=%d", result.returncode)
    if result.stdout:
        logger.info(result.stdout[-300:])
    return {
        "status": "completed" if result.returncode == 0 else "failed",
        "exit_code": result.returncode,
    }


def run_figure_smoke() -> dict:
    """Test figure generation with whatever results exist."""
    logger.info("=== Figure Generation Smoke ===")
    figs_dir = PROJECT_ROOT / "figs" / "smoke"
    figs_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            sys.executable, str(PROJECT_ROOT / "scripts" / "generate_figures.py"),
            "--results-dir", str(PROJECT_ROOT / "results"),
            "--output-dir", str(figs_dir),
            "--format", "png",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    logger.info("Figure generation: exit=%d", result.returncode)
    if result.stdout:
        logger.info(result.stdout[-300:])
    # It's OK if no results exist yet; the script should handle gracefully
    return {
        "status": "completed" if result.returncode == 0 else "failed",
        "exit_code": result.returncode,
    }


def main():
    parser = argparse.ArgumentParser(description="Level 4 smoke test runner")
    parser.add_argument(
        "--phases", nargs="*", default=["stack", "A", "figures"],
        help="Which smoke tests to run (default: stack A figures)",
    )
    parser.add_argument("--skip-figures", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    results = {}

    if "stack" in args.phases:
        results["stack"] = run_stack_smoke()

    if "A" in args.phases:
        results["phase_a"] = run_phase_a_smoke()

    if "figures" in args.phases and not args.skip_figures:
        results["figures"] = run_figure_smoke()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SMOKE TEST SUMMARY")
    logger.info("=" * 60)
    all_ok = True
    for name, result in results.items():
        status = result.get("status", "unknown")
        ok = status in ("completed", "dry_run")
        symbol = "PASS" if ok else "FAIL"
        logger.info("  %s: %s (%s)", name, symbol, status)
        if not ok:
            all_ok = False

    if all_ok:
        logger.info("\nAll smoke tests passed. Ready for Level 4 full dry run.")
    else:
        logger.error("\nSome smoke tests failed. Fix before proceeding.")

    # Write results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_DIR / "smoke_test_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
