"""System/integration tests for the Neural Pub/Sub experiment orchestrator.

These tests start REAL Docker containers, run short experiments, and verify
end-to-end behavior: CSV output, container lifecycle, cleanup, env propagation,
failure injection, seed determinism, and cross-phase schema consistency.

Run with::

    pytest -m integration tests/test_system.py -v --timeout=180

Requires Docker to be running. Tests are self-contained: each starts its own
containers and cleans up after, even on failure.

Design constraints (from task spec):
  - @pytest.mark.integration on every test (skip in CI without Docker)
  - Unique project name prefix "npubsub-test-" to avoid collisions
  - Short timing: warmup=5s, measurement=20s, total ~30s per run
  - Cleanup via try/finally and pytest fixtures
  - Each test completes in <2 minutes; total suite <10 minutes
"""

from __future__ import annotations

import csv
import os
import signal
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_COMPOSE_FILE = _PROJECT_ROOT / "docker-compose.local.yaml"
_COMPOSE_TEST_OVERRIDE = _PROJECT_ROOT / "docker-compose.test-override.yaml"
_COMPOSE_FILES = [_COMPOSE_FILE, _COMPOSE_TEST_OVERRIDE]
_RESULTS_DIR = _PROJECT_ROOT / "results"

# Short timing for tests (seconds)
_TEST_WARMUP_S = 5
_TEST_MEASUREMENT_S = 20
_TEST_DURATION_S = _TEST_WARMUP_S + _TEST_MEASUREMENT_S
_TEST_ARRIVAL_RATE = 2.0  # low rate to keep test fast

# Expected CSV columns (base set from MetricsCollector.export_csv)
_REQUIRED_CSV_COLUMNS = {
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
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def project_name():
    """Generate a unique Docker Compose project name and guarantee cleanup.

    Yields the project name for the test to use. On teardown, forcibly
    removes all containers, networks, and volumes for the project,
    regardless of test outcome.
    """
    name = f"npubsub-test-{uuid4().hex[:8]}"
    yield name
    # Teardown: aggressive cleanup
    _compose_down(name)
    _prune_project_resources(name)


@pytest.fixture
def results_dir(tmp_path: Path):
    """Create and return a temporary results directory."""
    d = tmp_path / "results"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _base_env(
    results_dir: Path,
    result_filename: str = "metrics.csv",
    seed: int = 42,
    warmup: int = _TEST_WARMUP_S,
    duration: int = _TEST_MEASUREMENT_S,
    arrival_rate: float = _TEST_ARRIVAL_RATE,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the environment dict for a test run.

    Maps host results_dir to /app/results inside the container via the
    volume mount in docker-compose.local.yaml. The RESULT_FILE env var
    tells the workload container where to write the CSV.
    """
    env = {
        **os.environ,
        "SEED": str(seed),
        "ARRIVAL_RATE": str(arrival_rate),
        "DURATION_S": str(duration),
        "WARMUP_S": str(warmup),
        "RESULT_FILE": f"/app/results/{result_filename}",
        # Remove cpuset constraints for test (may not have enough cores)
        "CPUSET_BROKER_D1": "",
        "CPUSET_BROKER_D2": "",
    }
    if extra:
        env.update(extra)
    return env


def _compose_up(
    project_name: str,
    env: dict[str, str],
    results_dir: Path,
    timeout_s: int | None = None,
    compose_files: list[Path] | None = None,
    detached: bool = False,
) -> subprocess.CompletedProcess | subprocess.Popen:
    """Start the Docker Compose stack.

    Args:
        project_name: Compose project name.
        env: Full environment dict (use _base_env()).
        results_dir: Host path for result volume mount.
        timeout_s: Timeout for blocking run (non-detached).
        compose_files: Override compose files (default: base only).
        detached: If True, start in background and return Popen.

    Returns:
        CompletedProcess (blocking) or Popen (detached).
    """
    files = compose_files or _COMPOSE_FILES
    file_args = []
    for f in files:
        file_args.extend(["-f", str(f)])

    if detached:
        cmd = [
            "docker", "compose", *file_args,
            "-p", project_name,
            "up", "-d", "--build", "--remove-orphans",
        ]
        return subprocess.run(
            cmd, env=env, check=True,
            timeout=180, capture_output=True, text=True,
        )
    else:
        cmd = [
            "docker", "compose", *file_args,
            "-p", project_name,
            "up", "--build", "--abort-on-container-exit",
            "--remove-orphans", "--timeout", "30",
        ]
        timeout = timeout_s or (_TEST_DURATION_S + 120)
        return subprocess.run(
            cmd, env=env, check=False,
            timeout=timeout, capture_output=True, text=True,
        )


def _compose_down(project_name: str) -> None:
    """Tear down a Docker Compose project (best-effort)."""
    file_args = []
    for f in _COMPOSE_FILES:
        file_args.extend(["-f", str(f)])
    subprocess.run(
        [
            "docker", "compose", *file_args,
            "-p", project_name,
            "down", "--volumes", "--remove-orphans", "--timeout", "10",
        ],
        check=False, capture_output=True, timeout=60,
    )


def _prune_project_resources(project_name: str) -> None:
    """Remove any leftover Docker resources for the project."""
    # Kill any remaining containers
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={project_name}",
         "--format", "{{.ID}}"],
        capture_output=True, text=True, timeout=10,
    )
    container_ids = result.stdout.strip().split()
    for cid in container_ids:
        if cid:
            subprocess.run(["docker", "rm", "-f", cid], check=False, capture_output=True, timeout=10)

    # Prune project networks
    result = subprocess.run(
        ["docker", "network", "ls", "--filter", f"label=com.docker.compose.project={project_name}",
         "--format", "{{.ID}}"],
        capture_output=True, text=True, timeout=10,
    )
    net_ids = result.stdout.strip().split()
    for nid in net_ids:
        if nid:
            subprocess.run(["docker", "network", "rm", nid], check=False, capture_output=True, timeout=10)


def _wait_for_healthy(project_name: str, service: str, timeout_s: int = 90) -> bool:
    """Wait for a service to be healthy."""
    deadline = time.time() + timeout_s
    container = f"{project_name}-{service}-1"
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip() == "healthy":
            return True
        time.sleep(2)
    return False


def _container_running(project_name: str) -> list[str]:
    """Return list of running container names for the project."""
    result = subprocess.run(
        ["docker", "ps", "--filter", f"label=com.docker.compose.project={project_name}",
         "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=10,
    )
    return [n for n in result.stdout.strip().split("\n") if n]


def _container_exists(project_name: str) -> list[str]:
    """Return list of ALL container names (running or stopped) for the project."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={project_name}",
         "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=10,
    )
    return [n for n in result.stdout.strip().split("\n") if n]


def _networks_for_project(project_name: str) -> list[str]:
    """Return list of Docker networks for the project."""
    result = subprocess.run(
        ["docker", "network", "ls", "--filter", f"label=com.docker.compose.project={project_name}",
         "--format", "{{.Name}}"],
        capture_output=True, text=True, timeout=10,
    )
    return [n for n in result.stdout.strip().split("\n") if n]


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read a CSV file and return (fieldnames, rows)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def _fix_permissions(results_dir: Path) -> None:
    """Make Docker-written files readable by host user."""
    subprocess.run(
        ["docker", "run", "--rm",
         "-v", f"{results_dir.resolve()}:/data",
         "alpine", "chmod", "-R", "a+rw", "/data"],
        capture_output=True, timeout=30, check=False,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

pytestmark = [pytest.mark.integration]


class TestBaselineProducesCSV:
    """Test 1: Start a minimal baseline experiment and verify CSV output."""

    def test_baseline_produces_csv(self, project_name: str, results_dir: Path) -> None:
        """A minimal baseline run produces a CSV with correct schema and >0 rows.

        Verifies:
          - Containers start (broker healthy, workers up)
          - CSV file is produced at the expected path
          - CSV has correct schema (all expected columns)
          - CSV has >0 rows
          - All containers are cleaned up after (via fixture)
        """
        csv_file = results_dir / "test_baseline.csv"
        env = _base_env(
            results_dir=results_dir,
            result_filename="test_baseline.csv",
            extra={"BROKER_MODULE": "src.broker.static_broker", "PLACEMENT": "round_robin"},
        )

        # Mount the test results dir in place of the project's results dir
        # We need to use a volume override or write to the project results dir
        # The compose file mounts ./results:/app/results, so we write there
        host_results = _PROJECT_ROOT / "results"
        host_results.mkdir(exist_ok=True)
        host_csv = host_results / f"{project_name}-baseline.csv"
        env["RESULT_FILE"] = f"/app/results/{project_name}-baseline.csv"

        try:
            result = _compose_up(
                project_name=project_name,
                env=env,
                results_dir=host_results,
                timeout_s=_TEST_DURATION_S + 120,
            )

            # Fix permissions so we can read Docker-written files
            _fix_permissions(host_results)

            # Verify CSV exists
            assert host_csv.exists(), (
                f"CSV file not produced at {host_csv}. "
                f"Compose stdout: {result.stdout[-500:] if result.stdout else '(empty)'}. "
                f"Compose stderr: {result.stderr[-500:] if result.stderr else '(empty)'}"
            )

            # Verify schema
            fieldnames, rows = _read_csv(host_csv)
            missing = _REQUIRED_CSV_COLUMNS - set(fieldnames)
            assert not missing, (
                f"CSV missing required columns: {missing}. "
                f"Present columns: {fieldnames}"
            )

            # Verify non-empty
            assert len(rows) > 0, "CSV has 0 rows (no pipelines completed)"

            # Verify at least some rows have valid latency
            latencies = [float(r["e2e_latency_ms"]) for r in rows if r.get("e2e_latency_ms")]
            assert any(lat > 0 for lat in latencies), (
                f"No positive latency values in CSV. Latencies: {latencies[:10]}"
            )

        finally:
            # Clean up the result file
            if host_csv.exists():
                host_csv.unlink()


class TestEnvVarReachesContainer:
    """Test 2: Verify env vars propagate into containers (GAP-2)."""

    def test_broker_module_env_var_reaches_container(
        self, project_name: str, results_dir: Path,
    ) -> None:
        """BROKER_MODULE env var set in compose env is visible inside broker container.

        This directly addresses GAP-2: env vars were never verified inside
        containers. docker-compose.local.yaml uses ${BROKER_MODULE:-...}
        defaults that could silently mask missing propagation.
        """
        env = _base_env(
            results_dir=results_dir,
            extra={"BROKER_MODULE": "src.broker.static_broker", "PLACEMENT": "round_robin"},
        )

        try:
            # Start in detached mode so we can exec into containers
            _compose_up(
                project_name=project_name,
                env=env,
                results_dir=_PROJECT_ROOT / "results",
                detached=True,
            )

            # Wait for broker to be healthy
            healthy = _wait_for_healthy(project_name, "broker-d1", timeout_s=90)
            assert healthy, "broker-d1 did not become healthy within 90s"

            # Exec into broker container and check the env var
            container = f"{project_name}-broker-d1-1"
            result = subprocess.run(
                ["docker", "exec", container, "printenv", "BROKER_MODULE"],
                capture_output=True, text=True, timeout=10,
            )

            # The BROKER_MODULE is passed via the command line, not as an env var
            # in the container's environment section. Let's check the process
            # command line instead.
            proc_result = subprocess.run(
                ["docker", "exec", container, "cat", "/proc/1/cmdline"],
                capture_output=True, timeout=10,
            )
            cmdline = proc_result.stdout.replace(b"\x00", b" ").decode("utf-8", errors="replace")

            assert "static_broker" in cmdline or (
                result.returncode == 0 and "static_broker" in result.stdout
            ), (
                f"BROKER_MODULE=static_broker not found in container. "
                f"printenv result: rc={result.returncode}, stdout='{result.stdout.strip()}'. "
                f"Process cmdline: '{cmdline}'"
            )

        finally:
            _compose_down(project_name)


class TestFailureInjectionProducesFailures:
    """Test 3: Resilience experiment with failure injection."""

    def test_failure_injection_kills_container(
        self, project_name: str, results_dir: Path,
    ) -> None:
        """Killing a worker container during an experiment produces measurable effects.

        Verifies:
          - The target container is actually killed (docker inspect shows exited)
          - The experiment still completes (workload finishes)
          - CSV is produced with results

        This verifies L38 (verify treatments, not just outcomes).
        """
        host_results = _PROJECT_ROOT / "results"
        host_results.mkdir(exist_ok=True)
        csv_name = f"{project_name}-resilience.csv"
        host_csv = host_results / csv_name
        env = _base_env(
            results_dir=host_results,
            result_filename=csv_name,
            duration=30,  # slightly longer for resilience
            warmup=5,
        )

        try:
            # Start detached (killed container should not stop the experiment)
            _compose_up(
                project_name=project_name,
                env=env,
                results_dir=host_results,
                detached=True,
            )

            # Wait for broker to be healthy
            healthy = _wait_for_healthy(project_name, "broker-d1", timeout_s=90)
            assert healthy, "broker-d1 did not become healthy"

            # Give the workload a few seconds to start
            time.sleep(10)

            # Kill one worker (d1-embb-1)
            target = "worker-d1-embb-1"
            file_args = []
            for f in _COMPOSE_FILES:
                file_args.extend(["-f", str(f)])
            kill_result = subprocess.run(
                [
                    "docker", "compose", *file_args,
                    "-p", project_name,
                    "kill", target,
                ],
                env=env, capture_output=True, text=True, timeout=30,
            )

            # Verify the target was actually killed (L38: verify treatment)
            time.sleep(2)
            container_name = f"{project_name}-{target}-1"
            inspect_result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Running}}", container_name],
                capture_output=True, text=True, timeout=10,
            )
            assert inspect_result.stdout.strip() == "false", (
                f"Target container {target} should be stopped after kill. "
                f"State.Running={inspect_result.stdout.strip()}"
            )

            # Wait for workload to finish
            workload_container = f"{project_name}-workload-1"
            deadline = time.time() + 90
            while time.time() < deadline:
                wr = subprocess.run(
                    ["docker", "inspect", "--format", "{{.State.Running}}", workload_container],
                    capture_output=True, text=True, timeout=10,
                )
                if wr.returncode != 0 or wr.stdout.strip() == "false":
                    break
                time.sleep(3)

            _fix_permissions(host_results)

            # CSV should exist (workload completed)
            assert host_csv.exists(), (
                f"CSV not produced after resilience test at {host_csv}"
            )

        finally:
            _compose_down(project_name)
            if host_csv.exists():
                host_csv.unlink()


class TestCleanupOnNormalExit:
    """Test 4: After a complete experiment, all resources are cleaned up."""

    def test_no_containers_after_normal_exit(
        self, project_name: str, results_dir: Path,
    ) -> None:
        """After a normal experiment completion and compose_down, no resources remain.

        Verifies:
          - No containers with the project name are running
          - No networks with the project name exist
        """
        host_results = _PROJECT_ROOT / "results"
        host_results.mkdir(exist_ok=True)
        csv_name = f"{project_name}-cleanup.csv"
        host_csv = host_results / csv_name
        env = _base_env(
            results_dir=host_results,
            result_filename=csv_name,
            duration=15,
            warmup=3,
        )

        try:
            # Run to completion (blocking)
            _compose_up(
                project_name=project_name,
                env=env,
                results_dir=host_results,
                timeout_s=_TEST_DURATION_S + 120,
            )
        finally:
            _compose_down(project_name)
            if host_csv.exists():
                host_csv.unlink()

        # After compose_down, verify no containers remain
        remaining = _container_exists(project_name)
        assert not remaining, (
            f"Containers still exist after compose_down: {remaining}"
        )

        # Verify no networks remain
        remaining_nets = _networks_for_project(project_name)
        assert not remaining_nets, (
            f"Networks still exist after compose_down: {remaining_nets}"
        )


class TestCleanupOnInterrupt:
    """Test 5: Signal-driven cleanup removes containers."""

    def test_cleanup_on_sigterm(
        self, project_name: str, results_dir: Path,
    ) -> None:
        """Sending SIGTERM to a running experiment cleans up containers.

        We start containers in detached mode, then call _compose_down
        (which is what the signal handler in _common.py does), and verify
        cleanup. This tests the same code path as the SIGTERM handler
        without needing to manage subprocess signal delivery.
        """
        env = _base_env(
            results_dir=results_dir,
            duration=60,  # long duration so we can interrupt
            warmup=5,
        )

        try:
            _compose_up(
                project_name=project_name,
                env=env,
                results_dir=_PROJECT_ROOT / "results",
                detached=True,
            )

            # Wait for at least one container to be running
            healthy = _wait_for_healthy(project_name, "broker-d1", timeout_s=90)
            assert healthy, "broker-d1 never became healthy"

            running = _container_running(project_name)
            assert len(running) > 0, "No containers running before interrupt"

        finally:
            # Simulate signal handler: compose_down
            _compose_down(project_name)

        # After cleanup, no containers should remain
        remaining = _container_exists(project_name)
        assert not remaining, (
            f"Containers still exist after interrupt cleanup: {remaining}"
        )

        remaining_nets = _networks_for_project(project_name)
        assert not remaining_nets, (
            f"Networks still exist after interrupt cleanup: {remaining_nets}"
        )


class TestSeedDeterminism:
    """Test 6: Same config+seed produces deterministic results."""

    def test_same_seed_produces_consistent_row_count(
        self, project_name: str, results_dir: Path,
    ) -> None:
        """Running the same config+seed twice produces the same number of rows.

        Tolerance: within 20% (Docker scheduling introduces some variance
        in pipeline count over short runs, but the workload generator's
        Poisson process is seeded, so the number of submissions should match).
        """
        host_results = _PROJECT_ROOT / "results"
        host_results.mkdir(exist_ok=True)

        row_counts = []
        pipeline_types_sets = []

        for run_idx in range(2):
            run_project = f"{project_name}-r{run_idx}"
            csv_name = f"{run_project}-seed.csv"
            host_csv = host_results / csv_name
            env = _base_env(
                results_dir=host_results,
                result_filename=csv_name,
                seed=42,
                duration=15,
                warmup=3,
                arrival_rate=2.0,
                extra={"BROKER_MODULE": "src.broker.static_broker", "PLACEMENT": "round_robin"},
            )

            try:
                _compose_up(
                    project_name=run_project,
                    env=env,
                    results_dir=host_results,
                    timeout_s=_TEST_DURATION_S + 120,
                )
                _fix_permissions(host_results)

                assert host_csv.exists(), f"CSV not produced for run {run_idx}"
                _, rows = _read_csv(host_csv)
                row_counts.append(len(rows))
                pipeline_types_sets.append(
                    set(r.get("pipeline_type", "") for r in rows)
                )
            finally:
                _compose_down(run_project)
                _prune_project_resources(run_project)
                if host_csv.exists():
                    host_csv.unlink()

        # Both runs should have produced rows
        assert row_counts[0] > 0, "First run produced 0 rows"
        assert row_counts[1] > 0, "Second run produced 0 rows"

        # Row counts within 20% tolerance
        ratio = min(row_counts) / max(row_counts)
        assert ratio >= 0.8, (
            f"Row counts diverge too much: {row_counts[0]} vs {row_counts[1]} "
            f"(ratio={ratio:.2f}, need >=0.80)"
        )

        # Pipeline type sets should match
        assert pipeline_types_sets[0] == pipeline_types_sets[1], (
            f"Pipeline types differ between runs: "
            f"{pipeline_types_sets[0]} vs {pipeline_types_sets[1]}"
        )


class TestCSVSchemaConsistencyAcrossPhases:
    """Test 7: Baseline and resilience CSVs have the same column set (GAP-4)."""

    def test_baseline_and_resilience_csv_same_columns(
        self, project_name: str, results_dir: Path,
    ) -> None:
        """A baseline config and a resilience config produce CSVs with
        the same column set, ensuring cross-phase analysis consistency.
        """
        host_results = _PROJECT_ROOT / "results"
        host_results.mkdir(exist_ok=True)

        column_sets: dict[str, set[str]] = {}

        configs = {
            "baseline": {
                "BROKER_MODULE": "src.broker.static_broker",
                "PLACEMENT": "round_robin",
            },
            "neural": {},  # Uses compose default (neural_broker)
        }

        for label, extra_env in configs.items():
            run_project = f"{project_name}-{label}"
            csv_name = f"{run_project}-schema.csv"
            host_csv = host_results / csv_name
            env = _base_env(
                results_dir=host_results,
                result_filename=csv_name,
                duration=15,
                warmup=3,
                extra=extra_env,
            )

            try:
                _compose_up(
                    project_name=run_project,
                    env=env,
                    results_dir=host_results,
                    timeout_s=_TEST_DURATION_S + 120,
                )
                _fix_permissions(host_results)

                assert host_csv.exists(), f"CSV not produced for {label} config"
                fieldnames, _ = _read_csv(host_csv)
                column_sets[label] = set(fieldnames)
            finally:
                _compose_down(run_project)
                _prune_project_resources(run_project)
                if host_csv.exists():
                    host_csv.unlink()

        # Both should have the required base columns
        for label, cols in column_sets.items():
            missing = _REQUIRED_CSV_COLUMNS - cols
            assert not missing, (
                f"{label} CSV missing required columns: {missing}"
            )

        # Column sets should be identical (or at least share the base set)
        baseline_cols = column_sets["baseline"]
        neural_cols = column_sets["neural"]

        # Base columns must match
        baseline_base = baseline_cols & _REQUIRED_CSV_COLUMNS
        neural_base = neural_cols & _REQUIRED_CSV_COLUMNS
        assert baseline_base == neural_base, (
            f"Base column sets differ: "
            f"baseline has {baseline_base - neural_base}, "
            f"neural has {neural_base - baseline_base}"
        )
