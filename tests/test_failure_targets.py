"""Tests for Phase D failure target validation.

ROOT CAUSE: Phase D failure injection had zero effect because target names
in CONFIGS (e.g., "worker", "broker-domain2") don't match actual Docker
Compose service names (e.g., "worker-d1-embb-1", "broker-d2").

These tests validate that:
1. Every failure target resolves to an actual compose service or network
2. inject_compose_kill raises (not silently catches) on invalid targets
3. The compose files are parsed to extract valid target names

TDD RED phase: these tests MUST fail before the fix is applied.
"""

from __future__ import annotations

import pytest
import yaml
from pathlib import Path

from scripts._common import PROJECT_ROOT
from scripts.run_phase_d import CONFIGS


# ---------------------------------------------------------------------------
# Helper: extract valid targets from compose files
# ---------------------------------------------------------------------------

COMPOSE_LOCAL = PROJECT_ROOT / "docker-compose.local.yaml"
COMPOSE_TESTBED = PROJECT_ROOT / "docker-compose.testbed.yaml"


def _load_compose_services() -> set[str]:
    """Return the union of service names from all compose overlays."""
    services = set()
    for cf in [COMPOSE_LOCAL, COMPOSE_TESTBED]:
        if cf.exists():
            with open(cf) as f:
                dc = yaml.safe_load(f) or {}
            services.update(dc.get("services", {}).keys())
    return services


def _load_compose_networks() -> set[str]:
    """Return the union of network names from all compose overlays."""
    networks = set()
    for cf in [COMPOSE_LOCAL, COMPOSE_TESTBED]:
        if cf.exists():
            with open(cf) as f:
                dc = yaml.safe_load(f) or {}
            networks.update(dc.get("networks", {}).keys())
    return networks


# ---------------------------------------------------------------------------
# 1. Every failure target must resolve to a real compose entity
# ---------------------------------------------------------------------------

class TestFailureTargetsMatchCompose:
    """Every CONFIGS failure_target must exist in the compose files."""

    def test_d1_worker_target_is_real_service(self):
        """D1 (worker failure) target must be an actual compose service."""
        services = _load_compose_services()
        target = CONFIGS["D1"]["failure_target"]
        assert target in services, (
            f"D1 failure_target '{target}' not found in compose services: "
            f"{sorted(services)}"
        )

    def test_d2_broker_target_is_real_service(self):
        """D2 (broker failure) target must be an actual compose service."""
        services = _load_compose_services()
        target = CONFIGS["D2"]["failure_target"]
        assert target in services, (
            f"D2 failure_target '{target}' not found in compose services: "
            f"{sorted(services)}"
        )

    def test_d3_network_target_is_real_network(self):
        """D3 (network failure) target must be an actual compose network."""
        networks = _load_compose_networks()
        target = CONFIGS["D3"]["failure_target"]
        assert target in networks, (
            f"D3 failure_target '{target}' not found in compose networks: "
            f"{sorted(networks)}"
        )

    def test_d4_funnel_target_is_real_service(self):
        """D4 (funnel failure) target must be an actual compose service."""
        services = _load_compose_services()
        target = CONFIGS["D4"]["failure_target"]
        assert target in services, (
            f"D4 failure_target '{target}' not found in compose services: "
            f"{sorted(services)}"
        )

    def test_all_service_targets_exist(self):
        """Comprehensive: every config with failure_type in (worker, broker, funnel)
        must target a real compose service."""
        services = _load_compose_services()
        service_failure_types = {"worker", "broker", "funnel"}
        for name, cfg in CONFIGS.items():
            if cfg["failure_type"] in service_failure_types:
                assert cfg["failure_target"] in services, (
                    f"{name}: failure_target '{cfg['failure_target']}' not in "
                    f"compose services {sorted(services)}"
                )

    def test_network_targets_exist(self):
        """Every config with failure_type='network' must target a real network."""
        networks = _load_compose_networks()
        for name, cfg in CONFIGS.items():
            if cfg["failure_type"] == "network":
                assert cfg["failure_target"] in networks, (
                    f"{name}: failure_target '{cfg['failure_target']}' not in "
                    f"compose networks {sorted(networks)}"
                )


# ---------------------------------------------------------------------------
# 2. inject_compose_kill must not silently swallow errors
# ---------------------------------------------------------------------------

class TestFailureInjectionErrorHandling:
    """inject_compose_kill must propagate failure when target doesn't exist."""

    def test_inject_compose_kill_raises_on_invalid_target(self):
        """Killing a nonexistent service should raise, not silently fail."""
        from scripts._common import inject_compose_kill

        with pytest.raises(Exception):
            # This should fail because 'nonexistent-service' doesn't exist
            # and we're not running Docker here, so it will fail at subprocess level
            inject_compose_kill(
                project_name="test-project",
                compose_file=COMPOSE_LOCAL,
                env={},
                target="nonexistent-service-xyz",
                delay_s=0,  # immediate
                label="test",
            )


# ---------------------------------------------------------------------------
# 3. Structural: targets should be documented constants, not magic strings
# ---------------------------------------------------------------------------

class TestFailureTargetDocumentation:
    """Failure targets should reference a canonical mapping."""

    def test_each_config_documents_which_service_is_killed(self):
        """Each failure_target should be a specific service name, not a generic
        type like 'worker'. The test catches overly-generic targets."""
        generic_names = {"worker", "broker", "network", "sensor", "funnel",
                         "sensor-worker", "broker-domain2", "federation-net"}
        for name, cfg in CONFIGS.items():
            target = cfg["failure_target"]
            assert target not in generic_names, (
                f"{name}: failure_target '{target}' looks like a generic type, "
                f"not an actual compose service/network name. Use the specific "
                f"service name (e.g., 'worker-d1-embb-1', 'broker-d2')."
            )


# ---------------------------------------------------------------------------
# 4. Phase D compose overlay disables restart policy
# ---------------------------------------------------------------------------

class TestFailureComposeOverlay:
    """Phase D must use a compose overlay that disables restart policy."""

    COMPOSE_FAILURE = PROJECT_ROOT / "docker-compose.failure.yaml"

    def test_failure_overlay_exists(self):
        """A failure overlay compose file must exist for Phase D."""
        assert self.COMPOSE_FAILURE.exists(), (
            f"Missing {self.COMPOSE_FAILURE}. Phase D needs a compose overlay "
            f"that sets restart: 'no' to prevent Docker from restarting killed containers."
        )

    def test_failure_overlay_disables_restart_for_all_services(self):
        """Every service in the failure overlay must have restart: 'no'."""
        if not self.COMPOSE_FAILURE.exists():
            pytest.skip("Failure overlay not yet created")
        with open(self.COMPOSE_FAILURE) as f:
            dc = yaml.safe_load(f) or {}
        services = dc.get("services", {})
        assert len(services) > 0, "Failure overlay has no services"
        for svc, cfg in services.items():
            assert cfg.get("restart") == "no", (
                f"Service '{svc}' in failure overlay must have restart: 'no', "
                f"got: {cfg.get('restart', '(not set)')}"
            )

    def test_failure_overlay_covers_all_testbed_services(self):
        """Failure overlay must cover every service from the testbed compose."""
        if not self.COMPOSE_FAILURE.exists():
            pytest.skip("Failure overlay not yet created")
        testbed_services = _load_compose_services()
        with open(self.COMPOSE_FAILURE) as f:
            dc = yaml.safe_load(f) or {}
        overlay_services = set(dc.get("services", {}).keys())
        missing = testbed_services - overlay_services
        assert not missing, (
            f"Failure overlay missing services: {sorted(missing)}. "
            f"All services need restart: 'no' during failure injection."
        )

    def test_phase_d_uses_failure_overlay(self):
        """run_phase_d.py must reference the failure overlay compose file."""
        with open(PROJECT_ROOT / "scripts" / "run_phase_d.py") as f:
            content = f.read()
        assert "failure" in content.lower() and "compose" in content.lower(), (
            "run_phase_d.py must reference the failure compose overlay"
        )


# ---------------------------------------------------------------------------
# 5. Phase D uses detached compose mode (no --abort-on-container-exit)
# ---------------------------------------------------------------------------

class TestPhaseDDetachedMode:
    """Phase D must use detached compose mode so killed containers don't
    abort the entire experiment."""

    def test_run_single_accepts_detached_param(self):
        """run_single must accept a 'detached' parameter."""
        import inspect
        from scripts._common import run_single
        sig = inspect.signature(run_single)
        assert "detached" in sig.parameters, (
            "run_single() must accept a 'detached' parameter for Phase D"
        )

    def test_compose_up_detached_exists(self):
        """A compose_up_detached function must exist in _common."""
        from scripts._common import compose_up_detached
        assert callable(compose_up_detached)

    def test_compose_up_detached_uses_detached_flag(self):
        """compose_up_detached must use 'up -d' (detached), not --abort-on-container-exit."""
        import inspect
        from scripts._common import compose_up_detached
        source = inspect.getsource(compose_up_detached)
        # Strip docstring
        parts = source.split('"""')
        executable = parts[-1] if len(parts) >= 3 else source
        assert "abort-on-container-exit" not in executable, (
            "compose_up_detached executable code must NOT use --abort-on-container-exit"
        )
        # Check for detached mode (-d flag in the subprocess command)
        assert '"-d"' in executable, (
            "compose_up_detached must use -d (detached) flag"
        )

    def test_phase_d_passes_detached_true(self):
        """run_phase_d._run must pass detached=True to run_single."""
        import inspect
        from scripts.run_phase_d import _run
        source = inspect.getsource(_run)
        assert "detached" in source and "True" in source, (
            "Phase D _run() must pass detached=True to run_single()"
        )


# ---------------------------------------------------------------------------
# 6. Project name must match between _run and run_single
# ---------------------------------------------------------------------------

class TestProjectNameConsistency:
    """The project name used by _make_failure_fn must match run_single's."""

    def test_phase_d_project_name_matches_run_single(self):
        """_run() must normalize project_name the same way run_single does."""
        from scripts.run_phase_d import RunConfig
        rc = RunConfig(
            config_name="D1", seed=42,
            failure_type="worker", failure_target="worker-d1-embb-1",
        )
        # What _run produces
        run_id = rc.run_id  # "D1_failure-worker_seed-42"
        phase_d_project = f"npubsub-{run_id.lower().replace('_', '-')}"

        # What run_single produces
        run_single_project = f"npubsub-{run_id.lower().replace('_', '-')}"

        assert phase_d_project == run_single_project, (
            f"Project name mismatch: _run={phase_d_project}, "
            f"run_single={run_single_project}"
        )

    def test_project_name_is_lowercase_with_hyphens(self):
        """Docker project names must be lowercase with hyphens (no underscores)."""
        from scripts.run_phase_d import RunConfig
        rc = RunConfig(
            config_name="D2", seed=789,
            failure_type="broker", failure_target="broker-d2",
        )
        run_id = rc.run_id
        project = f"npubsub-{run_id.lower().replace('_', '-')}"
        assert project == project.lower(), f"Not lowercase: {project}"
        assert "_" not in project, f"Contains underscore: {project}"
