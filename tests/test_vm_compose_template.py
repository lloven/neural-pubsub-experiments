"""Tests for deploy/docker-compose.vm.yaml broker module templating.

The VM compose file must support both neural_broker and static_broker
via the BROKER_MODULE env var. Neural-broker-specific CLI flags must
not appear in the entrypoint (they should be in environment block).
"""

from pathlib import Path

import yaml
import pytest

VM_COMPOSE = Path(__file__).resolve().parent.parent / "deploy" / "docker-compose.vm.yaml"


@pytest.fixture
def compose_config():
    """Load the VM compose file as a dict."""
    with open(VM_COMPOSE) as f:
        return yaml.safe_load(f)


def test_broker_entrypoint_uses_broker_module_template(compose_config):
    """Broker entrypoint must use ${BROKER_MODULE} not a hardcoded module."""
    broker = compose_config["services"]["broker"]
    entrypoint = broker.get("entrypoint", [])
    entrypoint_str = " ".join(str(e) for e in entrypoint)
    assert "BROKER_MODULE" in entrypoint_str or "broker_module" in entrypoint_str.lower(), (
        f"Broker entrypoint must use ${{BROKER_MODULE}} template, got: {entrypoint}"
    )
    assert "src.broker.neural_broker" not in entrypoint_str or "BROKER_MODULE" in entrypoint_str, (
        "Broker module must not be hardcoded without a template fallback"
    )


def test_no_neural_broker_specific_flags_in_entrypoint(compose_config):
    """Neural-broker-only flags must be in environment, not entrypoint."""
    broker = compose_config["services"]["broker"]
    entrypoint = broker.get("entrypoint", [])
    entrypoint_str = " ".join(str(e) for e in entrypoint)

    neural_only_flags = ["--placement-mode", "--summary-interval", "--wan-cost"]
    for flag in neural_only_flags:
        assert flag not in entrypoint_str, (
            f"Neural-broker-only flag '{flag}' must be in environment block, "
            f"not entrypoint (breaks static broker)"
        )


def test_broker_environment_has_placement_vars(compose_config):
    """Broker environment must include PLACEMENT_MODE and PLACEMENT for both broker types."""
    broker = compose_config["services"]["broker"]
    env_list = broker.get("environment", [])
    env_str = " ".join(str(e) for e in env_list)

    assert "PLACEMENT_MODE" in env_str, "Missing PLACEMENT_MODE in broker environment"
    assert "PLACEMENT" in env_str, "Missing PLACEMENT in broker environment"
    assert "GOVERNANCE_ENABLED" in env_str, "Missing GOVERNANCE_ENABLED in broker environment"


def test_broker_environment_has_broker_module(compose_config):
    """Broker environment must include BROKER_MODULE so workers/workload know the broker type."""
    broker = compose_config["services"]["broker"]
    env_list = broker.get("environment", [])
    env_str = " ".join(str(e) for e in env_list)

    assert "BROKER_MODULE" in env_str, "Missing BROKER_MODULE in broker environment"


def test_shared_flags_in_entrypoint(compose_config):
    """Entrypoint must contain flags shared by both brokers: --domain, --port, --peers."""
    broker = compose_config["services"]["broker"]
    entrypoint = broker.get("entrypoint", [])
    entrypoint_str = " ".join(str(e) for e in entrypoint)

    for flag in ["--domain", "--port", "--peers"]:
        assert flag in entrypoint_str, (
            f"Shared flag '{flag}' must be in entrypoint (used by both brokers)"
        )
