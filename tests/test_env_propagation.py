"""Integration tests for env var propagation: config resolution -> compose defaults.

GAP-2 from test-completeness-integration.md: Env vars were never verified
inside containers. docker-compose.local.yaml uses defaults like
${BROKER_MODULE:-src.broker.neural_broker} that could silently mask
missing propagation, causing S1/S2 (static broker) to actually run the
neural broker.

These tests verify that resolve_config() produces the correct env vars for
each strategy, and cross-reference against the compose file's defaults to
detect silent masking.

No Docker required — tests parse config and compose files.
"""

import re
from pathlib import Path

import pytest
import yaml

from scripts._common import (
    COMPOSE_FILE,
    _CONFIG_TABLE,
    resolve_config,
    TRANSPORTS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COMPOSE_PATH = COMPOSE_FILE


def _parse_compose_defaults(compose_path: Path) -> dict[str, str]:
    """Extract env var defaults from compose file's ${VAR:-default} patterns.

    Scans for patterns like ${BROKER_MODULE:-src.broker.neural_broker} in the
    'command' and 'environment' sections and returns a dict of var -> default.
    """
    text = compose_path.read_text()
    # Match ${VAR:-default} patterns
    pattern = r'\$\{(\w+):-([^}]+)\}'
    defaults = {}
    for match in re.finditer(pattern, text):
        var_name = match.group(1)
        default_val = match.group(2)
        defaults[var_name] = default_val
    return defaults


# ---------------------------------------------------------------------------
# Test 1: A1 (round-robin static broker) env vars
# ---------------------------------------------------------------------------


class TestA1Config:
    """Verify A1 (round-robin baseline) sets static broker explicitly."""

    def test_a1_sets_broker_module_to_static(self) -> None:
        """A1 MUST explicitly set BROKER_MODULE=src.broker.static_broker."""
        rc = resolve_config("A1")
        assert "BROKER_MODULE" in rc.env, (
            "A1 must explicitly set BROKER_MODULE in env dict"
        )
        assert rc.env["BROKER_MODULE"] == "src.broker.static_broker", (
            f"A1 BROKER_MODULE should be static_broker, got {rc.env['BROKER_MODULE']}"
        )

    def test_a1_sets_round_robin_placement(self) -> None:
        """A1 must set PLACEMENT=round_robin."""
        rc = resolve_config("A1")
        assert rc.env.get("PLACEMENT") == "round_robin", (
            f"A1 PLACEMENT should be round_robin, got {rc.env.get('PLACEMENT')}"
        )


# ---------------------------------------------------------------------------
# Test 2: A2 (random static broker) env vars
# ---------------------------------------------------------------------------


class TestA2Config:
    """Verify A2 (random baseline) sets static broker explicitly."""

    def test_a2_sets_broker_module_to_static(self) -> None:
        """A2 MUST explicitly set BROKER_MODULE=src.broker.static_broker."""
        rc = resolve_config("A2")
        assert "BROKER_MODULE" in rc.env
        assert rc.env["BROKER_MODULE"] == "src.broker.static_broker"

    def test_a2_sets_random_placement(self) -> None:
        """A2 must set PLACEMENT=random."""
        rc = resolve_config("A2")
        assert rc.env.get("PLACEMENT") == "random"


# ---------------------------------------------------------------------------
# Test 3: A3 (neural broker) env vars
# ---------------------------------------------------------------------------


class TestA3Config:
    """Verify A3 (neural broker) uses compose default correctly."""

    def test_a3_either_sets_neural_or_relies_on_compose_default(self) -> None:
        """A3 should use neural_broker (either explicit or via compose default)."""
        rc = resolve_config("A3")
        compose_defaults = _parse_compose_defaults(_COMPOSE_PATH)

        if "BROKER_MODULE" in rc.env:
            # If explicitly set, must be neural_broker
            assert "neural_broker" in rc.env["BROKER_MODULE"], (
                f"A3 explicitly sets BROKER_MODULE but not to neural_broker: "
                f"{rc.env['BROKER_MODULE']}"
            )
        else:
            # Relies on compose default — verify compose default IS neural_broker
            assert "BROKER_MODULE" in compose_defaults, (
                "BROKER_MODULE has no default in compose file"
            )
            assert "neural_broker" in compose_defaults["BROKER_MODULE"], (
                f"Compose default for BROKER_MODULE is not neural_broker: "
                f"{compose_defaults['BROKER_MODULE']}"
            )


# ---------------------------------------------------------------------------
# Test 4: Compose file defaults analysis
# ---------------------------------------------------------------------------


class TestComposeDefaults:
    """Verify compose file defaults and their interaction with config table."""

    def test_compose_broker_module_default_is_neural(self) -> None:
        """docker-compose.local.yaml defaults BROKER_MODULE to neural_broker."""
        defaults = _parse_compose_defaults(_COMPOSE_PATH)
        assert "BROKER_MODULE" in defaults, (
            "BROKER_MODULE not found in compose file defaults"
        )
        assert defaults["BROKER_MODULE"] == "src.broker.neural_broker", (
            f"Compose BROKER_MODULE default is '{defaults['BROKER_MODULE']}', "
            f"expected 'src.broker.neural_broker'"
        )

    def test_compose_arrival_rate_default(self) -> None:
        """Compose defaults for ARRIVAL_RATE should be documented."""
        defaults = _parse_compose_defaults(_COMPOSE_PATH)
        assert "ARRIVAL_RATE" in defaults
        # Verify the default is a valid number
        float(defaults["ARRIVAL_RATE"])


# ---------------------------------------------------------------------------
# Test 5: Static broker configs MUST override compose default
# ---------------------------------------------------------------------------


class TestStaticBrokerOverride:
    """CRITICAL: Any config that should run static_broker MUST explicitly
    set BROKER_MODULE, because the compose default is neural_broker.

    If a config intended for static_broker does NOT set BROKER_MODULE,
    the experiment silently runs the neural broker instead, producing
    invalid results. This is the core GAP-2 issue.
    """

    def _configs_with_static_broker(self) -> list[str]:
        """Return config names that declare broker=static_broker."""
        return [
            name for name, entry in _CONFIG_TABLE.items()
            if entry.get("broker") == "src.broker.static_broker"
        ]

    def test_all_static_broker_configs_override_broker_module(self) -> None:
        """Every config with broker=static_broker MUST have BROKER_MODULE in env."""
        compose_defaults = _parse_compose_defaults(_COMPOSE_PATH)
        compose_broker_default = compose_defaults.get("BROKER_MODULE", "")

        for name in self._configs_with_static_broker():
            rc = resolve_config(name)
            assert "BROKER_MODULE" in rc.env, (
                f"CRITICAL: Config {name} declares broker=static_broker "
                f"but does NOT set BROKER_MODULE in env. "
                f"Compose default is '{compose_broker_default}', so this config "
                f"would SILENTLY RUN NEURAL BROKER. "
                f"Experiments using {name} may be INVALID."
            )
            assert rc.env["BROKER_MODULE"] == "src.broker.static_broker", (
                f"Config {name}: BROKER_MODULE is '{rc.env['BROKER_MODULE']}', "
                f"expected 'src.broker.static_broker'"
            )

    def test_static_configs_are_a1_a2_b2flat(self) -> None:
        """Verify which configs are supposed to use static_broker.

        This is a documentation test: if a new config is added that should
        use static_broker, this test will fail and require an update.
        """
        static_configs = set(self._configs_with_static_broker())
        expected = {"A1", "A2", "B2flat"}
        assert static_configs == expected, (
            f"Static broker configs changed: expected {expected}, got {static_configs}. "
            f"If a new static config was added, update this test and verify it sets "
            f"BROKER_MODULE in its env dict."
        )


# ---------------------------------------------------------------------------
# Test 6: Neural broker configs (no override needed, but verify consistency)
# ---------------------------------------------------------------------------


class TestNeuralBrokerConfigs:
    """Configs that use neural broker can either set it explicitly or
    rely on the compose default. Verify the compose default matches."""

    def _configs_with_neural_broker(self) -> list[str]:
        """Return config names that either declare broker=None (neural default)
        or explicitly declare neural_broker."""
        return [
            name for name, entry in _CONFIG_TABLE.items()
            if entry.get("broker") is None
        ]

    def test_neural_configs_do_not_accidentally_set_static(self) -> None:
        """No neural config should have BROKER_MODULE=static_broker in env."""
        for name in self._configs_with_neural_broker():
            rc = resolve_config(name)
            broker = rc.env.get("BROKER_MODULE", "")
            assert "static_broker" not in broker, (
                f"Config {name} has broker=None (neural default) but "
                f"BROKER_MODULE={broker} in env, which contradicts. "
                f"This would run static_broker instead of neural."
            )


# ---------------------------------------------------------------------------
# Test 7: Cross-reference ALL strategies against compose defaults
# ---------------------------------------------------------------------------


class TestAllStrategiesVsComposeDefaults:
    """For EVERY config in _CONFIG_TABLE, verify the env dict overrides
    ALL compose defaults that matter for experiment correctness."""

    def test_every_config_resolves_without_error(self) -> None:
        """resolve_config works for all registered config names."""
        for name in _CONFIG_TABLE:
            rc = resolve_config(name)
            assert rc.compose_files, f"Config {name} has empty compose_files"

    def test_arrival_rate_always_set(self) -> None:
        """ARRIVAL_RATE is always set by resolve_config, overriding compose default."""
        for name in _CONFIG_TABLE:
            rc = resolve_config(name)
            assert "ARRIVAL_RATE" in rc.env, (
                f"Config {name} missing ARRIVAL_RATE in env"
            )
            # Verify it's a valid float
            rate = float(rc.env["ARRIVAL_RATE"])
            assert rate > 0, f"Config {name} has non-positive ARRIVAL_RATE={rate}"

    def test_seed_always_set(self) -> None:
        """SEED is always set by resolve_config."""
        for name in _CONFIG_TABLE:
            rc = resolve_config(name)
            assert "SEED" in rc.env, f"Config {name} missing SEED in env"

    def test_broker_module_consistency_matrix(self) -> None:
        """Comprehensive matrix: for each config, verify broker_module
        field is consistent with the env dict BROKER_MODULE.

        This catches cases where:
        - broker field says static but env doesn't set BROKER_MODULE
        - broker field says None (neural) but env sets static_broker
        """
        compose_defaults = _parse_compose_defaults(_COMPOSE_PATH)
        compose_broker = compose_defaults.get("BROKER_MODULE", "UNSET")

        for name, entry in _CONFIG_TABLE.items():
            rc = resolve_config(name)
            declared_broker = entry.get("broker")
            env_broker = rc.env.get("BROKER_MODULE")

            if declared_broker is not None:
                # Config explicitly declares a broker module
                assert env_broker is not None, (
                    f"Config {name}: declares broker={declared_broker} but "
                    f"BROKER_MODULE not in env. Compose default ({compose_broker}) "
                    f"would be used instead."
                )
                assert env_broker == declared_broker, (
                    f"Config {name}: broker={declared_broker} but "
                    f"BROKER_MODULE={env_broker} in env (mismatch)."
                )
            else:
                # Config relies on compose default (should be neural_broker)
                if env_broker is not None:
                    # If env explicitly sets it, must be neural
                    assert "neural_broker" in env_broker, (
                        f"Config {name}: broker=None (neural default) but "
                        f"env sets BROKER_MODULE={env_broker}"
                    )

    def test_transport_parameter_accepted(self) -> None:
        """resolve_config accepts both http and kafka transport for all configs."""
        for name in _CONFIG_TABLE:
            for transport in TRANSPORTS:
                rc = resolve_config(name, transport=transport)
                assert rc.compose_files  # Should always have at least base compose


# ---------------------------------------------------------------------------
# Test 8: CRITICAL — detect silent neural broker masking for S1/S2
# ---------------------------------------------------------------------------


class TestSilentMaskingDetection:
    """THE MOST CRITICAL TEST: detect if any baseline config (A1/A2)
    that should run static_broker would silently fall through to
    neural_broker due to missing env override.

    If this test fails, ALL experiments using that config are INVALID
    and must be re-run. (See GAP-2 in test-completeness-integration.md)
    """

    def test_a1_would_not_use_compose_default(self) -> None:
        """A1 sets BROKER_MODULE explicitly, so compose default is irrelevant."""
        rc = resolve_config("A1")
        assert "BROKER_MODULE" in rc.env, (
            "CRITICAL: A1 (round-robin baseline) does NOT set BROKER_MODULE. "
            "Compose default is neural_broker. "
            "ALL A1 EXPERIMENT RESULTS ARE RUNNING NEURAL BROKER, NOT STATIC. "
            "EXPERIMENTS MUST BE STOPPED AND RE-EXAMINED."
        )

    def test_a2_would_not_use_compose_default(self) -> None:
        """A2 sets BROKER_MODULE explicitly, so compose default is irrelevant."""
        rc = resolve_config("A2")
        assert "BROKER_MODULE" in rc.env, (
            "CRITICAL: A2 (random baseline) does NOT set BROKER_MODULE. "
            "Compose default is neural_broker. "
            "ALL A2 EXPERIMENT RESULTS ARE RUNNING NEURAL BROKER, NOT STATIC. "
            "EXPERIMENTS MUST BE STOPPED AND RE-EXAMINED."
        )

    def test_b2flat_would_not_use_compose_default(self) -> None:
        """B2flat sets BROKER_MODULE explicitly."""
        rc = resolve_config("B2flat")
        assert "BROKER_MODULE" in rc.env, (
            "CRITICAL: B2flat (flat baseline) does NOT set BROKER_MODULE. "
            "Compose default is neural_broker. "
            "ALL B2flat EXPERIMENT RESULTS MAY BE INVALID."
        )

    def test_effective_broker_for_each_config(self) -> None:
        """Print the effective broker module for EVERY config.

        This is a documentation/audit test. It constructs what each config
        would ACTUALLY use (env override or compose default) and verifies
        it makes sense for the experiment design.
        """
        compose_defaults = _parse_compose_defaults(_COMPOSE_PATH)
        compose_broker = compose_defaults.get("BROKER_MODULE", "UNSET")

        effective_brokers: dict[str, str] = {}
        for name in _CONFIG_TABLE:
            rc = resolve_config(name)
            env_broker = rc.env.get("BROKER_MODULE")
            effective = env_broker if env_broker else compose_broker
            effective_brokers[name] = effective

        # A1 and A2 must effectively use static_broker
        assert "static_broker" in effective_brokers["A1"], (
            f"A1 effective broker is {effective_brokers['A1']}, expected static_broker"
        )
        assert "static_broker" in effective_brokers["A2"], (
            f"A2 effective broker is {effective_brokers['A2']}, expected static_broker"
        )

        # A3 must effectively use neural_broker
        assert "neural_broker" in effective_brokers["A3"], (
            f"A3 effective broker is {effective_brokers['A3']}, expected neural_broker"
        )

        # B2flat must effectively use static_broker
        assert "static_broker" in effective_brokers["B2flat"], (
            f"B2flat effective broker is {effective_brokers['B2flat']}, expected static_broker"
        )
