"""Single source of truth for the experiment matrix structure.

Every phase's configs, seeds, transports, and expected run counts are
defined here.  Test files import from this module instead of hardcoding
matrix sizes.  When a config is added or removed, only this file changes.

Usage in tests::

    from scripts.experiment_matrix import expected_run_count, get_configs, get_seeds

    matrix = build_run_matrix(get_configs("B"), get_seeds("B"))
    assert len(matrix) == expected_run_count("B")
"""

from __future__ import annotations

from scripts._common import DEFAULT_SEEDS, EXTENDED_SEEDS, TRANSPORTS

# ---------------------------------------------------------------------------
# Experiment definitions
# ---------------------------------------------------------------------------

EXPERIMENTS: dict[str, dict] = {
    "A": {
        "description": "Single-site baselines (dual-transport factorial)",
        "configs": ["A1", "A2", "A3"],
        "seeds": DEFAULT_SEEDS,
        "transports": TRANSPORTS,
        "notes": (
            "Core factorial: configs x transports x seeds at medium rate, "
            "plus rate sensitivity arm (A3/http x all rates). "
            "Run count is computed by build_run_matrix, not a simple product."
        ),
    },
    "A6": {
        "description": "Resource contention",
        "configs": ["A6.1", "A6.2", "A6.3"],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        # Simple product: 3 configs x 5 seeds = 15
    },
    "B": {
        "description": "Slice-aware placement",
        "configs": ["B1", "B1eq", "B2", "B2flat", "B3", "B4"],
        "seeds": DEFAULT_SEEDS,
        "transports": TRANSPORTS,
        # 6 configs x 2 transports x 5 seeds = 60
    },
    "C": {
        "description": "Cross-site federation",
        "configs": ["C1", "C2", "C3", "C4"],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        # 4 configs x 5 seeds = 20
    },
    "D": {
        "description": "Failure and adaptation",
        "configs": ["D1", "D2", "D3", "D4", "D5"],
        "seeds": EXTENDED_SEEDS,
        "transports": ["http"],
        # Default (S3 only): 5 configs x 10 seeds = 50
        # Strategy-all (S1+S2+S3): 5 configs x 3 strategies x seeds
        "strategies": ["S1", "S2", "S3"],
        "default_strategy": ["S3"],
    },
    "E": {
        "description": "Combined H3+H6 contention + failure",
        "configs": ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8"],
        "seeds": DEFAULT_SEEDS,
        "transports": ["http"],
        # 8 configs x 5 seeds = 40
    },
}


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------


def get_configs(phase: str) -> list[str]:
    """Return the config names for the given phase.

    Args:
        phase: Phase identifier (e.g., "A", "B", "D", "A6", "E").

    Returns:
        List of config name strings.

    Raises:
        KeyError: If the phase is not defined.
    """
    return list(EXPERIMENTS[phase]["configs"])


def get_seeds(phase: str) -> list[int]:
    """Return the seed list for the given phase.

    Args:
        phase: Phase identifier.

    Returns:
        List of integer seeds.
    """
    return list(EXPERIMENTS[phase]["seeds"])


def get_transports(phase: str) -> list[str]:
    """Return the transport list for the given phase.

    Args:
        phase: Phase identifier.

    Returns:
        List of transport strings.
    """
    return list(EXPERIMENTS[phase]["transports"])


def expected_run_count(
    phase: str,
    *,
    configs: list[str] | None = None,
    seeds: list[int] | None = None,
    transports: list[str] | None = None,
    strategies: list[str] | None = None,
) -> int:
    """Compute the expected number of runs for a phase (or subset).

    For most phases this is a simple Cartesian product.  Phase A is
    special (factorial + rate-sensitivity arm) and is not handled here;
    use the actual build_run_matrix function for Phase A counts.

    Args:
        phase: Phase identifier.
        configs: Override config list (default: all configs for the phase).
        seeds: Override seed list (default: phase default).
        transports: Override transport list (default: phase default).
        strategies: For Phase D only, override strategies (default: ["S3"]).

    Returns:
        Integer run count.
    """
    exp = EXPERIMENTS[phase]
    n_configs = len(configs) if configs is not None else len(exp["configs"])
    n_seeds = len(seeds) if seeds is not None else len(exp["seeds"])

    if phase == "B":
        n_transports = len(transports) if transports is not None else len(exp["transports"])
        return n_configs * n_transports * n_seeds

    if phase == "D":
        n_strategies = (
            len(strategies)
            if strategies is not None
            else len(exp.get("default_strategy", ["S3"]))
        )
        return n_configs * n_strategies * n_seeds

    if phase in ("A6", "C", "E"):
        return n_configs * n_seeds

    # Phase A: not a simple product; caller should use build_run_matrix
    raise ValueError(
        f"Phase {phase} does not support simple run-count calculation. "
        f"Use the phase's build_run_matrix() function directly."
    )
