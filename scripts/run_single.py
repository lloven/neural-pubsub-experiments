#!/usr/bin/env python3
"""Ad-hoc single experiment run.

Routes to the appropriate phase runner based on config name.

Usage:
    python -m scripts.run_single rr medium 3 42
    python -m scripts.run_single flat medium 3 42 --dry-run
    python -m scripts.run_single embb-kill medium 3 42
"""

from __future__ import annotations

import argparse
import logging
import sys

from scripts._common import PROJECT_ROOT

logger = logging.getLogger(__name__)

# Map config names to their phase (for results directory routing)
_CONFIG_TO_PHASE: dict[str, str] = {
    # Baseline
    "rr": "baseline", "random": "baseline", "neural": "baseline",
    # Slicing
    "flat": "slicing", "slicing-neural": "slicing", "slicing-rr": "slicing",
    "gov": "slicing", "gov-fail": "slicing",
    # Federation
    "static": "federation", "fed-neural": "federation", "fed-gov": "federation",
    "broker-kill": "federation", "net-part": "federation",
    # Resilience
    "embb-kill": "resilience", "urllc-kill": "resilience",
    "funnel-wait": "resilience", "funnel-proceed": "resilience", "funnel-abort": "resilience",
    # Contention
    "20pps": "contention", "50pps": "contention", "10pps-kill": "contention",
}

# Stress configs follow a naming pattern: {rate}pps-{strategy}-{fail}
# They are not enumerated here; detected by pattern match.


def _detect_phase(config: str) -> str:
    """Detect which phase a config belongs to."""
    if config in _CONFIG_TO_PHASE:
        return _CONFIG_TO_PHASE[config]
    # Stress configs: NNpps-{rr|neural}-{nofail|fail}
    if "pps-" in config and ("-fail" in config or "-nofail" in config):
        return "stress"
    raise ValueError(
        f"Unknown config: {config!r}. "
        f"Valid: {sorted(_CONFIG_TO_PHASE.keys())} + stress patterns (e.g. 20pps-rr-fail)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a single experiment configuration (routes to correct phase)",
    )
    parser.add_argument("config", help="Config name (rr, flat, embb-kill, 20pps-rr-fail, ...)")
    parser.add_argument("rate", help="Rate label (low, medium, high) or numeric")
    parser.add_argument("stages", type=int, help="Pipeline complexity (stages)")
    parser.add_argument("seed", type=int, help="Random seed")
    parser.add_argument("--dry-run", action="store_true", help="Print without executing")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))

    phase = _detect_phase(args.config)
    logger.info("Config %r → phase %r", args.config, phase)

    # Delegate to the appropriate phase runner with --configs and --seeds
    import subprocess
    cmd = [
        sys.executable, "-m", f"scripts.run_{phase}",
        "--configs", args.config,
        "--seeds", str(args.seed),
    ]
    if args.dry_run:
        cmd.append("--dry-run")

    logger.info("Delegating to: %s", " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
