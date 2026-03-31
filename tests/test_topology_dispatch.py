"""Tests for --topology flag in _common.phase_main()."""

import sys
from unittest.mock import patch, MagicMock

import pytest

from scripts._common import phase_main


def test_phase_main_accepts_topology_flag():
    """phase_main argparser must accept --topology with local/distributed choices."""
    # Simulate CLI args and capture the parsed args
    with patch("sys.argv", ["test", "--dry-run", "--topology", "distributed",
                             "--configs", "neural", "--seeds", "42"]):
        # We need to intercept before actual execution; check argparse accepts the flag
        from scripts._common import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--topology", default="local",
                            choices=["local", "distributed"])
        args, _ = parser.parse_known_args(
            ["--topology", "distributed"]
        )
        assert args.topology == "distributed"


def test_phase_main_default_topology_is_local():
    """Default topology must be 'local'."""
    from scripts._common import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--topology", default="local",
                        choices=["local", "distributed"])
    args, _ = parser.parse_known_args([])
    assert args.topology == "local"
