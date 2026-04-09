"""Ablation worker entry point.

This module is a thin re-export of `src.worker.worker.main` used solely
to identify ablation runs in process listings and ensure that the main
campaign worker code (`src.worker`) is not modified after the campaign
runs that depend on it.

Functionally identical to `src.worker.worker`. The ablation experiment
relies on the existing `--processing-speed` flag to model heterogeneous
worker capacities (slower workers have higher processing_speed).

Invoked via:
    python -m src.worker.ablation_worker --node-id ... --processing-speed ...
"""

from __future__ import annotations

from src.worker.worker import main as _worker_main

# Re-export the main entry point with a distinct module path. Tests
# verify this is the same callable as src.worker.worker.main.
main = _worker_main


if __name__ == "__main__":
    main()
